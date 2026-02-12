use crate::error::*;
use crate::structs::*;
use crate::historical_downloader::HistoricalDownloader;

use binance_sdk::{
    config::{ConfigurationRestApi, ProxyConfig, ProxyAuth},
    derivatives_trading_usds_futures::{
        DerivativesTradingUsdsFuturesRestApi,
        rest_api::{
            RestApi, 
            KlineCandlestickDataIntervalEnum, KlineCandlestickDataParams, KlineCandlestickDataResponseItemInner,
        },
        websocket_streams::KlineCandlestickStreamsResponseK,
    },
};

use futures::StreamExt;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use reqwest::Client;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use rand::Rng;
use serde::Deserialize;

/// Read proxy configuration from environment variables.
/// Returns None if PROXY_HOST is not set (direct connection).
fn proxy_config_from_env(port: u16) -> Option<ProxyConfig> {
    let host = std::env::var("PROXY_HOST").ok()?;
    if host.is_empty() {
        return None;
    }
    let username = std::env::var("PROXY_USERNAME").unwrap_or_default();
    let password = std::env::var("PROXY_PASSWORD").unwrap_or_default();
    let protocol = std::env::var("PROXY_PROTOCOL").unwrap_or_else(|_| "https".to_string());

    let auth = if username.is_empty() {
        None
    } else {
        Some(ProxyAuth {
            username,
            password,
        })
    };

    Some(ProxyConfig {
        host,
        port,
        protocol: Some(protocol),
        auth,
    })
}

// Combined stream response wrapper
#[derive(Debug, Deserialize)]
struct CombinedStreamWrapper {
    #[allow(dead_code)]
    stream: String,
    data: CombinedStreamData,
}

#[derive(Debug, Deserialize)]
struct CombinedStreamData {
    s: Option<String>,
    k: Option<KlineCandlestickStreamsResponseK>,
}

const BATCH_SIZE: i64 = 1000;
const ONE_MINUTE_MS: i64 = 60_000;
const MAX_SYMBOLS_PER_WS: usize = 50;  // Binance limit per WebSocket connection
// Rate limit: 2400 weight/min, limit=1000 costs 5 weight
// 2400/5 = 480 requests/min = 8 req/sec = 125ms interval
// Use 150ms for safety margin
const REQUEST_INTERVAL_MS: u64 = 150;

pub struct BinanceCollector {
    pub symbols: Vec<String>,
    rest_client: RestApi,
    last_closed_timestamps: Arc<RwLock<HashMap<String, i64>>>,
}

impl BinanceCollector {
    pub fn new(symbols: Vec<String>) -> Self {
        assert!(!symbols.is_empty(), "symbols cannot be empty");

        let symbols: Vec<String> = symbols.into_iter().map(|s| s.to_lowercase()).collect();

        let port: u16 = rand::rng().random_range(10036..=10066);

        let mut config_builder = ConfigurationRestApi::builder()
            .timeout(10000);

        if let Some(proxy) = proxy_config_from_env(port) {
            config_builder = config_builder.proxy(proxy);
        }

        let rest_client_config = config_builder
            .build()
            .expect("Failed to initialize the rest api client");

        let rest_client = DerivativesTradingUsdsFuturesRestApi::production(rest_client_config);

        Self {
            symbols,
            rest_client,
            last_closed_timestamps: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Backfill K-line data for a specific symbol from start_time to end_time.
    /// Uses limit=1000 per request (weight=5, ~480 requests/min allowed).
    /// Returns total number of candles fetched.
    pub async fn backfill(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
        candle_tx: &mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let symbol_upper = symbol.to_uppercase();
        let symbol_lower = symbol.to_lowercase();
        let mut current_start = start_time;
        let mut total_count: u64 = 0;

        info!(
            "Starting backfill for {} from {} to {}",
            symbol_upper, start_time, end_time
        );

        while current_start < end_time {
            let params = KlineCandlestickDataParams::builder(
                symbol_upper.clone(),
                KlineCandlestickDataIntervalEnum::Interval1m,
            )
            .start_time(Some(current_start))
            .end_time(Some(end_time))
            .limit(Some(BATCH_SIZE))
            .build()
            .map_err(|e| CollectorError::RestApiError(e.to_string()))?;

            let response = self.rest_client
                .kline_candlestick_data(params)
                .await
                .map_err(|e| CollectorError::RestApiError(e.to_string()))?;

            let klines = response.data().await
                .map_err(|e| CollectorError::RestApiError(e.to_string()))?;

            let batch_count = klines.len();
            if batch_count == 0 {
                break;
            }

            let mut last_timestamp = current_start;

            for kline in &klines {
                if let Some(candle) = Self::parse_rest_kline(&symbol_upper, kline) {
                    last_timestamp = candle.timestamp;

                    if let Err(e) = candle_tx.send(candle).await {
                        warn!("Failed to send candle: {}", e);
                    }

                    total_count += 1;
                }
            }

            // Update last_closed_timestamp for this symbol
            {
                let mut timestamps = self.last_closed_timestamps.write().await;
                timestamps.insert(symbol_lower.clone(), last_timestamp);
            }

            debug!(
                "Fetched {} klines for {}, total: {}, last_ts: {}",
                batch_count, symbol_upper, total_count, last_timestamp
            );

            current_start = last_timestamp + ONE_MINUTE_MS;

            if batch_count < BATCH_SIZE as usize {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(REQUEST_INTERVAL_MS)).await;
        }

        info!(
            "Backfill completed for {}: {} candles",
            symbol_upper, total_count
        );

        Ok(total_count)
    }

    /// Backfill all symbols in the collector.
    pub async fn backfill_all(
        &self,
        start_time: i64,
        end_time: i64,
        candle_tx: &mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let mut total_count: u64 = 0;

        for symbol in &self.symbols.clone() {
            let count = self.backfill(symbol, start_time, end_time, candle_tx).await?;
            total_count += count;
        }

        Ok(total_count)
    }

    /// Start WebSocket streams for real-time K-line data on all symbols.
    /// Creates multiple connections if symbols > MAX_SYMBOLS_PER_WS.
    /// Automatically reconnects and backfills missed data on disconnect.
    pub async fn start_stream(
        &self,
        candle_tx: mpsc::Sender<CandleData>,
    ) -> Result<(), CollectorError> {
        let total_symbols = self.symbols.len();
        let num_connections = (total_symbols + MAX_SYMBOLS_PER_WS - 1) / MAX_SYMBOLS_PER_WS;
        
        info!(
            "Starting {} WebSocket connections for {} symbols",
            num_connections, total_symbols
        );

        // Split symbols into batches
        let batches: Vec<Vec<String>> = self.symbols
            .chunks(MAX_SYMBOLS_PER_WS)
            .map(|chunk| chunk.to_vec())
            .collect();

        loop {
            // Start all WebSocket connections in parallel
            let mut handles = Vec::new();
            
            for (i, batch) in batches.iter().enumerate() {
                let batch_symbols = batch.clone();
                let tx = candle_tx.clone();
                let last_ts = self.last_closed_timestamps.clone();
                
                let handle = tokio::spawn(async move {
                    Self::run_single_ws_connection(i, batch_symbols, tx, last_ts).await
                });
                handles.push(handle);
            }

            // Wait for any connection to fail
            let mut all_ok = true;
            for (i, handle) in handles.into_iter().enumerate() {
                match handle.await {
                    Ok(Ok(())) => info!("WS connection {} ended normally", i),
                    Ok(Err(e)) => {
                        error!("WS connection {} error: {}", i, e);
                        all_ok = false;
                    }
                    Err(e) => {
                        error!("WS connection {} join error: {}", i, e);
                        all_ok = false;
                    }
                }
            }

            if all_ok {
                break;
            }

            // Reconnect after error
            error!("WebSocket error. Reconnecting in 5 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            // Backfill missed data
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let timestamps = self.last_closed_timestamps.read().await.clone();
            
            for symbol in &self.symbols {
                if let Some(&last_ts) = timestamps.get(symbol) {
                    if now - last_ts > ONE_MINUTE_MS {
                        info!("Backfilling missed data for {} from {}", symbol, last_ts);
                        if let Err(e) = self.backfill(symbol, last_ts, now, &candle_tx).await {
                            warn!("Backfill failed for {}: {}", symbol, e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Run a single WebSocket connection for a batch of symbols (combined streams)
    async fn run_single_ws_connection(
        connection_id: usize,
        symbols: Vec<String>,
        candle_tx: mpsc::Sender<CandleData>,
        last_closed_timestamps: Arc<RwLock<HashMap<String, i64>>>,
    ) -> Result<(), CollectorError> {
        // Build combined stream URL: wss://fstream.binance.com/stream?streams=symbol1@kline_1m/symbol2@kline_1m
        let streams: Vec<String> = symbols.iter()
            .map(|s| format!("{}@kline_1m", s.to_lowercase()))
            .collect();
        let url = format!("wss://fstream.binance.com/stream?streams={}", streams.join("/"));

        let (ws_stream, _) = connect_async(&url)
            .await
            .map_err(|e| CollectorError::ConnectionFailed(e.to_string()))?;

        info!("WS {} connected with {} symbols (combined stream)", connection_id, symbols.len());

        let (_, mut read) = ws_stream.split();

        // Process incoming messages
        while let Some(msg_result) = read.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    // Combined stream format: {"stream":"btcusdt@kline_1m","data":{...}}
                    if let Ok(wrapper) = serde_json::from_str::<CombinedStreamWrapper>(&text) {
                        if let Some(k) = wrapper.data.k {
                            let symbol = wrapper.data.s.unwrap_or_default();
                            let candle = Self::parse_ws_kline(&symbol, &k);

                            if let Err(e) = candle_tx.send(candle.clone()).await {
                                warn!("Failed to send candle: {}", e);
                            }

                            if candle.is_closed {
                                let mut timestamps = last_closed_timestamps.write().await;
                                timestamps.insert(candle.symbol.to_lowercase(), candle.timestamp);

                                debug!(
                                    "Kline closed: {} ts={} c={:.2} net_vol={:.4}",
                                    candle.symbol, candle.timestamp, candle.close, candle.net_volume
                                );
                            }
                        }
                    }
                }
                Ok(Message::Ping(data)) => {
                    debug!("WS {} received ping", connection_id);
                    // tungstenite auto-responds to pings
                    let _ = data;
                }
                Ok(Message::Close(_)) => {
                    info!("WS {} received close", connection_id);
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    error!("WS {} error: {}", connection_id, e);
                    break;
                }
            }
        }

        Err(CollectorError::ConnectionFailed("Disconnected".to_string()))
    }

    fn parse_rest_kline(symbol: &str, kline: &Vec<KlineCandlestickDataResponseItemInner>) -> Option<CandleData> {
        if kline.len() < 12 {
            return None;
        }

        let timestamp = match &kline[0] {
            KlineCandlestickDataResponseItemInner::Integer(v) => *v,
            _ => return None,
        };

        let open = Self::parse_string_field(&kline[1])?;
        let high = Self::parse_string_field(&kline[2])?;
        let low = Self::parse_string_field(&kline[3])?;
        let close = Self::parse_string_field(&kline[4])?;
        let volume = Self::parse_string_field(&kline[5])?;
        let taker_buy_volume = Self::parse_string_field(&kline[9])?;

        let net_volume = CandleData::calculate_net_volume(volume, taker_buy_volume);

        Some(CandleData {
            symbol: symbol.to_uppercase(),
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            taker_buy_volume,
            net_volume,
            is_closed: true,
        })
    }

    fn parse_string_field(item: &KlineCandlestickDataResponseItemInner) -> Option<f64> {
        match item {
            KlineCandlestickDataResponseItemInner::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    fn parse_ws_kline(symbol: &str, k: &KlineCandlestickStreamsResponseK) -> CandleData {
        let timestamp = k.t.unwrap_or(0);
        let open = k.o.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let high = k.h.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let low = k.l.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let close = k.c.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let volume = k.v.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let taker_buy_volume = k.v_uppercase.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let is_closed = k.x.unwrap_or(false);

        let net_volume = CandleData::calculate_net_volume(volume, taker_buy_volume);

        CandleData {
            symbol: symbol.to_uppercase(),
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            taker_buy_volume,
            net_volume,
            is_closed,
        }
    }


    /// Get all tradable symbol in current market
    pub async fn get_symbol() -> Result<Vec<Symbol>, CollectorError> {

        let port: u16 = rand::rng().random_range(10036..=10066);

        let mut config_builder = ConfigurationRestApi::builder()
            .timeout(10000);

        if let Some(proxy) = proxy_config_from_env(port) {
            config_builder = config_builder.proxy(proxy);
        }

        let rest_client_config = config_builder
            .build()
            .expect("Failed to initialize the rest api client");

        let rest_client = DerivativesTradingUsdsFuturesRestApi::production(rest_client_config);

        let mut symbol_list = Vec::new();

        let response = rest_client
            .exchange_information()
            .await
            .map_err(|e| CollectorError::GetSymbolError(e.to_string()))?;

        let data = response.data().await.unwrap();

        let symbol_vec = data.symbols.unwrap();

        for item in symbol_vec {

            if item.contract_type.unwrap() == "PERPETUAL" {

                let symbol = Symbol { 

                    symbol: item.symbol.unwrap(),
                    start_timestamp: item.onboard_date.unwrap(),
                };

                symbol_list.push(symbol);
            }
        }                                            
                        
        Ok(symbol_list)
    }


    /// Build proxy list (port 10000-10099) and create clients
    pub async fn build_clients() -> Vec<Arc<RestApi>> {
        let mut handles = Vec::new();

        let port_start: u16 = std::env::var("PROXY_PORT_START")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(10000);
        let port_end: u16 = std::env::var("PROXY_PORT_END")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(10099);

        for port in port_start..=port_end {
            let handle = tokio::spawn(async move {
                let mut config_builder = ConfigurationRestApi::builder()
                    .timeout(5000);

                if let Some(proxy) = proxy_config_from_env(port) {
                    config_builder = config_builder.proxy(proxy);
                }

                let config = config_builder
                    .build()
                    .ok()?;

                let client = DerivativesTradingUsdsFuturesRestApi::production(config);
                
                // Test connection
                if client.check_server_time().await.ok()?.data().await.is_ok() {
                    Some(Arc::new(client))
                } else {
                    None
                }
            });
            handles.push(handle);
        }

        let mut clients = Vec::new();
        for handle in handles {
            if let Ok(Some(client)) = handle.await {
                clients.push(client);
            }
        }

        info!("Built {} working clients", clients.len());
        clients
    }

    /// Comprehensive sync: Monthly ZIP → Daily ZIP → API (fastest to slowest)
    pub async fn sync_comprehensive(
        symbol: String,
        start_time: i64,
        api_clients: Vec<Arc<RestApi>>,
        http_clients: &[Arc<Client>],
        candle_tx: mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let mut total = 0u64;
        let mut current_start = start_time;
        
        // Phase 1: Monthly ZIP download (complete months only)
        let month_end = HistoricalDownloader::last_complete_month_end();
        if current_start < month_end && !http_clients.is_empty() {
            info!("{}: Phase 1 - Monthly ZIP download", symbol);
            match HistoricalDownloader::download_symbol_with_clients(
                &symbol,
                current_start,
                http_clients,
                candle_tx.clone(),
            ).await {
                Ok(count) => {
                    total += count;
                    if count > 0 {
                        current_start = month_end;
                        info!("{}: Monthly ZIP done, {} candles", symbol, count);
                    }
                }
                Err(e) => warn!("{}: Monthly ZIP failed: {}", symbol, e),
            }
        }

        // Phase 2: Daily ZIP download (current month's completed days)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let today_start = (now / (24 * 60 * 60 * 1000)) * (24 * 60 * 60 * 1000);
        
        if current_start < today_start && !http_clients.is_empty() {
            info!("{}: Phase 2 - Daily ZIP download", symbol);
            match HistoricalDownloader::download_days_with_clients(
                &symbol,
                current_start,
                http_clients,
                candle_tx.clone(),
            ).await {
                Ok(count) => {
                    total += count;
                    if count > 0 {
                        current_start = today_start;
                        info!("{}: Daily ZIP done, {} candles", symbol, count);
                    }
                }
                Err(e) => warn!("{}: Daily ZIP failed: {}", symbol, e),
            }
        }

        // Phase 3: API for remaining (today's data)
        if current_start < now && !api_clients.is_empty() {
            info!("{}: Phase 3 - API sync for today", symbol);
            match Self::sync_from_scratch(
                symbol.clone(),
                current_start,
                api_clients,
                candle_tx,
            ).await {
                Ok(count) => {
                    total += count;
                    info!("{}: API sync done, {} candles", symbol, count);
                }
                Err(e) => warn!("{}: API sync failed: {}", symbol, e),
            }
        }

        info!("{}: Comprehensive sync complete, total {} candles", symbol, total);
        Ok(total)
    }

    /// Sync single symbol using ALL clients in parallel (each client handles a time segment)
    pub async fn sync_from_scratch(
        symbol: String,
        start_time: i64,
        clients: Vec<Arc<RestApi>>,
        candle_tx: mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let num_clients = clients.len();
        if num_clients == 0 {
            return Err(CollectorError::RestApiError("No clients".to_string()));
        }

        let total_duration = now - start_time;
        let segment_size = total_duration / num_clients as i64;

        info!(
            "Syncing {} with {} clients, {} -> {} ({} ms per segment)",
            symbol, num_clients, start_time, now, segment_size
        );

        // Each client gets a time segment
        let mut handles = Vec::new();

        for (i, client) in clients.into_iter().enumerate() {
            let seg_start = start_time + (i as i64 * segment_size);
            let seg_end = if i == num_clients - 1 {
                now // Last segment goes to now
            } else {
                start_time + ((i + 1) as i64 * segment_size)
            };

            let sym = symbol.clone();
            let tx = candle_tx.clone();

            handles.push(tokio::spawn(async move {
                Self::sync_segment(sym, seg_start, seg_end, client, tx).await
            }));
        }

        // Wait and sum results
        let mut total = 0u64;
        for (i, h) in handles.into_iter().enumerate() {
            match h.await {
                Ok(Ok(count)) => {
                    total += count;
                    debug!("Client {} done: {} candles", i, count);
                }
                Ok(Err(e)) => error!("Client {} error: {}", i, e),
                Err(e) => error!("Client {} join error: {}", i, e),
            }
        }

        info!("{} sync complete: {} candles", symbol, total);
        Ok(total)
    }

    /// Sync a specific time segment
    async fn sync_segment(
        symbol: String,
        start_time: i64,
        end_time: i64,
        client: Arc<RestApi>,
        candle_tx: mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let mut current = start_time;
        let mut count = 0u64;

        while current < end_time {
            let params = KlineCandlestickDataParams::builder(
                symbol.clone(),
                KlineCandlestickDataIntervalEnum::Interval1m,
            )
            .start_time(Some(current))
            .end_time(Some(end_time))
            .limit(Some(BATCH_SIZE))
            .build()
            .map_err(|e| CollectorError::RestApiError(e.to_string()))?;

            let res = client
                .kline_candlestick_data(params)
                .await
                .map_err(|e| CollectorError::RestApiError(e.to_string()))?;

            let klines = res.data().await
                .map_err(|e| CollectorError::RestApiError(e.to_string()))?;

            if klines.is_empty() { break; }

            let batch_len = klines.len();
            for kline in &klines {
                if let Some(candle) = Self::parse_rest_kline(&symbol, kline) {
                    current = candle.timestamp + ONE_MINUTE_MS;
                    let _ = candle_tx.send(candle).await;
                    count += 1;
                }
            }

            if batch_len < BATCH_SIZE as usize { break; }
            tokio::time::sleep(tokio::time::Duration::from_millis(REQUEST_INTERVAL_MS)).await;
        }

        Ok(count)
    }
}
