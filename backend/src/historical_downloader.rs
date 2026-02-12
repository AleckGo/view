use crate::structs::CandleData;
use crate::error::CollectorError;

use chrono::{Datelike, NaiveDate, Utc};
use log::{info, warn};
use std::io::{Cursor, Read};
use std::sync::Arc;
use tokio::sync::mpsc;
use futures::future::join_all;
use reqwest::Client;

const BASE_URL: &str = "https://data.binance.vision/data/futures/um/monthly/klines";
const DAILY_BASE_URL: &str = "https://data.binance.vision/data/futures/um/daily/klines";

/// Download historical klines from Binance data archive
pub struct HistoricalDownloader;

impl HistoricalDownloader {
    /// Build proxy clients from env vars (PROXY_HOST, PROXY_USERNAME, PROXY_PASSWORD, PROXY_PROTOCOL)
    /// Port range defaults to PROXY_PORT_START..PROXY_PORT_END (default 10000..10099)
    /// Returns direct (no-proxy) clients if PROXY_HOST is not set.
    pub async fn build_clients() -> Vec<Arc<Client>> {
        let mut handles = Vec::new();

        let proxy_host = std::env::var("PROXY_HOST").unwrap_or_default();
        let proxy_username = std::env::var("PROXY_USERNAME").unwrap_or_default();
        let proxy_password = std::env::var("PROXY_PASSWORD").unwrap_or_default();
        let proxy_protocol = std::env::var("PROXY_PROTOCOL").unwrap_or_else(|_| "https".to_string());
        let port_start: u16 = std::env::var("PROXY_PORT_START")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(10000);
        let port_end: u16 = std::env::var("PROXY_PORT_END")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(10099);

        if proxy_host.is_empty() {
            // No proxy configured — return a single direct client
            info!("No proxy configured (PROXY_HOST not set), using direct connection");
            if let Ok(client) = Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
            {
                return vec![Arc::new(client)];
            }
            return vec![];
        }

        for port in port_start..=port_end {
            let host = proxy_host.clone();
            let user = proxy_username.clone();
            let pass = proxy_password.clone();
            let proto = proxy_protocol.clone();

            handles.push(tokio::spawn(async move {
                let proxy_url = if user.is_empty() {
                    format!("{}://{}:{}", proto, host, port)
                } else {
                    format!("{}://{}:{}@{}:{}", proto, user, pass, host, port)
                };

                match reqwest::Proxy::all(&proxy_url) {
                    Ok(proxy) => {
                        match Client::builder()
                            .proxy(proxy)
                            .timeout(std::time::Duration::from_secs(60))
                            .build()
                        {
                            Ok(client) => Some(Arc::new(client)),
                            Err(_) => None,
                        }
                    }
                    Err(_) => None,
                }
            }));
        }
        
        let mut clients = Vec::new();
        for handle in handles {
            if let Ok(Some(client)) = handle.await {
                clients.push(client);
            }
        }
        
        info!("Built {} download clients with proxies", clients.len());
        clients
    }

    /// Download all monthly klines for a symbol using proxy clients
    pub async fn download_symbol_with_clients(
        symbol: &str,
        start_timestamp: i64,
        clients: &[Arc<Client>],
        candle_tx: mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let symbol_upper = symbol.to_uppercase();
        
        if clients.is_empty() {
            return Err(CollectorError::RestApiError("No clients available".to_string()));
        }
        
        // Calculate start and end months
        let start_date = Self::timestamp_to_date(start_timestamp);
        let now = Utc::now().naive_utc().date();
        
        // Generate list of months to download (exclude current month - incomplete)
        let months = Self::generate_months(start_date, now);
        
        if months.is_empty() {
            info!("{}: No complete months to download", symbol_upper);
            return Ok(0);
        }
        
        info!(
            "{}: Downloading {} months ({}-{:02} to {}-{:02}) with {} clients",
            symbol_upper,
            months.len(),
            months.first().unwrap().0, months.first().unwrap().1,
            months.last().unwrap().0, months.last().unwrap().1,
            clients.len()
        );
        
        // Download all months in parallel using all clients
        let mut handles = Vec::new();
        
        for (i, &(year, month)) in months.iter().enumerate() {
            let client = clients[i % clients.len()].clone();
            let sym = symbol_upper.clone();
            let tx = candle_tx.clone();
            
            handles.push(tokio::spawn(async move {
                Self::download_month_with_client(&sym, year, month, &client, tx).await
            }));
        }
        
        // Wait for all downloads
        let results = join_all(handles).await;
        
        let mut total_candles = 0u64;
        for result in results {
            match result {
                Ok(Ok(count)) => total_candles += count,
                Ok(Err(e)) => warn!("Download error: {}", e),
                Err(e) => warn!("Join error: {}", e),
            }
        }
        
        info!("{}: Downloaded {} candles from archive", symbol_upper, total_candles);
        Ok(total_candles)
    }

    /// Download a single month's klines using a specific client
    async fn download_month_with_client(
        symbol: &str,
        year: i32,
        month: u32,
        client: &Client,
        candle_tx: mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let url = format!(
            "{}/{}/1m/{}-1m-{}-{:02}.zip",
            BASE_URL, symbol, symbol, year, month
        );
        
        // Download ZIP file with proxy client
        let response = client.get(&url)
            .send()
            .await
            .map_err(|e| CollectorError::RestApiError(format!("Download failed: {}", e)))?;
        
        if !response.status().is_success() {
            return Err(CollectorError::RestApiError(
                format!("HTTP {}: {}", response.status(), url)
            ));
        }
        
        let bytes = response.bytes()
            .await
            .map_err(|e| CollectorError::RestApiError(format!("Read bytes failed: {}", e)))?;
        
        // Extract CSV from ZIP
        let cursor = Cursor::new(bytes);
        let mut archive = zip::ZipArchive::new(cursor)
            .map_err(|e| CollectorError::RestApiError(format!("ZIP error: {}", e)))?;
        
        let mut csv_content = String::new();
        {
            let mut file = archive.by_index(0)
                .map_err(|e| CollectorError::RestApiError(format!("ZIP file error: {}", e)))?;
            file.read_to_string(&mut csv_content)
                .map_err(|e| CollectorError::RestApiError(format!("Read CSV error: {}", e)))?;
        }
        
        // Parse CSV and send candles
        let count = Self::parse_csv(symbol, &csv_content, &candle_tx).await?;
        
        info!("{} {}-{:02}: {} candles", symbol, year, month, count);
        Ok(count)
    }
    
    /// Download all monthly klines (without proxy - original method)
    pub async fn download_symbol(
        symbol: &str,
        start_timestamp: i64,
        candle_tx: mpsc::Sender<CandleData>,
        max_parallel: usize,
    ) -> Result<u64, CollectorError> {
        let symbol_upper = symbol.to_uppercase();
        
        let start_date = Self::timestamp_to_date(start_timestamp);
        let now = Utc::now().naive_utc().date();
        let months = Self::generate_months(start_date, now);
        
        if months.is_empty() {
            info!("{}: No complete months to download", symbol_upper);
            return Ok(0);
        }
        
        info!(
            "{}: Downloading {} months from {}-{:02} to {}-{:02}",
            symbol_upper,
            months.len(),
            months.first().unwrap().0, months.first().unwrap().1,
            months.last().unwrap().0, months.last().unwrap().1
        );
        
        let mut total_candles = 0u64;
        
        for chunk in months.chunks(max_parallel) {
            let mut handles = Vec::new();
            
            for &(year, month) in chunk {
                let sym = symbol_upper.clone();
                let tx = candle_tx.clone();
                
                handles.push(tokio::spawn(async move {
                    Self::download_month(&sym, year, month, tx).await
                }));
            }
            
            let results = join_all(handles).await;
            
            for result in results {
                match result {
                    Ok(Ok(count)) => total_candles += count,
                    Ok(Err(e)) => warn!("Download error: {}", e),
                    Err(e) => warn!("Join error: {}", e),
                }
            }
        }
        
        info!("{}: Downloaded {} candles from archive", symbol_upper, total_candles);
        Ok(total_candles)
    }

    /// Download a single month (without proxy)
    async fn download_month(
        symbol: &str,
        year: i32,
        month: u32,
        candle_tx: mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let url = format!(
            "{}/{}/1m/{}-1m-{}-{:02}.zip",
            BASE_URL, symbol, symbol, year, month
        );
        
        let response = reqwest::get(&url)
            .await
            .map_err(|e| CollectorError::RestApiError(format!("Download failed: {}", e)))?;
        
        if !response.status().is_success() {
            return Err(CollectorError::RestApiError(
                format!("HTTP {}: {}", response.status(), url)
            ));
        }
        
        let bytes = response.bytes()
            .await
            .map_err(|e| CollectorError::RestApiError(format!("Read bytes failed: {}", e)))?;
        
        let cursor = Cursor::new(bytes);
        let mut archive = zip::ZipArchive::new(cursor)
            .map_err(|e| CollectorError::RestApiError(format!("ZIP error: {}", e)))?;
        
        let mut csv_content = String::new();
        {
            let mut file = archive.by_index(0)
                .map_err(|e| CollectorError::RestApiError(format!("ZIP file error: {}", e)))?;
            file.read_to_string(&mut csv_content)
                .map_err(|e| CollectorError::RestApiError(format!("Read CSV error: {}", e)))?;
        }
        
        let count = Self::parse_csv(symbol, &csv_content, &candle_tx).await?;
        
        info!("{} {}-{:02}: {} candles", symbol, year, month, count);
        Ok(count)
    }

    /// Parse CSV content and send candles through channel
    async fn parse_csv(
        symbol: &str,
        csv_content: &str,
        candle_tx: &mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(csv_content.as_bytes());
        
        let mut count = 0u64;
        
        for result in reader.records() {
            let record = match result {
                Ok(r) => r,
                Err(_) => continue,
            };
            
            if record.len() < 11 {
                continue;
            }
            
            let timestamp: i64 = record[0].parse().unwrap_or(0);
            let open: f64 = record[1].parse().unwrap_or(0.0);
            let high: f64 = record[2].parse().unwrap_or(0.0);
            let low: f64 = record[3].parse().unwrap_or(0.0);
            let close: f64 = record[4].parse().unwrap_or(0.0);
            let volume: f64 = record[5].parse().unwrap_or(0.0);
            let taker_buy_volume: f64 = record[9].parse().unwrap_or(0.0);
            
            let net_volume = taker_buy_volume * 2.0 - volume;
            
            let candle = CandleData {
                symbol: symbol.to_string(),
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                taker_buy_volume,
                net_volume,
                is_closed: true,
            };
            
            if candle_tx.send(candle).await.is_err() {
                break;
            }
            count += 1;
        }
        
        Ok(count)
    }
    
    fn timestamp_to_date(ts: i64) -> NaiveDate {
        let secs = ts / 1000;
        chrono::DateTime::from_timestamp(secs, 0)
            .unwrap_or_else(|| Utc::now())
            .naive_utc()
            .date()
    }
    
    fn generate_months(start: NaiveDate, end: NaiveDate) -> Vec<(i32, u32)> {
        let mut months = Vec::new();
        
        let mut year = start.year();
        let mut month = start.month();
        
        let end_year = if end.month() == 1 { end.year() - 1 } else { end.year() };
        let end_month = if end.month() == 1 { 12 } else { end.month() - 1 };
        
        loop {
            if year > end_year || (year == end_year && month > end_month) {
                break;
            }
            
            months.push((year, month));
            
            month += 1;
            if month > 12 {
                month = 1;
                year += 1;
            }
        }
        
        months
    }
    
    pub fn last_complete_month_end() -> i64 {
        let now = Utc::now().naive_utc();
        let first_of_this_month = NaiveDate::from_ymd_opt(now.year(), now.month(), 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        first_of_this_month.and_utc().timestamp_millis()
    }

    /// Download daily klines for a symbol (for current incomplete month)
    pub async fn download_days_with_clients(
        symbol: &str,
        start_timestamp: i64,
        clients: &[Arc<Client>],
        candle_tx: mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let symbol_upper = symbol.to_uppercase();
        
        if clients.is_empty() {
            return Err(CollectorError::RestApiError("No clients available".to_string()));
        }
        
        let start_date = Self::timestamp_to_date(start_timestamp);
        let yesterday = Utc::now().naive_utc().date().pred_opt().unwrap_or(start_date);
        
        let days = Self::generate_days(start_date, yesterday);
        
        if days.is_empty() {
            info!("{}: No days to download", symbol_upper);
            return Ok(0);
        }
        
        info!(
            "{}: Downloading {} days ({} to {}) with {} clients",
            symbol_upper, days.len(),
            days.first().unwrap(), days.last().unwrap(),
            clients.len()
        );
        
        let mut handles = Vec::new();
        
        for (i, date) in days.iter().enumerate() {
            let client = clients[i % clients.len()].clone();
            let sym = symbol_upper.clone();
            let tx = candle_tx.clone();
            let d = *date;
            
            handles.push(tokio::spawn(async move {
                Self::download_day_with_client(&sym, d, &client, tx).await
            }));
        }
        
        let results = join_all(handles).await;
        
        let mut total_candles = 0u64;
        for result in results {
            match result {
                Ok(Ok(count)) => total_candles += count,
                Ok(Err(e)) => warn!("Daily download error: {}", e),
                Err(e) => warn!("Join error: {}", e),
            }
        }
        
        info!("{}: Downloaded {} candles from daily archive", symbol_upper, total_candles);
        Ok(total_candles)
    }

    /// Download a single day's klines
    async fn download_day_with_client(
        symbol: &str,
        date: NaiveDate,
        client: &Client,
        candle_tx: mpsc::Sender<CandleData>,
    ) -> Result<u64, CollectorError> {
        let url = format!(
            "{}/{}/1m/{}-1m-{}.zip",
            DAILY_BASE_URL, symbol, symbol, date.format("%Y-%m-%d")
        );
        
        let response = client.get(&url)
            .send()
            .await
            .map_err(|e| CollectorError::RestApiError(format!("Download failed: {}", e)))?;
        
        if !response.status().is_success() {
            return Err(CollectorError::RestApiError(
                format!("HTTP {}: {}", response.status(), url)
            ));
        }
        
        let bytes = response.bytes()
            .await
            .map_err(|e| CollectorError::RestApiError(format!("Read bytes failed: {}", e)))?;
        
        let cursor = Cursor::new(bytes);
        let mut archive = zip::ZipArchive::new(cursor)
            .map_err(|e| CollectorError::RestApiError(format!("ZIP error: {}", e)))?;
        
        let mut csv_content = String::new();
        {
            let mut file = archive.by_index(0)
                .map_err(|e| CollectorError::RestApiError(format!("ZIP file error: {}", e)))?;
            file.read_to_string(&mut csv_content)
                .map_err(|e| CollectorError::RestApiError(format!("Read CSV error: {}", e)))?;
        }
        
        let count = Self::parse_csv(symbol, &csv_content, &candle_tx).await?;
        
        info!("{} {}: {} candles", symbol, date, count);
        Ok(count)
    }

    /// Generate list of days between start and end (inclusive)
    fn generate_days(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
        let mut days = Vec::new();
        let mut current = start;
        
        while current <= end {
            days.push(current);
            current = current.succ_opt().unwrap_or(end);
            if current == end && days.last() == Some(&end) {
                break;
            }
        }
        
        days
    }

    /// Get timestamp for start of current month (for daily download start point)
    pub fn current_month_start() -> i64 {
        let now = Utc::now().naive_utc();
        let first_of_this_month = NaiveDate::from_ymd_opt(now.year(), now.month(), 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        first_of_this_month.and_utc().timestamp_millis()
    }
}
