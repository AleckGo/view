use backend::{BinanceCollector, CandleData, DatabaseHandler, HistoricalDownloader};

use log::{info, error};
use dotenv::dotenv;
use tokio::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    // 1. Connect to database
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    
    info!("Connecting to database...");
    let db = Arc::new(
        DatabaseHandler::new(&database_url)
            .await
            .expect("Failed to connect to database")
    );

    // 2. Get all USDT symbols
    info!("Fetching all symbols from Binance...");
    let all_symbols = BinanceCollector::get_symbol().await.unwrap();
    let symbols: Vec<_> = all_symbols
        .into_iter()
        .filter(|s| s.symbol.ends_with("USDT"))
        .collect();
    info!("Got {} USDT symbols to sync", symbols.len());

    // 3. Build clients
    info!("Building download clients (with proxies)...");
    let download_clients = HistoricalDownloader::build_clients().await;
    
    info!("Building API clients (for recent data)...");
    let api_clients = BinanceCollector::build_clients().await;
    info!("Built {} download clients, {} API clients", download_clients.len(), api_clients.len());

    // Get last complete month timestamp
    let archive_end = HistoricalDownloader::last_complete_month_end();
    info!("Archive data available until: {}", archive_end);

    // Use the global cutoff timestamp
    info!("Data cutoff: 2024-01-01 00:00:00 UTC (timestamp: {})", backend::DATA_CUTOFF_TIMESTAMP);

    // 4. Sync each symbol
    let total_symbols = symbols.len();

    for (i, symbol) in symbols.into_iter().enumerate() {
        info!("========================================");
        info!("[{}/{}] {} - Starting fast sync", i + 1, total_symbols, symbol.symbol);

        // Check if we have data already
        let latest_ts = db.get_latest_timestamp(&symbol.symbol).await.ok().flatten();

        let start_time = match latest_ts {
            Some(ts) => {
                info!("[{}/{}] {} - Has data until {}, continuing from there",
                    i + 1, total_symbols, symbol.symbol, ts);
                ts + 60000
            }
            None => {
                // No existing data, use the later of symbol start or cutoff
                let actual_start = symbol.start_timestamp.max(backend::DATA_CUTOFF_TIMESTAMP);
                if symbol.start_timestamp < backend::DATA_CUTOFF_TIMESTAMP {
                    info!("[{}/{}] {} - Original start is before 2024-01-01, starting from cutoff instead",
                        i + 1, total_symbols, symbol.symbol);
                }
                actual_start
            }
        };

        // Create channels
        let (tx, rx) = mpsc::channel::<CandleData>(500000);
        let (db_tx, db_rx) = mpsc::channel::<CandleData>(500000);
        
        // Progress tracking
        let candle_count = Arc::new(AtomicU64::new(0));
        let candle_count_clone = candle_count.clone();
        let symbol_name = symbol.symbol.clone();
        let idx = i + 1;
        
        // Progress forwarder
        let progress_handle = tokio::spawn(async move {
            let mut rx = rx;
            let mut last_report = 0u64;
            
            while let Some(candle) = rx.recv().await {
                let count = candle_count_clone.fetch_add(1, Ordering::Relaxed) + 1;
                
                if count - last_report >= 500000 {
                    info!("[{}/{}] {} - Progress: {} candles", idx, total_symbols, symbol_name, count);
                    last_report = count;
                }
                
                let _ = db_tx.send(candle).await;
            }
        });

        // DB consumer (batch insert)
        let db_clone = db.clone();
        let consumer_handle = tokio::spawn(async move {
            db_clone.start_consumer(db_rx).await;
        });

        // STEP 1: Download historical data from archive (with proxies)
        if start_time < archive_end && !download_clients.is_empty() {
            info!("[{}/{}] {} - Downloading from archive with {} proxies...", 
                i + 1, total_symbols, symbol.symbol, download_clients.len());
            
            match HistoricalDownloader::download_symbol_with_clients(
                &symbol.symbol,
                start_time,
                &download_clients,
                tx.clone(),
            ).await {
                Ok(count) => info!("[{}/{}] {} - Archive download: {} candles", 
                    i + 1, total_symbols, symbol.symbol, count),
                Err(e) => error!("[{}/{}] {} - Archive download failed: {}", 
                    i + 1, total_symbols, symbol.symbol, e),
            }
        }

        // STEP 2: Sync recent data via API
        let api_start = archive_end.max(start_time);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        if now - api_start > 60000 && !api_clients.is_empty() {
            info!("[{}/{}] {} - Syncing recent data via API...", i + 1, total_symbols, symbol.symbol);
            
            match BinanceCollector::sync_from_scratch(
                symbol.symbol.clone(),
                api_start,
                api_clients.clone(),
                tx.clone(),
            ).await {
                Ok(count) => info!("[{}/{}] {} - API sync: {} candles", 
                    i + 1, total_symbols, symbol.symbol, count),
                Err(e) => error!("[{}/{}] {} - API sync failed: {}", 
                    i + 1, total_symbols, symbol.symbol, e),
            }
        }

        // Close channel and wait
        drop(tx);
        let _ = progress_handle.await;
        let _ = consumer_handle.await;

        let final_count = candle_count.load(Ordering::Relaxed);
        info!("[{}/{}] {} - Complete: {} total candles", 
            i + 1, total_symbols, symbol.symbol, final_count);
    }

    info!("========================================");
    info!("All symbols synced!");
}
