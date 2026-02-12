use backend::{BinanceCollector, CandleData, DatabaseHandler};

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

    // 2. Get all symbols (filter USDT pairs only)
    info!("Fetching all symbols from Binance...");
    let all_symbols = BinanceCollector::get_symbol().await.unwrap();
    let symbols: Vec<_> = all_symbols
        .into_iter()
        .filter(|s| s.symbol.ends_with("USDT"))
        .collect();
    info!("Got {} USDT symbols to sync", symbols.len());

    // 3. Build clients
    info!("Building clients...");
    let clients = BinanceCollector::build_clients().await;
    info!("Built {} working clients", clients.len());

    if clients.is_empty() {
        error!("No working clients, exiting");
        return;
    }

    // 4. Sync each symbol one by one
    let total_symbols = symbols.len();
    
    for (i, symbol) in symbols.into_iter().enumerate() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        let start_time = symbol.start_timestamp;
        let total_range_ms = now - start_time;
        // Estimate total candles (1 candle per minute)
        let estimated_total = (total_range_ms / 60000) as u64;
        
        info!("[{}/{}] Starting sync for {} (estimated {} candles)", 
            i + 1, total_symbols, symbol.symbol, estimated_total);

        // Create channels
        let (tx, rx) = mpsc::channel::<CandleData>(100000);
        let (db_tx, db_rx) = mpsc::channel::<CandleData>(100000);

        // Progress tracking
        let candle_count = Arc::new(AtomicU64::new(0));
        let last_progress = Arc::new(AtomicU64::new(0));

        // Progress monitor and forward task
        let symbol_name = symbol.symbol.clone();
        let candle_count_clone = candle_count.clone();
        let last_progress_clone = last_progress.clone();
        let idx = i + 1;
        
        let progress_handle = tokio::spawn(async move {
            let mut rx = rx;
            while let Some(candle) = rx.recv().await {
                let count = candle_count_clone.fetch_add(1, Ordering::Relaxed) + 1;
                
                // Calculate progress based on received candles vs estimated total
                let progress = if estimated_total > 0 {
                    ((count as f64 / estimated_total as f64) * 100.0).min(100.0) as u64
                } else {
                    100
                };
                
                // Report every 20%
                let last = last_progress_clone.load(Ordering::Relaxed);
                let milestone = (progress / 20) * 20;
                if milestone > last && milestone <= 100 {
                    if last_progress_clone.compare_exchange(
                        last, milestone, Ordering::Relaxed, Ordering::Relaxed
                    ).is_ok() {
                        info!("[{}/{}] {} progress: {}% ({}/{} candles)", 
                            idx, total_symbols, symbol_name, milestone, count, estimated_total);
                    }
                }
                
                // Forward to database
                let _ = db_tx.send(candle).await;
            }
        });

        // Database consumer
        let db_clone = db.clone();
        let consumer_handle = tokio::spawn(async move {
            db_clone.start_consumer(db_rx).await;
        });

        // Sync this symbol
        let result = BinanceCollector::sync_from_scratch(
            symbol.symbol.clone(),
            symbol.start_timestamp,
            clients.clone(),
            tx,
        ).await;

        // Wait for tasks to finish
        let _ = progress_handle.await;
        let _ = consumer_handle.await;

        let final_count = candle_count.load(Ordering::Relaxed);
        
        match result {
            Ok(count) => {
                info!("[{}/{}] {} completed: {} candles", 
                    i + 1, total_symbols, symbol.symbol, count);
            }
            Err(e) => {
                error!("[{}/{}] {} failed: {} (received: {} candles)", 
                    i + 1, total_symbols, symbol.symbol, e, final_count);
            }
        }
    }

    info!("All symbols synced!");
}
