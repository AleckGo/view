use backend::{BinanceCollector, CandleData};
use tokio::sync::mpsc;
use dotenv::dotenv;
use log::info;

#[tokio::test]
async fn test_sync_full_history_single_symbol() {
    dotenv().ok();
    env_logger::init();

    // 1. Get all symbols
    let symbol_vec = BinanceCollector::get_symbol().await.unwrap();
    info!("Got {} symbols", symbol_vec.len());

    // 2. Find ETHUSDT start time
    let test_symbol = "ETHUSDT";
    let test_start_time = symbol_vec
        .iter()
        .find(|item| item.symbol == test_symbol)
        .map(|item| item.start_timestamp)
        .expect("ETHUSDT not found");

    info!("{} start_timestamp: {}", test_symbol, test_start_time);

    // 3. Build clients (all clients work together on this symbol)
    let clients = BinanceCollector::build_clients().await;
    info!("Built {} clients", clients.len());
    assert!(!clients.is_empty(), "Need at least 1 client");

    // 4. Create channel
    let (tx, mut rx) = mpsc::channel::<CandleData>(100000);

    // 5. Sync from scratch (all clients work in parallel on different time segments)
    info!("Starting full history sync for {} with {} clients...", test_symbol, clients.len());
    
    let sync_handle = tokio::spawn(async move {
        BinanceCollector::sync_from_scratch(
            test_symbol.to_string(),
            test_start_time,
            clients,  // Pass all clients
            tx,
        ).await
    });

    // 6. Consume and count
    let mut count = 0u64;
    let mut last_ts = 0i64;
    
    while let Some(candle) = rx.recv().await {
        count += 1;
        last_ts = candle.timestamp;
        
        if count % 100000 == 0 {
            info!("Progress: {} candles, last_ts: {}", count, last_ts);
        }
    }

    let result = sync_handle.await.unwrap();
    
    info!("Sync result: {:?}", result);
    info!("Total received: {} candles", count);
    info!("Last timestamp: {}", last_ts);

    assert!(count > 0, "Should have synced some candles");
}
