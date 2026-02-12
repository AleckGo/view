use backend::{BinanceCollector, CandleData};
use tokio::sync::mpsc;
use dotenv::dotenv;
use log::info;

#[tokio::test]
async fn test_build_clients() {
    dotenv().ok();
    env_logger::init();

    info!("Testing build_clients...");
    
    let clients = BinanceCollector::build_clients().await;
    
    info!("Built {} working clients", clients.len());
    assert!(clients.len() > 0, "Should have at least 1 working client");
}

#[tokio::test]
async fn test_sync_single_symbol() {
    dotenv().ok();
    let _ = env_logger::try_init();

    info!("Testing sync_from_scratch for single symbol...");

    // Build clients (all work together)
    let clients = BinanceCollector::build_clients().await;
    assert!(!clients.is_empty(), "Need at least 1 client");
    info!("Built {} clients", clients.len());

    // Create channel
    let (tx, mut rx) = mpsc::channel::<CandleData>(10000);

    // Sync last 5 minutes of BTCUSDT
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let start_time = now - (5 * 60 * 1000); // 5 minutes ago

    info!("Syncing BTCUSDT from {} to {}", start_time, now);

    let count = BinanceCollector::sync_from_scratch(
        "BTCUSDT".to_string(),
        start_time,
        clients,  // All clients work together
        tx,
    ).await.unwrap();

    info!("Sync returned {} candles", count);

    // Drain channel
    let mut received = 0;
    while let Ok(candle) = rx.try_recv() {
        info!(
            "Candle: {} ts={} o={:.2} h={:.2} l={:.2} c={:.2} nv={:.4}",
            candle.symbol, candle.timestamp, 
            candle.open, candle.high, candle.low, candle.close,
            candle.net_volume
        );
        received += 1;
    }

    info!("Received {} candles from channel", received);
    assert!(count >= 3, "Expected at least 3 candles for 5 min, got {}", count);
}


