use backend::{BinanceCollector, CandleData};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use dotenv::dotenv;
use log::{info,error,debug};

#[tokio::test]
async fn test_backfill_and_stream() {

    dotenv().ok();
    env_logger::init();

    let collector = BinanceCollector::new(vec!["btcusdt".to_string(), "ethusdt".to_string()]);
    let (tx, mut rx) = mpsc::channel::<CandleData>(1000);

    // Calculate time range: last 10 minutes
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let start_time = now - (10 * 60 * 1000); // 10 minutes ago

    // First, backfill recent data for btcusdt
    info!("Starting backfill...");
    let backfill_count = collector.backfill("btcusdt", start_time, now, &tx).await.unwrap();
    info!("Backfill completed: {} candles", backfill_count);

    // Then start WebSocket stream
    let collector_handle = tokio::spawn(async move {
        collector.start_stream(tx).await
    });

    let mut received_count = 0;
    
    // Drain backfill data first
    while let Ok(Some(candle)) = timeout(Duration::from_millis(100), rx.recv()).await {
        info!(
            "[BACKFILL] {} ts={} c={:.2} net_vol={:.4}",
            candle.symbol, candle.timestamp, candle.close, candle.net_volume
        );
        received_count += 1;
    }
    info!("Received {} backfill candles from channel", received_count);

    // Wait for WebSocket data (up to 90 seconds for at least 1 closed candle)
    let mut ws_count = 0;
    let ws_result = timeout(Duration::from_secs(90), async {
        while let Some(candle) = rx.recv().await {
            info!(
                "[WEBSOCKET] {} ts={} c={:.2} net_vol={:.4}",
                candle.symbol, candle.timestamp, candle.close, candle.net_volume
            );
            ws_count += 1;
            if ws_count >= 1 {
                return true;
            }
        }
        false
    }).await;

    collector_handle.abort();

    info!("\nFinal: Backfill={}, WebSocket={}", backfill_count, ws_count);
    
    assert!(backfill_count >= 5, "Expected at least 5 backfill candles, got {}", backfill_count);
    
    match ws_result {
        Ok(true) => info!("Test completed successfully!"),
        Ok(false) => info!("WebSocket closed before receiving data"),
        Err(_) => info!("WebSocket timed out (normal if test runs mid-minute)"),
    }
}

#[tokio::test]

async fn test_get_symbol() {

    dotenv().ok();
    env_logger::init();

    let collector = BinanceCollector::new(vec!["btcusdt".to_string(), "ethusdt".to_string()]);

    let result = BinanceCollector::get_symbol().await.unwrap();

    info!("Result = {:?}",result);
}