use backend::{DatabaseHandler, CandleData};
use tokio::sync::mpsc;
use dotenv::dotenv;
use log::info;
use std::env;

fn get_database_url() -> String {

    dotenv().ok();
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://quant:2Nr!Ya&oVvY5pp@172.18.0.10:5432/crypto_database".to_string()
    })
}

#[tokio::test]
async fn test_database_connection() {

    dotenv().ok();
    env_logger::init();
    
    let db_url = get_database_url();
    info!("Connecting to database...");
    
    let db = DatabaseHandler::new(&db_url).await;
    assert!(db.is_ok(), "Failed to connect to database: {:?}", db.err());
    
    info!("Database connection successful!");
}

#[tokio::test]
async fn test_get_active_symbols() {

    dotenv().ok();
    env_logger::init();

    let db_url = get_database_url();
    let db = DatabaseHandler::new(&db_url).await.expect("Failed to connect");
    
    let symbols = db.get_active_symbols().await;
    info!("Active symbols: {:?}", symbols);
    
    assert!(symbols.is_ok(), "Failed to get active symbols: {:?}", symbols.err());
}

#[tokio::test]
async fn test_insert_and_query() {

    dotenv().ok();
    env_logger::init();

    let db_url = get_database_url();
    let db = DatabaseHandler::new(&db_url).await.expect("Failed to connect");
    
    // Create test candle
    let test_candle = CandleData {
        symbol: "TESTUSDT".to_string(),
        timestamp: 1700000000000, // Fixed test timestamp
        open: 100.0,
        high: 105.0,
        low: 99.0,
        close: 102.0,
        volume: 1000.0,
        taker_buy_volume: 600.0,
        net_volume: 200.0,
        is_closed: true,
    };
    
    // Insert
    let result = db.insert_candle(&test_candle).await;
    assert!(result.is_ok(), "Failed to insert candle: {:?}", result.err());
    info!("Inserted test candle");
    
    // Query latest timestamp
    let latest = db.get_latest_timestamp("TESTUSDT").await;
    assert!(latest.is_ok(), "Failed to get latest timestamp: {:?}", latest.err());
    
    let ts = latest.unwrap();
    assert!(ts.is_some(), "No timestamp found for TESTUSDT");
    assert_eq!(ts.unwrap(), 1700000000000, "Timestamp mismatch");
    
    info!("Latest timestamp for TESTUSDT: {:?}", ts);
}

#[tokio::test]
async fn test_batch_consumer() {

    dotenv().ok();
    env_logger::init();

    let db_url = get_database_url();
    let db = DatabaseHandler::new(&db_url).await.expect("Failed to connect");
    
    let (tx, rx) = mpsc::channel::<CandleData>(100);
    
    // Spawn consumer
    let db_handle = tokio::spawn(async move {
        db.start_consumer(rx).await;
    });
    
    // Send test candles
    let base_ts = 1700000100000i64;
    for i in 0..10 {
        let candle = CandleData {
            symbol: "BATCHTEST".to_string(),
            timestamp: base_ts + (i * 60000),
            open: 100.0 + i as f64,
            high: 105.0 + i as f64,
            low: 99.0 + i as f64,
            close: 102.0 + i as f64,
            volume: 1000.0,
            taker_buy_volume: 600.0,
            net_volume: 200.0,
            is_closed: true,
        };
        tx.send(candle).await.unwrap();
    }
    
    // Close channel to trigger flush
    drop(tx);
    
    // Wait for consumer to finish
    let _ = tokio::time::timeout(
        tokio::time::Duration::from_secs(10),
        db_handle
    ).await;
    
    info!("Batch insert test completed");
}
