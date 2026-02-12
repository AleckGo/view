use backend::{
    Scheduler, DatabaseHandler, SchedulerCommand,
    create_command_channel,
};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
use dotenv::dotenv;
use log::info;
use std::env;

fn get_database_url() -> String {
    dotenv().ok();
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://quant:2Nr!Ya&oVvY5pp@172.18.0.2:5432/crypto_database".to_string()
    })
}

#[tokio::test]
async fn test_scheduler_startup_and_shutdown() {

    dotenv().ok();
    env_logger::try_init().ok();
    
    let db_url = get_database_url();
    let db = Arc::new(DatabaseHandler::new(&db_url).await.expect("Failed to connect to database"));
    
    let (command_tx, command_rx) = create_command_channel();
    let mut scheduler = Scheduler::new(db.clone(), command_rx, None);
    
    // Start scheduler in background
    let scheduler_handle = tokio::spawn(async move {
        scheduler.run().await;
    });
    
    // Give it time to start
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Send shutdown command
    command_tx.send(SchedulerCommand::Shutdown).await.expect("Failed to send shutdown");
    
    // Wait for scheduler to stop
    let result = timeout(Duration::from_secs(10), scheduler_handle).await;
    assert!(result.is_ok(), "Scheduler did not shutdown in time");
    
    info!("Scheduler startup and shutdown test passed");
}

#[tokio::test]
async fn test_get_status() {

    dotenv().ok();
    env_logger::try_init().ok();
    
    let db_url = get_database_url();
    let db = Arc::new(DatabaseHandler::new(&db_url).await.expect("Failed to connect to database"));
    
    let (command_tx, command_rx) = create_command_channel();
    let mut scheduler = Scheduler::new(db.clone(), command_rx, None);
    
    // Start scheduler
    let scheduler_handle = tokio::spawn(async move {
        scheduler.run().await;
    });
    
    // Give it time to start
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Request status
    let (reply_tx, reply_rx) = oneshot::channel();
    command_tx.send(SchedulerCommand::GetStatus { reply: reply_tx }).await.expect("Failed to send GetStatus");
    
    let status = timeout(Duration::from_secs(5), reply_rx).await
        .expect("Timeout waiting for status")
        .expect("Failed to receive status");
    
    info!("Scheduler status: {:?}", status);
    assert!(status.is_running || status.active_symbols.is_empty(), "Scheduler should be running or have no symbols");
    
    // Shutdown
    command_tx.send(SchedulerCommand::Shutdown).await.expect("Failed to send shutdown");
    let _ = timeout(Duration::from_secs(10), scheduler_handle).await;
    
    info!("GetStatus test passed");
}

#[tokio::test]
async fn test_add_and_remove_symbol() {

    dotenv().ok();
    env_logger::try_init().ok();
    
    let db_url = get_database_url();
    let db = Arc::new(DatabaseHandler::new(&db_url).await.expect("Failed to connect to database"));
    
    let test_symbol = "testscheduler";
    
    // Clean up first - remove test symbol if exists
    let _ = db.remove_symbol(test_symbol).await;
    
    let (command_tx, command_rx) = create_command_channel();
    let mut scheduler = Scheduler::new(db.clone(), command_rx, None);
    
    // Start scheduler
    let scheduler_handle = tokio::spawn(async move {
        scheduler.run().await;
    });
    
    // Give it time to start
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Add symbol (with recent backfill_from to avoid long backfill)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let recent_time = now - (5 * 60 * 1000); // 5 minutes ago
    
    info!("Adding test symbol: {}", test_symbol);
    command_tx.send(SchedulerCommand::AddSymbol { 
        symbol: test_symbol.to_string(),
        backfill_from: Some(recent_time),
    }).await.expect("Failed to send AddSymbol");
    
    // Wait for processing
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Check status
    let (reply_tx, reply_rx) = oneshot::channel();
    command_tx.send(SchedulerCommand::GetStatus { reply: reply_tx }).await.expect("Failed to send GetStatus");
    
    let status = timeout(Duration::from_secs(5), reply_rx).await
        .expect("Timeout")
        .expect("Failed to receive");
    
    info!("Status after add: {:?}", status);
    
    // Verify symbol was added
    let is_tracked = db.is_symbol_tracked(test_symbol).await.expect("Failed to check tracking");
    assert!(is_tracked, "Symbol should be tracked after AddSymbol");
    
    // Remove symbol
    info!("Removing test symbol: {}", test_symbol);
    command_tx.send(SchedulerCommand::RemoveSymbol { 
        symbol: test_symbol.to_string(),
    }).await.expect("Failed to send RemoveSymbol");
    
    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Verify symbol was removed
    let is_tracked = db.is_symbol_tracked(test_symbol).await.expect("Failed to check tracking");
    assert!(!is_tracked, "Symbol should not be tracked after RemoveSymbol");
    
    // Shutdown
    command_tx.send(SchedulerCommand::Shutdown).await.expect("Failed to send shutdown");
    let _ = timeout(Duration::from_secs(10), scheduler_handle).await;
    
    info!("Add and remove symbol test passed");
}

#[tokio::test]
async fn test_restart_collector() {

    dotenv().ok();
    env_logger::try_init().ok();
    
    let db_url = get_database_url();
    let db = Arc::new(DatabaseHandler::new(&db_url).await.expect("Failed to connect to database"));
    
    let (command_tx, command_rx) = create_command_channel();
    let mut scheduler = Scheduler::new(db.clone(), command_rx, None);
    
    // Start scheduler
    let scheduler_handle = tokio::spawn(async move {
        scheduler.run().await;
    });
    
    // Give it time to start
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Get initial status
    let (reply_tx, reply_rx) = oneshot::channel();
    command_tx.send(SchedulerCommand::GetStatus { reply: reply_tx }).await.unwrap();
    let status_before = reply_rx.await.unwrap();
    info!("Status before restart: {:?}", status_before);
    
    // Send restart command
    info!("Sending RestartCollector command");
    command_tx.send(SchedulerCommand::RestartCollector).await.expect("Failed to send RestartCollector");
    
    // Wait for restart
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Get status after restart
    let (reply_tx, reply_rx) = oneshot::channel();
    command_tx.send(SchedulerCommand::GetStatus { reply: reply_tx }).await.unwrap();
    let status_after = reply_rx.await.unwrap();
    info!("Status after restart: {:?}", status_after);
    
    // Shutdown
    command_tx.send(SchedulerCommand::Shutdown).await.expect("Failed to send shutdown");
    let _ = timeout(Duration::from_secs(10), scheduler_handle).await;
    
    info!("Restart collector test passed");
}

#[tokio::test]
async fn test_multiple_commands() {

    dotenv().ok();
    env_logger::try_init().ok();
    
    let db_url = get_database_url();
    let db = Arc::new(DatabaseHandler::new(&db_url).await.expect("Failed to connect to database"));
    
    let (command_tx, command_rx) = create_command_channel();
    let mut scheduler = Scheduler::new(db.clone(), command_rx, None);
    
    // Start scheduler
    let scheduler_handle = tokio::spawn(async move {
        scheduler.run().await;
    });
    
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Send multiple status requests rapidly
    for i in 0..5 {
        let (reply_tx, reply_rx) = oneshot::channel();
        command_tx.send(SchedulerCommand::GetStatus { reply: reply_tx }).await.unwrap();
        let status = reply_rx.await.unwrap();
        info!("Status request {}: {:?}", i, status);
    }
    
    // Shutdown
    command_tx.send(SchedulerCommand::Shutdown).await.expect("Failed to send shutdown");
    let _ = timeout(Duration::from_secs(10), scheduler_handle).await;
    
    info!("Multiple commands test passed");
}

#[tokio::test]
async fn test_real_btcusdt_collection() {

    dotenv().ok();
    env_logger::try_init().ok();
    
    let db_url = get_database_url();
    let db = Arc::new(DatabaseHandler::new(&db_url).await.expect("Failed to connect to database"));
    
    let symbol = "btcusdt";
    
    // Clean up - remove from tracking first
    let _ = db.remove_symbol(symbol).await;
    
    let (command_tx, command_rx) = create_command_channel();
    let mut scheduler = Scheduler::new(db.clone(), command_rx, None);
    
    // Start scheduler
    let scheduler_handle = tokio::spawn(async move {
        scheduler.run().await;
    });
    
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Calculate start time: 5 minutes ago
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let five_min_ago = now - (5 * 60 * 1000);
    
    info!("Adding BTCUSDT, backfill from {} (5 min ago)", five_min_ago);
    
    // Add BTCUSDT with backfill from 5 minutes ago
    command_tx.send(SchedulerCommand::AddSymbol { 
        symbol: symbol.to_string(),
        backfill_from: Some(five_min_ago),
    }).await.expect("Failed to send AddSymbol");
    
    // Wait for backfill to complete
    info!("Waiting for backfill...");
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // Check latest timestamp in database
    let latest_ts = db.get_latest_timestamp(symbol).await.expect("Failed to get timestamp");
    info!("Latest BTCUSDT timestamp in DB: {:?}", latest_ts);
    
    assert!(latest_ts.is_some(), "Should have BTCUSDT data in database");
    
    // Wait for 2 more candles (about 2 minutes + buffer)
    info!("Waiting for 2 live candles (~2.5 minutes)...");
    tokio::time::sleep(Duration::from_secs(150)).await;
    
    // Check new latest timestamp
    let new_latest_ts = db.get_latest_timestamp(symbol).await.expect("Failed to get timestamp");
    info!("New latest BTCUSDT timestamp: {:?}", new_latest_ts);
    
    assert!(new_latest_ts.unwrap() > latest_ts.unwrap(), "Should have received new candles");
    
    // Get status
    let (reply_tx, reply_rx) = oneshot::channel();
    command_tx.send(SchedulerCommand::GetStatus { reply: reply_tx }).await.unwrap();
    let status = reply_rx.await.unwrap();
    info!("Final status: {:?}", status);
    
    assert!(status.active_symbols.contains(&symbol.to_string()), "BTCUSDT should be in active symbols");
    
    // Clean up - remove symbol
    command_tx.send(SchedulerCommand::RemoveSymbol { 
        symbol: symbol.to_string(),
    }).await.expect("Failed to remove symbol");
    
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Shutdown
    command_tx.send(SchedulerCommand::Shutdown).await.expect("Failed to send shutdown");
    let _ = timeout(Duration::from_secs(10), scheduler_handle).await;
    
    info!("Real BTCUSDT collection test passed!");
    info!("Check database: SELECT * FROM klines_1m WHERE symbol = 'BTCUSDT' ORDER BY timestamp DESC LIMIT 10;");
}
