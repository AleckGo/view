use backend::{
    DatabaseHandler, Scheduler, create_command_channel,
    KlineChartState, klinechart_routes,
    TradingViewState, tradingview_routes,
    CandleData,
};

use axum::Router;
use tower_http::cors::{CorsLayer, Any};
use axum::http::Method;
use std::sync::Arc;
use log::info;
use dotenv::*;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {

    dotenv().ok();
    env_logger::init();

    info!("Current Version 1.4");

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://quant:2Nr!Ya&oVvY5pp@172.18.0.2:5432/crypto_database".to_string());

    info!("Connecting to database...");
    let db = Arc::new(
        DatabaseHandler::new(&database_url)
            .await
            .expect("Failed to connect to database")
    );

    let (command_tx, command_rx) = create_command_channel();

    // Create TradingView state with broadcast channel
    let (tradingview_state, _) = TradingViewState::new(db.clone());
    let tradingview_state = Arc::new(tradingview_state);

    // Create channel for WebSocket broadcasts
    let (ws_broadcast_tx, mut ws_broadcast_rx) = mpsc::channel::<CandleData>(10000);

    // Forward candles to TradingView broadcast
    let tv_state_clone = tradingview_state.clone();
    tokio::spawn(async move {
        while let Some(candle) = ws_broadcast_rx.recv().await {
            tv_state_clone.broadcast_candle(candle);
        }
    });

    // Start scheduler in background with ws_broadcast_tx
    let scheduler_db = db.clone();
    tokio::spawn(async move {
        let mut scheduler = Scheduler::new(scheduler_db, command_rx, Some(ws_broadcast_tx));
        scheduler.run().await;
    });

    // KlineChart state (for klinechart frontend)
    let klinechart_state = Arc::new(KlineChartState {
        db: db.clone(),
        command_tx,
    });

    // CORS for frontend
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_headers(Any);

    // Merge routes from both modules
    let app = Router::new()
        .merge(klinechart_routes().with_state(klinechart_state))
        .merge(tradingview_routes().with_state(tradingview_state))
        .layer(cors);

    let addr = "0.0.0.0:3000";
    info!("Starting API server on {}", addr);
    info!("KlineChart API: /api/klines, /api/symbols, /api/status");
    info!("TradingView UDF: /config, /symbols, /search, /history, /time");
    
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
