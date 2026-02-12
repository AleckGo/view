use crate::{
    DatabaseHandler, SchedulerCommand,
    CandleData, KlineQuery, ApiResponse, AddSymbolRequest, SchedulerStatus,
};

use axum::{
    Router,
    routing::{get, post, delete},
    extract::{Path, Query, State},
    response::Json,
    http::StatusCode,
};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub struct KlineChartState {
    pub db: Arc<DatabaseHandler>,
    pub command_tx: mpsc::Sender<SchedulerCommand>,
}

pub fn klinechart_routes() -> Router<Arc<KlineChartState>> {
    Router::new()
        .route("/api/klines/{symbol}", get(get_klines))
        .route("/api/symbols", get(get_symbols))
        .route("/api/symbols", post(add_symbol))
        .route("/api/symbols/{symbol}", delete(remove_symbol))
        .route("/api/status", get(get_status))
}

// GET /api/klines/{symbol}?limit=800&interval=1m&end_time=...
async fn get_klines(
    State(state): State<Arc<KlineChartState>>,
    Path(symbol): Path<String>,
    Query(query): Query<KlineQuery>,
) -> Json<ApiResponse<Vec<CandleData>>> {
    let limit = query.limit.unwrap_or(800);
    let interval = query.interval.unwrap_or_default();
    let end_time = query.end_time;
    
    match state.db.get_klines_aggregated(&symbol, interval, limit, end_time).await {
        Ok(candles) => Json(ApiResponse::ok(candles)),
        Err(e) => Json(ApiResponse::err(&e.to_string())),
    }
}

// GET /api/symbols
async fn get_symbols(
    State(state): State<Arc<KlineChartState>>,
) -> Json<ApiResponse<Vec<String>>> {
    match state.db.get_active_symbols().await {
        Ok(symbols) => Json(ApiResponse::ok(symbols)),
        Err(e) => Json(ApiResponse::err(&e.to_string())),
    }
}

// POST /api/symbols
async fn add_symbol(
    State(state): State<Arc<KlineChartState>>,
    Json(req): Json<AddSymbolRequest>,
) -> (StatusCode, Json<ApiResponse<String>>) {
    let result = state.command_tx.send(SchedulerCommand::AddSymbol {
        symbol: req.symbol.clone(),
        backfill_from: req.backfill_from,
    }).await;

    match result {
        Ok(_) => (StatusCode::OK, Json(ApiResponse::ok(format!("Adding symbol: {}", req.symbol)))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::err(&e.to_string()))),
    }
}

// DELETE /api/symbols/{symbol}
async fn remove_symbol(
    State(state): State<Arc<KlineChartState>>,
    Path(symbol): Path<String>,
) -> (StatusCode, Json<ApiResponse<String>>) {
    let result = state.command_tx.send(SchedulerCommand::RemoveSymbol {
        symbol: symbol.clone(),
    }).await;

    match result {
        Ok(_) => (StatusCode::OK, Json(ApiResponse::ok(format!("Removing symbol: {}", symbol)))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::err(&e.to_string()))),
    }
}

// GET /api/status
async fn get_status(
    State(state): State<Arc<KlineChartState>>,
) -> Json<ApiResponse<SchedulerStatus>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    
    let send_result = state.command_tx.send(SchedulerCommand::GetStatus { reply: reply_tx }).await;
    
    if send_result.is_err() {
        return Json(ApiResponse::err("Failed to send status request"));
    }

    match reply_rx.await {
        Ok(status) => Json(ApiResponse::ok(status)),
        Err(_) => Json(ApiResponse::err("Failed to receive status")),
    }
}
