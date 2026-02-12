use crate::{DatabaseHandler, Interval, CandleData, WsMessage};

use axum::{
    Router,
    routing::{get, post, delete},
    extract::{Query, State, WebSocketUpgrade, ws::{Message, WebSocket}},
    response::{Json, IntoResponse},
    http::StatusCode,
};
use futures::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::path::PathBuf;
use tokio::sync::{broadcast, RwLock};
use tokio::fs;
use log::{info, warn, debug, error};

pub struct TradingViewState {
    pub db: Arc<DatabaseHandler>,
    pub candle_tx: broadcast::Sender<CandleData>,
}

impl TradingViewState {
    pub fn new(db: Arc<DatabaseHandler>) -> (Self, broadcast::Receiver<CandleData>) {
        let (candle_tx, candle_rx) = broadcast::channel(10000);
        (Self { db, candle_tx }, candle_rx)
    }

    /// Send a candle update to all connected WebSocket clients
    pub fn broadcast_candle(&self, candle: CandleData) {
        // Ignore errors (no subscribers)
        let _ = self.candle_tx.send(candle);
    }
}

pub fn tradingview_routes() -> Router<Arc<TradingViewState>> {
    Router::new()
        .route("/config", get(get_config))
        .route("/time", get(get_time))
        .route("/symbols", get(get_symbol_info))
        .route("/search", get(search_symbols))
        .route("/tracked-symbols", get(get_tracked_symbols))
        .route("/daily-opens", get(get_daily_opens))
        .route("/history", get(get_history))
        .route("/ws", get(ws_handler))
        // Canvas API
        .route("/canvas/list", get(canvas_list))
        .route("/canvas/load", get(canvas_load))
        .route("/canvas/save", post(canvas_save))
        .route("/canvas/delete", delete(canvas_delete))
}

// ============ UDF Response Types ============

#[derive(Serialize)]
struct UdfConfig {
    supported_resolutions: Vec<&'static str>,
    supports_group_request: bool,
    supports_marks: bool,
    supports_search: bool,
    supports_timescale_marks: bool,
}

#[derive(Serialize)]
struct UdfSymbolInfo {
    symbol: String,
    ticker: String,
    name: String,
    full_name: String,
    description: String,
    exchange: String,
    listed_exchange: String,
    #[serde(rename = "type")]
    symbol_type: String,
    currency_code: String,
    session: String,
    timezone: String,
    minmovement: i32,
    minmov: i32,
    minmovement2: i32,
    minmov2: i32,
    pricescale: i64,
    supported_resolutions: Vec<&'static str>,
    has_intraday: bool,
    has_daily: bool,
    has_weekly_and_monthly: bool,
    data_status: String,
}

#[derive(Serialize)]
struct UdfSearchResult {
    symbol: String,
    full_name: String,
    description: String,
    exchange: String,
    ticker: String,
    #[serde(rename = "type")]
    symbol_type: String,
}

#[derive(Serialize)]
#[serde(untagged)]
enum UdfHistoryResponse {
    Ok {
        s: String,  // "ok"
        t: Vec<i64>,      // timestamps (seconds)
        o: Vec<f64>,      // open
        h: Vec<f64>,      // high
        l: Vec<f64>,      // low
        c: Vec<f64>,      // close
        v: Vec<f64>,      // volume
        nv: Vec<f64>,     // net volume (custom)
        tbv: Vec<f64>,    // taker buy volume (custom)
    },
    NoData {
        s: String,  // "no_data"
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(rename = "nextTime")]
        next_time: Option<i64>,
    },
    Error {
        s: String,  // "error"
        errmsg: String,
    },
}

// ============ Query Parameters ============

#[derive(Deserialize)]
struct SymbolQuery {
    symbol: String,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct SearchQuery {
    query: Option<String>,
    #[serde(rename = "type")]
    symbol_type: Option<String>,
    exchange: Option<String>,
    limit: Option<i32>,
}

#[derive(Deserialize)]
struct HistoryQuery {
    symbol: String,
    resolution: String,
    from: i64,      // unix timestamp (seconds)
    to: i64,        // unix timestamp (seconds)
    countback: Option<i64>,
}

// ============ Handlers ============

// GET /config
async fn get_config() -> Json<UdfConfig> {
    Json(UdfConfig {
        supported_resolutions: vec!["1", "5", "15", "60", "240", "1D", "1W", "1M"],
        supports_group_request: false,
        supports_marks: false,
        supports_search: true,
        supports_timescale_marks: false,
    })
}

// GET /time
async fn get_time() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    now.to_string()
}

// GET /symbols?symbol=BTCUSDT
async fn get_symbol_info(
    Query(query): Query<SymbolQuery>,
) -> Json<UdfSymbolInfo> {
    let symbol = query.symbol.to_uppercase();
    
    Json(UdfSymbolInfo {
        symbol: symbol.clone(),
        ticker: symbol.clone(),
        name: symbol.clone(),
        full_name: format!("BINANCE:{}", symbol),
        description: symbol.clone(),
        exchange: "BINANCE".to_string(),
        listed_exchange: "BINANCE".to_string(),
        symbol_type: "crypto".to_string(),
        currency_code: "USDT".to_string(),
        session: "24x7".to_string(),
        timezone: "Etc/UTC".to_string(),
        minmovement: 1,
        minmov: 1,
        minmovement2: 0,
        minmov2: 0,
        pricescale: 100000000,  // 8 decimal places for crypto
        supported_resolutions: vec!["1", "5", "15", "60", "240", "1D", "1W", "1M"],
        has_intraday: true,
        has_daily: true,
        has_weekly_and_monthly: true,
        data_status: "streaming".to_string(),
    })
}

// GET /tracked-symbols - 返回 TRACKED_SYMBOL 环境变量中配置的 symbols
async fn get_tracked_symbols() -> Json<Vec<String>> {
    Json(crate::DatabaseHandler::get_symbols_from_env())
}

// GET /daily-opens - 返回所有 symbol 当天 UTC 00:00 的开盘价
async fn get_daily_opens(
    State(state): State<Arc<TradingViewState>>,
) -> Json<std::collections::HashMap<String, f64>> {
    match state.db.get_daily_opens().await {
        Ok(opens) => Json(opens),
        Err(_) => Json(std::collections::HashMap::new()),
    }
}

// GET /search?query=BTC&limit=10
async fn search_symbols(
    State(state): State<Arc<TradingViewState>>,
    Query(query): Query<SearchQuery>,
) -> Json<Vec<UdfSearchResult>> {
    let search_term = query.query.unwrap_or_default().to_uppercase();
    let limit = query.limit.unwrap_or(30) as usize;

    let symbols = match state.db.get_active_symbols().await {
        Ok(s) => s,
        Err(_) => return Json(vec![]),
    };

    let results: Vec<UdfSearchResult> = symbols
        .into_iter()
        .filter(|s| search_term.is_empty() || s.to_uppercase().contains(&search_term))
        .take(limit)
        .map(|s| {
            let upper = s.to_uppercase();
            UdfSearchResult {
                symbol: upper.clone(),
                full_name: format!("BINANCE:{}", upper),
                description: upper.clone(),
                exchange: "BINANCE".to_string(),
                ticker: upper,
                symbol_type: "crypto".to_string(),
            }
        })
        .collect();

    Json(results)
}

// GET /history?symbol=BTCUSDT&resolution=1&from=...&to=...
async fn get_history(
    State(state): State<Arc<TradingViewState>>,
    Query(query): Query<HistoryQuery>,
) -> Json<UdfHistoryResponse> {
    let symbol = query.symbol.to_uppercase();
    
    // Convert resolution string to Interval enum
    let interval = match query.resolution.as_str() {
        "1" => Interval::Min1,
        "5" => Interval::Min5,
        "15" => Interval::Min15,
        "60" => Interval::Hour1,
        "240" => Interval::Hour4,
        "D" | "1D" => Interval::Day1,
        "W" | "1W" => Interval::Week1,
        "M" | "1M" => Interval::Month1,
        _ => Interval::Min1,
    };

    // Convert from/to (seconds) to milliseconds for database query
    let from_ms = query.from * 1000;
    let to_ms = query.to * 1000;
    
    // Calculate limit based on countback or time range
    let limit = query.countback.unwrap_or(1000);

    match state.db.get_klines_aggregated(&symbol, interval, limit, Some(to_ms)).await {
        Ok(candles) => {
            // Filter by from_ms and convert to UDF format
            let filtered: Vec<_> = candles
                .into_iter()
                .filter(|c| c.timestamp >= from_ms && c.timestamp <= to_ms)
                .collect();

            if filtered.is_empty() {
                return Json(UdfHistoryResponse::NoData {
                    s: "no_data".to_string(),
                    next_time: None,
                });
            }

            let t: Vec<i64> = filtered.iter().map(|c| c.timestamp / 1000).collect();
            let o: Vec<f64> = filtered.iter().map(|c| c.open).collect();
            let h: Vec<f64> = filtered.iter().map(|c| c.high).collect();
            let l: Vec<f64> = filtered.iter().map(|c| c.low).collect();
            let c: Vec<f64> = filtered.iter().map(|c| c.close).collect();
            let v: Vec<f64> = filtered.iter().map(|c| c.volume).collect();
            let nv: Vec<f64> = filtered.iter().map(|c| c.net_volume).collect();
            let tbv: Vec<f64> = filtered.iter().map(|c| c.taker_buy_volume).collect();

            Json(UdfHistoryResponse::Ok {
                s: "ok".to_string(),
                t, o, h, l, c, v, nv, tbv,
            })
        }
        Err(e) => Json(UdfHistoryResponse::Error {
            s: "error".to_string(),
            errmsg: e.to_string(),
        }),
    }
}

// ============ WebSocket Handler ============

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<TradingViewState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state))
}

async fn handle_ws_connection(socket: WebSocket, state: Arc<TradingViewState>) {
    let (mut sender, mut receiver) = socket.split();

    // Subscribe to broadcast channel
    let mut candle_rx = state.candle_tx.subscribe();

    // Subscribed symbols for this client
    let subscribed_symbols: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));
    let subscribed_symbols_clone = subscribed_symbols.clone();

    info!("WebSocket client connected");

    // Task to receive messages from client
    let recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    // Parse incoming message
                    if let Ok(ws_msg) = serde_json::from_str::<WsMessage>(&text) {
                        match ws_msg {
                            WsMessage::Subscribe { symbols } => {
                                let mut subs = subscribed_symbols_clone.write().await;
                                for s in symbols {
                                    subs.insert(s.to_uppercase());
                                }
                                debug!("Client subscribed to {} symbols", subs.len());
                            }
                            WsMessage::Unsubscribe { symbols } => {
                                let mut subs = subscribed_symbols_clone.write().await;
                                for s in symbols {
                                    subs.remove(&s.to_uppercase());
                                }
                            }
                            WsMessage::Ping => {
                                // Pong is handled by the send task
                            }
                            _ => {}
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("WebSocket client disconnected");
                    break;
                }
                Err(e) => {
                    warn!("WebSocket receive error: {}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Task to send messages to client
    let send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                // Forward candle updates to client
                result = candle_rx.recv() => {
                    match result {
                        Ok(candle) => {
                            let subs = subscribed_symbols.read().await;
                            // Send to client if subscribed or if subscribed to all (empty set means all)
                            if subs.is_empty() || subs.contains(&candle.symbol.to_uppercase()) {
                                let msg = WsMessage::Kline(candle);
                                if let Ok(json) = serde_json::to_string(&msg) {
                                    if sender.send(Message::Text(json.into())).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("WebSocket client lagged {} messages", n);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                }
            }
        }
    });

    // Wait for either task to finish
    tokio::select! {
        _ = recv_task => {}
        _ = send_task => {}
    }

    info!("WebSocket connection closed");
}

// ============ Canvas API ============

const STORAGE_DIR: &str = "storage";
const DEFAULT_USER: &str = "default";

#[derive(Deserialize)]
struct CanvasListQuery {
    symbol: String,
}

#[derive(Deserialize)]
struct CanvasLoadQuery {
    symbol: String,
    name: String,
}

#[derive(Deserialize)]
struct CanvasSaveBody {
    symbol: String,
    name: String,
    data: serde_json::Value,
}

#[derive(Deserialize)]
struct CanvasDeleteQuery {
    symbol: String,
    name: String,
}

#[derive(Serialize)]
struct CanvasListResponse {
    canvases: Vec<String>,
}

fn get_user_id(headers: &axum::http::HeaderMap) -> String {
    headers.get("X-User-Id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_USER)
        .to_string()
}

fn get_canvas_dir(user_id: &str, symbol: &str) -> PathBuf {
    PathBuf::from(STORAGE_DIR)
        .join(user_id)
        .join(symbol.to_uppercase())
}

fn get_canvas_path(user_id: &str, symbol: &str, name: &str) -> PathBuf {
    get_canvas_dir(user_id, symbol).join(format!("{}.json", name))
}

// GET /canvas/list?symbol=BTCUSDT
async fn canvas_list(headers: axum::http::HeaderMap, Query(query): Query<CanvasListQuery>) -> Json<CanvasListResponse> {
    let user_id = get_user_id(&headers);
    let dir = get_canvas_dir(&user_id, &query.symbol);
    let mut canvases = Vec::new();
    
    if let Ok(mut entries) = fs::read_dir(&dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with(".json") {
                    canvases.push(name.trim_end_matches(".json").to_string());
                }
            }
        }
    }
    
    canvases.sort();
    Json(CanvasListResponse { canvases })
}

// GET /canvas/load?symbol=BTCUSDT&name=default
async fn canvas_load(
    headers: axum::http::HeaderMap,
    Query(query): Query<CanvasLoadQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let user_id = get_user_id(&headers);
    let path = get_canvas_path(&user_id, &query.symbol, &query.name);
    
    match fs::read_to_string(&path).await {
        Ok(content) => {
            match serde_json::from_str(&content) {
                Ok(data) => Ok(Json(data)),
                Err(e) => {
                    error!("Failed to parse canvas {}: {}", path.display(), e);
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
        Err(_) => Err(StatusCode::NOT_FOUND),
    }
}

// POST /canvas/save
async fn canvas_save(
    headers: axum::http::HeaderMap,
    Json(body): Json<CanvasSaveBody>,
) -> Result<StatusCode, StatusCode> {
    let user_id = get_user_id(&headers);
    let dir = get_canvas_dir(&user_id, &body.symbol);
    let path = get_canvas_path(&user_id, &body.symbol, &body.name);
    
    // Create directory if not exists
    if let Err(e) = fs::create_dir_all(&dir).await {
        error!("Failed to create dir {}: {}", dir.display(), e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    
    // Write canvas data
    let content = serde_json::to_string_pretty(&body.data)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    fs::write(&path, content).await
        .map_err(|e| {
            error!("Failed to write canvas {}: {}", path.display(), e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    
    info!("Saved canvas: {}", path.display());
    Ok(StatusCode::OK)
}

// DELETE /canvas/delete?symbol=BTCUSDT&name=default
async fn canvas_delete(
    headers: axum::http::HeaderMap,
    Query(query): Query<CanvasDeleteQuery>,
) -> StatusCode {
    let user_id = get_user_id(&headers);
    let path = get_canvas_path(&user_id, &query.symbol, &query.name);
    
    match fs::remove_file(&path).await {
        Ok(_) => {
            info!("Deleted canvas: {}", path.display());
            StatusCode::OK
        }
        Err(_) => StatusCode::NOT_FOUND,
    }
}
