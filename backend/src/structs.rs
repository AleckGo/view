use serde::{Serialize, Deserialize};
use tokio::sync::oneshot;

/// Data cutoff timestamp: 2024-01-01 00:00:00 UTC
/// Data before this timestamp will not be synced
pub const DATA_CUTOFF_TIMESTAMP: i64 = 1704067200000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleData {
    pub symbol: String,
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub taker_buy_volume: f64,
    pub net_volume: f64,
    pub is_closed: bool,
}

impl CandleData {
    pub fn calculate_net_volume(total_volume: f64, taker_buy_volume: f64) -> f64 {
        2.0 * taker_buy_volume - total_volume
    }
}

/// Commands for controlling the Scheduler
pub enum SchedulerCommand {
    /// Add a new symbol to track. Triggers backfill then collector restart.
    AddSymbol { 
        symbol: String, 
        backfill_from: Option<i64>,  // None = use EARLIEST_TIME
    },
    
    /// Remove a symbol from tracking. Triggers collector restart.
    RemoveSymbol { symbol: String },
    
    /// Restart the collector with current active symbols from database
    RestartCollector,
    
    /// Get current scheduler status
    GetStatus { reply: oneshot::Sender<SchedulerStatus> },
    
    /// Graceful shutdown
    Shutdown,
}

/// Scheduler status for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerStatus {
    pub is_running: bool,
    pub active_symbols: Vec<String>,
    pub collector_connected: bool,
}

// API Request/Response structs

#[derive(Debug, Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Interval {
    #[serde(rename = "1m")]
    Min1,
    #[serde(rename = "5m")]
    Min5,
    #[serde(rename = "15m")]
    Min15,
    #[serde(rename = "1h")]
    Hour1,
    #[serde(rename = "4h")]
    Hour4,
    #[serde(rename = "1d")]
    Day1,
    #[serde(rename = "1w")]
    Week1,
    #[serde(rename = "1M")]
    Month1,
}

impl Default for Interval {
    fn default() -> Self {
        Interval::Min1
    }
}

#[derive(Deserialize)]
pub struct KlineQuery {
    pub limit: Option<i64>,
    pub interval: Option<Interval>,
    pub end_time: Option<i64>,
}

#[derive(Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self { success: true, data: Some(data), error: None }
    }
    
    pub fn err(msg: &str) -> Self {
        Self { success: false, data: None, error: Some(msg.to_string()) }
    }
}

#[derive(Deserialize)]
pub struct AddSymbolRequest {
    pub symbol: String,
    pub backfill_from: Option<i64>,
}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct Symbol{

    pub symbol: String,
    pub start_timestamp: i64,
}

// WebSocket message types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum WsMessage {
    /// Kline update for a symbol
    #[serde(rename = "kline")]
    Kline(CandleData),

    /// Ticker update with price info for watchlist
    #[serde(rename = "ticker")]
    Ticker(TickerUpdate),

    /// Subscribe to symbols
    #[serde(rename = "subscribe")]
    Subscribe { symbols: Vec<String> },

    /// Unsubscribe from symbols
    #[serde(rename = "unsubscribe")]
    Unsubscribe { symbols: Vec<String> },

    /// Ping/Pong for keepalive
    #[serde(rename = "ping")]
    Ping,

    #[serde(rename = "pong")]
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerUpdate {
    pub symbol: String,
    pub price: f64,
    pub change_24h: f64,
    pub change_percent_24h: f64,
    pub volume_24h: f64,
    pub timestamp: i64,
}
