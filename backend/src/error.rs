use thiserror::Error;

#[derive(Error, Debug)]
pub enum CollectorError {
    #[error("WebSocket connection failed: {0}")]
    ConnectionFailed(String),

    #[error("REST API request failed: {0}")]
    RestApiError(String),

    #[error("Data parsing error: {0}")]
    ParseError(String),

    #[error("Invalid kline data: {0}")]
    InvalidKlineData(String),

    #[error("Failed to get opened symbol, with error {0}")]
    GetSymbolError(String),
    
}

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Collector error: {0}")]
    CollectorError(String),

    #[error("Backfill error: {0}")]
    BackfillError(String),

    #[error("No active symbols")]
    NoActiveSymbols,
}

impl From<sqlx::Error> for SchedulerError {
    fn from(err: sqlx::Error) -> Self {
        SchedulerError::DatabaseError(err.to_string())
    }
}

impl From<CollectorError> for SchedulerError {
    fn from(err: CollectorError) -> Self {
        SchedulerError::CollectorError(err.to_string())
    }
}
