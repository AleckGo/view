use crate::structs::*;

use log::{debug, error, info};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};

const BUFFER_SIZE: usize = 100;
const FLUSH_INTERVAL_SECS: u64 = 5;

pub struct DatabaseHandler {
    pool: PgPool,
}

impl DatabaseHandler {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .min_connections(5)
            .connect(database_url)
            .await?;

        info!("Database connected (pool: 5-20 connections)");

        Ok(Self { pool })
    }

    /// Start consuming from rx and batch insert into database
    pub async fn start_consumer(&self, mut rx: mpsc::Receiver<CandleData>) {
        let mut buffer: Vec<CandleData> = Vec::with_capacity(BUFFER_SIZE);
        let mut flush_timer = interval(Duration::from_secs(FLUSH_INTERVAL_SECS));

        info!("Database consumer started");

        loop {
            tokio::select! {
                // Receive candle data
                candle = rx.recv() => {
                    match candle {
                        Some(c) => {
                            buffer.push(c);
                            if buffer.len() >= BUFFER_SIZE {
                                self.flush_buffer(&mut buffer).await;
                            }
                        }
                        None => {
                            // Channel closed, flush remaining and exit
                            if !buffer.is_empty() {
                                self.flush_buffer(&mut buffer).await;
                            }
                            info!("Database consumer stopped (channel closed)");
                            break;
                        }
                    }
                }
                // Flush on timer
                _ = flush_timer.tick() => {
                    if !buffer.is_empty() {
                        self.flush_buffer(&mut buffer).await;
                    }
                }
            }
        }
    }

    async fn flush_buffer(&self, buffer: &mut Vec<CandleData>) {
        if buffer.is_empty() {
            return;
        }

        let count = buffer.len();
        
        match self.batch_insert(buffer).await {
            Ok(_) => {
                debug!("Inserted {} candles", count);
            }
            Err(e) => {
                error!("Failed to insert {} candles: {}", count, e);
            }
        }
        
        buffer.clear();
    }

    async fn batch_insert(&self, candles: &[CandleData]) -> Result<(), sqlx::Error> {
        if candles.is_empty() {
            return Ok(());
        }

        // Deduplicate by (symbol, timestamp) - keep last occurrence
        use std::collections::HashMap;
        let mut dedup_map: HashMap<(String, i64), &CandleData> = HashMap::new();
        for c in candles {
            dedup_map.insert((c.symbol.clone(), c.timestamp), c);
        }
        let unique_candles: Vec<&CandleData> = dedup_map.into_values().collect();

        if unique_candles.is_empty() {
            return Ok(());
        }

        // Build batch insert query
        let mut query = String::from(
            "INSERT INTO klines_1m (symbol, timestamp, open, high, low, close, volume, taker_buy_volume, net_volume) VALUES "
        );

        let mut values: Vec<String> = Vec::with_capacity(unique_candles.len());
        
        for (i, _) in unique_candles.iter().enumerate() {
            let idx = i * 9;
            values.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                idx + 1, idx + 2, idx + 3, idx + 4, idx + 5, idx + 6, idx + 7, idx + 8, idx + 9
            ));
        }
        
        query.push_str(&values.join(", "));
        query.push_str(" ON CONFLICT (symbol, timestamp) DO UPDATE SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume, taker_buy_volume = EXCLUDED.taker_buy_volume, net_volume = EXCLUDED.net_volume");

        let mut query_builder = sqlx::query(&query);
        
        for c in unique_candles {
            query_builder = query_builder
                .bind(&c.symbol)
                .bind(c.timestamp)
                .bind(c.open)
                .bind(c.high)
                .bind(c.low)
                .bind(c.close)
                .bind(c.volume)
                .bind(c.taker_buy_volume)
                .bind(c.net_volume);
        }

        query_builder.execute(&self.pool).await?;
        
        Ok(())
    }

    /// Get latest timestamp for a symbol
    pub async fn get_latest_timestamp(&self, symbol: &str) -> Result<Option<i64>, sqlx::Error> {
        let row: Option<(Option<i64>,)> = sqlx::query_as(
            "SELECT MAX(timestamp) FROM klines_1m WHERE symbol = $1"
        )
        .bind(symbol.to_uppercase())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.and_then(|r| r.0))
    }

    /// Get today's UTC 00:00 open price for all tracked symbols
    pub async fn get_daily_opens(&self) -> Result<std::collections::HashMap<String, f64>, sqlx::Error> {
        use chrono::Utc;
        let now = Utc::now();
        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
        
        let rows: Vec<(String, f64)> = sqlx::query_as(
            "SELECT symbol, open FROM klines_1m WHERE timestamp = $1"
        )
        .bind(today_start)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().collect())
    }

    /// Get all active symbols from tracked_symbols table
    pub async fn get_active_symbols(&self) -> Result<Vec<String>, sqlx::Error> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT symbol FROM tracked_symbols WHERE is_active = TRUE"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// Get tracked symbols from TRACKED_SYMBOL env variable
    /// Format: TRACKED_SYMBOL=[BTCUSDT,ETHUSDT,BNBUSDT]
    pub fn get_symbols_from_env() -> Vec<String> {
        std::env::var("TRACKED_SYMBOL")
            .unwrap_or_default()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Insert candle directly (for single inserts)
    pub async fn insert_candle(&self, candle: &CandleData) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO klines_1m (symbol, timestamp, open, high, low, close, volume, taker_buy_volume, net_volume) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (symbol, timestamp) DO UPDATE SET 
             open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, 
             close = EXCLUDED.close, volume = EXCLUDED.volume, 
             taker_buy_volume = EXCLUDED.taker_buy_volume, net_volume = EXCLUDED.net_volume"
        )
        .bind(&candle.symbol)
        .bind(candle.timestamp)
        .bind(candle.open)
        .bind(candle.high)
        .bind(candle.low)
        .bind(candle.close)
        .bind(candle.volume)
        .bind(candle.taker_buy_volume)
        .bind(candle.net_volume)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Add a new symbol to tracked_symbols
    pub async fn add_symbol(&self, symbol: &str) -> Result<(), sqlx::Error> {
        let symbol_lower = symbol.to_lowercase();
        
        sqlx::query(
            "INSERT INTO tracked_symbols (symbol, is_active) 
             VALUES ($1, TRUE)
             ON CONFLICT (symbol) DO UPDATE SET is_active = TRUE"
        )
        .bind(&symbol_lower)
        .execute(&self.pool)
        .await?;

        info!("Added symbol to tracking: {}", symbol_lower);
        Ok(())
    }

    /// Remove (deactivate) a symbol from tracking
    pub async fn remove_symbol(&self, symbol: &str) -> Result<(), sqlx::Error> {
        let symbol_lower = symbol.to_lowercase();
        
        sqlx::query(
            "UPDATE tracked_symbols SET is_active = FALSE WHERE symbol = $1"
        )
        .bind(&symbol_lower)
        .execute(&self.pool)
        .await?;

        info!("Removed symbol from tracking: {}", symbol_lower);
        Ok(())
    }

    /// Check if a symbol is currently being tracked
    pub async fn is_symbol_tracked(&self, symbol: &str) -> Result<bool, sqlx::Error> {
        let symbol_lower = symbol.to_lowercase();
        
        let row: Option<(bool,)> = sqlx::query_as(
            "SELECT is_active FROM tracked_symbols WHERE symbol = $1"
        )
        .bind(&symbol_lower)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.0).unwrap_or(false))
    }

    /// Delete all kline data for a symbol (use with caution)
    pub async fn delete_symbol_data(&self, symbol: &str) -> Result<u64, sqlx::Error> {
        let symbol_upper = symbol.to_uppercase();
        
        let result = sqlx::query(
            "DELETE FROM klines_1m WHERE symbol = $1"
        )
        .bind(&symbol_upper)
        .execute(&self.pool)
        .await?;

        let deleted = result.rows_affected();
        info!("Deleted {} candles for symbol: {}", deleted, symbol_upper);
        Ok(deleted)
    }

    /// Get klines for a symbol with limit and optional end_time
    pub async fn get_klines(&self, symbol: &str, limit: i64, end_time: Option<i64>) -> Result<Vec<CandleData>, sqlx::Error> {
        let symbol_upper = symbol.to_uppercase();
        
        let rows: Vec<(String, i64, f64, f64, f64, f64, f64, f64, f64)> = if let Some(et) = end_time {
            sqlx::query_as(
                "SELECT symbol, timestamp, open, high, low, close, volume, taker_buy_volume, net_volume 
                 FROM klines_1m 
                 WHERE symbol = $1 AND timestamp <= $3
                 ORDER BY timestamp DESC 
                 LIMIT $2"
            )
            .bind(&symbol_upper)
            .bind(limit)
            .bind(et)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                "SELECT symbol, timestamp, open, high, low, close, volume, taker_buy_volume, net_volume 
                 FROM klines_1m 
                 WHERE symbol = $1 
                 ORDER BY timestamp DESC 
                 LIMIT $2"
            )
            .bind(&symbol_upper)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        let mut candles: Vec<CandleData> = rows.into_iter().map(|r| CandleData {
            symbol: r.0,
            timestamp: r.1,
            open: r.2,
            high: r.3,
            low: r.4,
            close: r.5,
            volume: r.6,
            taker_buy_volume: r.7,
            net_volume: r.8,
            is_closed: true,
        }).collect();

        // Reverse to chronological order (oldest first)
        candles.reverse();
        
        Ok(candles)
    }

    /// Get klines aggregated to a specific interval using TimescaleDB time_bucket
    pub async fn get_klines_aggregated(
        &self, 
        symbol: &str, 
        interval: crate::Interval, 
        limit: i64,
        end_time: Option<i64>
    ) -> Result<Vec<CandleData>, sqlx::Error> {
        use crate::Interval;
        
        // For 1m interval, just return raw data
        if interval == Interval::Min1 {
            return self.get_klines(symbol, limit, end_time).await;
        }

        let symbol_upper = symbol.to_uppercase();
        
        // Calculate interval in milliseconds for time range filtering
        let interval_ms: i64 = match interval {
            Interval::Min1 => 60_000,
            Interval::Min5 => 5 * 60_000,
            Interval::Min15 => 15 * 60_000,
            Interval::Hour1 => 60 * 60_000,
            Interval::Hour4 => 4 * 60 * 60_000,
            Interval::Day1 => 24 * 60 * 60_000,
            Interval::Week1 => 7 * 24 * 60 * 60_000,
            Interval::Month1 => 30 * 24 * 60 * 60_000,
        };
        
        // Convert interval to PostgreSQL interval string
        let interval_str = match interval {
            Interval::Min1 => "1 minute",
            Interval::Min5 => "5 minutes",
            Interval::Min15 => "15 minutes",
            Interval::Hour1 => "1 hour",
            Interval::Hour4 => "4 hours",
            Interval::Day1 => "1 day",
            Interval::Week1 => "1 week",
            Interval::Month1 => "1 month",
        };

        // Calculate time range to limit scan (add 10% buffer)
        let et = end_time.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
        let time_range_needed = interval_ms * limit * 11 / 10;
        let start_time = et - time_range_needed;

        // Use TimescaleDB time_bucket for efficient aggregation with time range filter
        let query = format!(
            r#"
            SELECT 
                $1 as symbol,
                (EXTRACT(EPOCH FROM time_bucket('{interval}', to_timestamp(timestamp/1000.0))) * 1000)::bigint as bucket_ts,
                (array_agg(open ORDER BY timestamp ASC))[1] as open,
                max(high) as high,
                min(low) as low,
                (array_agg(close ORDER BY timestamp DESC))[1] as close,
                sum(volume) as volume,
                sum(taker_buy_volume) as taker_buy_volume,
                sum(net_volume) as net_volume
            FROM klines_1m
            WHERE symbol = $1 AND timestamp >= $3 AND timestamp <= $4
            GROUP BY bucket_ts
            ORDER BY bucket_ts DESC
            LIMIT $2
            "#,
            interval = interval_str,
        );

        let rows: Vec<(String, i64, f64, f64, f64, f64, f64, f64, f64)> = sqlx::query_as(&query)
            .bind(&symbol_upper)
            .bind(limit)
            .bind(start_time)
            .bind(et)
            .fetch_all(&self.pool)
            .await?;

        let mut candles: Vec<CandleData> = rows.into_iter().map(|r| CandleData {
            symbol: r.0,
            timestamp: r.1,
            open: r.2,
            high: r.3,
            low: r.4,
            close: r.5,
            volume: r.6,
            taker_buy_volume: r.7,
            net_volume: r.8,
            is_closed: true,
        }).collect();

        // Reverse to chronological order (oldest first)
        candles.reverse();

        Ok(candles)
    }

    /// Find gaps in 1-minute kline data for a symbol.
    /// Returns a list of (start_timestamp, end_timestamp) pairs representing gaps.
    /// Each gap represents missing data from start_timestamp to end_timestamp (exclusive).
    /// Only considers data after DATA_CUTOFF_TIMESTAMP (2024-01-01).
    pub async fn find_gaps(&self, symbol: &str, cutoff_timestamp: i64) -> Result<Vec<(i64, i64)>, sqlx::Error> {
        let symbol_upper = symbol.to_uppercase();

        // Query to find gaps using window function
        // We look for cases where the next timestamp is more than 1 minute away
        // Only consider data after cutoff_timestamp
        let rows: Vec<(i64, i64)> = sqlx::query_as(
            r#"
            WITH ordered_klines AS (
                SELECT timestamp,
                       LEAD(timestamp) OVER (ORDER BY timestamp) as next_timestamp
                FROM klines_1m
                WHERE symbol = $1 AND timestamp >= $2
            )
            SELECT timestamp + 60000 as gap_start, next_timestamp as gap_end
            FROM ordered_klines
            WHERE next_timestamp - timestamp > 60000
            ORDER BY timestamp
            "#
        )
        .bind(&symbol_upper)
        .bind(cutoff_timestamp)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Get the earliest timestamp for a symbol
    pub async fn get_earliest_timestamp(&self, symbol: &str) -> Result<Option<i64>, sqlx::Error> {
        let row: Option<(Option<i64>,)> = sqlx::query_as(
            "SELECT MIN(timestamp) FROM klines_1m WHERE symbol = $1"
        )
        .bind(symbol.to_uppercase())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.and_then(|r| r.0))
    }

    /// Get count of klines for a symbol
    pub async fn get_kline_count(&self, symbol: &str) -> Result<i64, sqlx::Error> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM klines_1m WHERE symbol = $1"
        )
        .bind(symbol.to_uppercase())
        .fetch_one(&self.pool)
        .await?;

        Ok(row.0)
    }
}
