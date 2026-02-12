use crate::database::DatabaseHandler;
use crate::binance_collector::BinanceCollector;
use crate::historical_downloader::HistoricalDownloader;
use crate::error::SchedulerError;
use crate::structs::*;
use crate::DATA_CUTOFF_TIMESTAMP;

use log::{info, error, warn};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

const ONE_MINUTE_MS: i64 = 60_000;

pub struct Scheduler {
    db: Arc<DatabaseHandler>,
    command_rx: mpsc::Receiver<SchedulerCommand>,
    collector_handle: Option<JoinHandle<()>>,
    is_running: Arc<RwLock<bool>>,
    active_symbols: Arc<RwLock<Vec<String>>>,
    ws_broadcast_tx: Option<mpsc::Sender<CandleData>>,
}

impl Scheduler {
    pub fn new(
        db: Arc<DatabaseHandler>,
        command_rx: mpsc::Receiver<SchedulerCommand>,
        ws_broadcast_tx: Option<mpsc::Sender<CandleData>>,
    ) -> Self {
        Self {
            db,
            command_rx,
            collector_handle: None,
            is_running: Arc::new(RwLock::new(false)),
            active_symbols: Arc::new(RwLock::new(Vec::new())),
            ws_broadcast_tx,
        }
    }

    pub async fn run(&mut self) {
        info!("Scheduler started");

        // Initial startup
        if let Err(e) = self.start_collector().await {
            error!("Failed to start collector on init: {}", e);
        }

        // Command processing loop
        while let Some(cmd) = self.command_rx.recv().await {
            match cmd {
                SchedulerCommand::AddSymbol { symbol, backfill_from: _ } => {
                    info!("AddSymbol command ignored - symbols managed by get_symbol()");
                    let _ = self.db.add_symbol(&symbol).await;
                }
                SchedulerCommand::RemoveSymbol { symbol } => {
                    info!("RemoveSymbol: {}", symbol);
                    let _ = self.db.remove_symbol(&symbol).await;
                }
                SchedulerCommand::RestartCollector => {
                    self.handle_restart_collector().await;
                }
                SchedulerCommand::GetStatus { reply } => {
                    self.handle_get_status(reply).await;
                }
                SchedulerCommand::Shutdown => {
                    info!("Shutdown command received");
                    self.stop_collector().await;
                    break;
                }
            }
        }

        info!("Scheduler stopped");
    }

    async fn handle_restart_collector(&mut self) {
        info!("Restarting collector...");
        self.stop_collector().await;
        if let Err(e) = self.start_collector().await {
            error!("Failed to restart collector: {}", e);
        }
    }

    async fn handle_get_status(&self, reply: tokio::sync::oneshot::Sender<SchedulerStatus>) {
        let is_running = *self.is_running.read().await;
        let active_symbols = self.active_symbols.read().await.clone();
        let collector_connected = self.collector_handle.is_some() && is_running;

        let _ = reply.send(SchedulerStatus {
            is_running,
            active_symbols,
            collector_connected,
        });
    }

    async fn start_collector(&mut self) -> Result<(), SchedulerError> {
        // STEP 1: Get symbols from TRACKED_SYMBOLS env variable
        let symbol_names = DatabaseHandler::get_symbols_from_env();
        
        if symbol_names.is_empty() {
            warn!("No symbols in TRACKED_SYMBOLS env variable. Set TRACKED_SYMBOLS=BTCUSDT,ETHUSDT,...");
            return Ok(());
        }
        info!("Tracking {} symbols from env: {:?}", symbol_names.len(), symbol_names);

        // Update active symbols
        {
            let mut active = self.active_symbols.write().await;
            *active = symbol_names.clone();
        }

        // STEP 2: Start WebSocket FIRST to capture real-time data immediately
        info!("Starting WebSocket collector FIRST (priority: real-time data)...");
        let collector = BinanceCollector::new(symbol_names.clone());

        let (candle_tx, mut candle_rx) = mpsc::channel::<CandleData>(10000);

        // Consumer that both saves to DB and broadcasts to WebSocket clients
        let db = self.db.clone();
        let ws_tx = self.ws_broadcast_tx.clone();
        tokio::spawn(async move {
            while let Some(candle) = candle_rx.recv().await {
                // Broadcast to WebSocket clients immediately (all updates for real-time charts)
                if let Some(ref tx) = ws_tx {
                    let _ = tx.send(candle.clone()).await;
                }

                // Only save closed candles to database
                if candle.is_closed {
                    if let Err(e) = db.insert_candle(&candle).await {
                        error!("Failed to insert realtime candle: {}", e);
                    }
                }
            }
        });

        let is_running = self.is_running.clone();
        let handle = tokio::spawn(async move {
            {
                let mut running = is_running.write().await;
                *running = true;
            }

            if let Err(e) = collector.start_stream(candle_tx).await {
                error!("Collector error: {}", e);
            }

            {
                let mut running = is_running.write().await;
                *running = false;
            }
        });

        self.collector_handle = Some(handle);
        info!("WebSocket collector started! Real-time data is now being captured.");

        // STEP 3: Background sync - runs in parallel with WebSocket
        // This fills in historical data without blocking real-time updates
        let db_for_sync = self.db.clone();
        let symbols_for_sync = symbol_names.clone();

        tokio::spawn(async move {
            info!("Starting background historical sync...");

            // Build API clients for parallel sync
            let api_clients = BinanceCollector::build_clients().await;
            if api_clients.is_empty() {
                error!("No working API clients available for background sync");
                return;
            }
            info!("Built {} API clients for background sync", api_clients.len());

            // Build HTTP clients for ZIP downloads
            let http_clients = HistoricalDownloader::build_clients().await;
            info!("Built {} HTTP clients for ZIP downloads", http_clients.len());

            // Create channel for background sync (lower priority, uses batching)
            let (sync_tx, sync_rx) = mpsc::channel::<CandleData>(100000);

            let db_consumer = db_for_sync.clone();
            let consumer_handle = tokio::spawn(async move {
                db_consumer.start_consumer(sync_rx).await;
            });

            // Sync symbols that need updates
            let mut synced_count = 0;
            let total_symbols = symbols_for_sync.len();

            for symbol in &symbols_for_sync {
                let latest_ts = db_for_sync.get_latest_timestamp(symbol).await.ok().flatten();

                let start_time = match latest_ts {
                    Some(ts) => ts + ONE_MINUTE_MS,
                    None => DATA_CUTOFF_TIMESTAMP,
                };

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                // Skip if less than 2 minutes behind (WebSocket will catch up)
                if now - start_time < ONE_MINUTE_MS * 2 {
                    synced_count += 1;
                    continue;
                }

                let behind_mins = (now - start_time) / ONE_MINUTE_MS;
                info!("[{}/{}] Syncing {} ({} minutes behind)...",
                    synced_count + 1, total_symbols, symbol, behind_mins);

                // Use comprehensive sync: Monthly ZIP → Daily ZIP → API
                match BinanceCollector::sync_comprehensive(
                    symbol.clone(),
                    start_time,
                    api_clients.clone(),
                    &http_clients,
                    sync_tx.clone(),
                ).await {
                    Ok(count) => {
                        synced_count += 1;
                        if count > 0 {
                            info!("[{}/{}] {} synced: {} candles", synced_count, total_symbols, symbol, count);
                        }
                    }
                    Err(e) => {
                        synced_count += 1;
                        error!("{} sync failed: {}", symbol, e);
                    }
                }
            }

            // Close sync channel
            drop(sync_tx);
            let _ = consumer_handle.await;
            info!("Background historical sync complete! Synced {} symbols", synced_count);

            // STEP 4: Gap detection and repair (also in background)
            info!("Starting background gap detection and repair...");
            let (gap_tx, gap_rx) = mpsc::channel::<CandleData>(100000);

            let db_gap = db_for_sync.clone();
            let gap_consumer_handle = tokio::spawn(async move {
                db_gap.start_consumer(gap_rx).await;
            });

            // Rebuild clients for gap repair
            let clients = BinanceCollector::build_clients().await;
            let mut total_gaps_repaired = 0u64;
            let mut symbols_with_gaps = 0;

            for symbol in &symbols_for_sync {
                match db_for_sync.find_gaps(symbol, DATA_CUTOFF_TIMESTAMP).await {
                    Ok(gaps) => {
                        if gaps.is_empty() {
                            continue;
                        }

                        symbols_with_gaps += 1;
                        info!("{} - Found {} gaps to repair", symbol, gaps.len());

                        for (gap_start, gap_end) in gaps {
                            let gap_duration_mins = (gap_end - gap_start) / ONE_MINUTE_MS;

                            // Skip very small gaps (less than 2 minutes)
                            if gap_duration_mins < 2 {
                                continue;
                            }

                            info!(
                                "{} - Repairing gap: {} minutes",
                                symbol, gap_duration_mins
                            );

                            match BinanceCollector::sync_from_scratch(
                                symbol.clone(),
                                gap_start,
                                clients.clone(),
                                gap_tx.clone(),
                            ).await {
                                Ok(count) => {
                                    total_gaps_repaired += count;
                                }
                                Err(e) => {
                                    error!("{} - Gap repair failed: {}", symbol, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("{} - Failed to check gaps: {}", symbol, e);
                    }
                }
            }

            // Close gap repair channel
            drop(gap_tx);
            let _ = gap_consumer_handle.await;
            info!("Gap detection complete! {} symbols had gaps, repaired {} candles total",
                symbols_with_gaps, total_gaps_repaired);
        });

        Ok(())
    }

    async fn stop_collector(&mut self) {
        if let Some(handle) = self.collector_handle.take() {
            info!("Stopping collector...");
            handle.abort();
            let _ = handle.await;

            let mut running = self.is_running.write().await;
            *running = false;
        }
    }
}

pub fn create_command_channel() -> (mpsc::Sender<SchedulerCommand>, mpsc::Receiver<SchedulerCommand>) {
    mpsc::channel(100)
}
