# 📊 Binance USDS Futures — Data Collection & API Backend

A high-performance Rust backend that collects, stores, and serves Binance USDS-M Futures K-line data in real-time. Designed as the data engine for custom charting frontends — supports both [KlineChart](https://klinecharts.com/) and [TradingView](https://www.tradingview.com/charting-library-docs/) out of the box.

## ✨ Features

- **Real-time WebSocket streaming** — Subscribe to 1-minute K-line updates via Binance combined streams, with auto-reconnect and backfill on disconnect
- **Multi-strategy historical sync** — Monthly ZIP → Daily ZIP → REST API fallback for fastest possible backfill
- **TimescaleDB-powered storage** — Hypertable-optimized with `time_bucket` aggregation for 8 timeframes (1m, 5m, 15m, 1h, 4h, 1D, 1W, 1M)
- **Dual API interface** — KlineChart REST API + TradingView UDF-compatible datafeed
- **Live WebSocket broadcast** — Push real-time candle updates to connected frontend clients
- **Canvas persistence** — Save/load chart drawings per symbol to local filesystem
- **Net Volume & Taker Buy Volume** — Custom indicators included in every response
- **Proxy pool support** — Up to 100 proxy clients (port 10000–10099) for high-throughput parallel downloads
- **Docker ready** — Multi-stage build with minimal runtime image

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        Axum HTTP Server (:3000)                  │
│                                                                  │
│   ┌─────────────────────┐       ┌──────────────────────────┐     │
│   │   KlineChart API    │       │   TradingView UDF API    │     │
│   │   /api/klines       │       │   /config /history /ws   │     │
│   │   /api/symbols      │       │   /symbols /search       │     │
│   │   /api/status       │       │   /canvas/*              │     │
│   └────────┬────────────┘       └────────────┬─────────────┘     │
│            │                                 │                   │
│            └──────────┬──────────────────────┘                   │
│                       ▼                                          │
│              ┌────────────────┐                                  │
│              │   Scheduler    │                                  │
│              │  (Command Bus) │                                  │
│              └───┬────────┬───┘                                  │
│                  │        │                                      │
│       ┌──────────▼──┐  ┌──▼──────────────────────┐               │
│       │  Binance    │  │  Historical Downloader  │               │
│       │  Collector  │  │  (ZIP + REST backfill)  │               │
│       │  (WebSocket)│  └─────────────────────────┘               │
│       └─────────────┘                                            │
│                                                                  │
│              ┌─────────────────────────┐                         │
│              │  DatabaseHandler        │                         │
│              │  (TimescaleDB + batch)  │                         │
│              └─────────────────────────┘                         │
└──────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
src/
├── main.rs                  # Axum server bootstrap & route composition
├── binance_collector.rs     # WebSocket real-time collection + REST sync
├── historical_downloader.rs # Binance data archive (ZIP) downloader
├── database.rs              # TimescaleDB operations, batch insert, aggregation
├── scheduler.rs             # Task coordination & collector lifecycle
├── klinechart.rs            # KlineChart REST API handlers
├── tradingview.rs           # TradingView UDF API + WebSocket + Canvas
├── structs.rs               # Data types (CandleData, Interval, WsMessage…)
├── error.rs                 # Custom error types (CollectorError, SchedulerError)
└── lib.rs                   # Public module exports

tests/
├── connection_test.rs       # Database connection tests
├── database_test.rs         # CRUD & query tests
├── scheduler_test.rs        # Scheduler command & lifecycle tests
├── sync_test.rs             # Single symbol sync tests
└── sync_full_history_test.rs # Full historical backfill tests

examples/
├── sync_all.rs              # Sync all symbols (standard)
├── sync_all_fast.rs         # Sync all symbols (parallel with proxy pool)
└── sql.txt                  # Reference SQL for TimescaleDB setup
```

## 🚀 Quick Start

### Prerequisites

- **Rust** 1.70+ (edition 2021)
- **PostgreSQL** with [TimescaleDB](https://docs.timescale.com/) extension
- (Recommended) Third-party rotating proxy with multi-port support for parallel downloads

### Environment Setup

```bash
cp .env.example .env
# Edit .env with your values
```

`.env.example`:
```env
RUST_LOG="INFO,binance_sdk::common::utils=off,binance_sdk::common::websocket=off"
DATABASE_URL="postgres://user:password@host:5432/crypto_database"
TRACKED_SYMBOL=[BTCUSDT,XRPUSDT,BNBUSDT,SOLUSDT,ETHUSDT]

# Proxy settings (optional — leave empty for direct connection)
PROXY_HOST=dc.your-proxy-provider.com
PROXY_USERNAME=your_username
PROXY_PASSWORD=your_password
PROXY_PROTOCOL=https
PROXY_PORT_START=10000
PROXY_PORT_END=10099
```

> [!WARNING]
> **Proxy is strongly recommended.** The backend downloads historical data from [Binance Data Archive](https://data.binance.vision/) for all tracked symbols. With a multi-port proxy pool (e.g. 100 concurrent connections), a full sync completes in minutes. **Without a proxy, syncing may take several days** due to single-connection rate limits. If `PROXY_HOST` is left empty, the backend falls back to a single direct connection.

### Run

```bash
# Development
cargo run

# Release build
cargo build --release
./target/release/backend

# Run tests
cargo test
```

### Docker

```bash
# Build image
docker build -t backend .

# Using docker-compose (connects to existing `cycle` network)
docker compose up -d
```

## 📡 API Reference

### KlineChart API

| Method | Endpoint | Description |
|--------|----------|-------------|
| **GET** | `/api/klines/{symbol}` | Query K-line data |
| **GET** | `/api/symbols` | List all tracked symbols |
| **POST** | `/api/symbols` | Add symbol to tracking (triggers backfill) |
| **DELETE** | `/api/symbols/{symbol}` | Remove symbol from tracking |
| **GET** | `/api/status` | Get scheduler status |

**Query Parameters** for `/api/klines/{symbol}`:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `limit` | `i64` | `800` | Number of candles to return |
| `interval` | `string` | `1m` | Timeframe: `1m`, `5m`, `15m`, `1h`, `4h`, `1d`, `1w`, `1M` |
| `end_time` | `i64` | *now* | Unix timestamp (ms) upper bound |

<details>
<summary>📄 Response Example</summary>

```json
{
  "success": true,
  "data": [
    {
      "symbol": "BTCUSDT",
      "timestamp": 1704067200000,
      "open": 42000.0,
      "high": 42100.0,
      "low": 41900.0,
      "close": 42050.0,
      "volume": 1000.5,
      "taker_buy_volume": 600.3,
      "net_volume": 200.1,
      "is_closed": true
    }
  ]
}
```

</details>

---

### TradingView UDF API

Fully compatible with the [TradingView UDF Datafeed API](https://www.tradingview.com/charting-library-docs/latest/connecting_data/UDF/).

| Method | Endpoint | Description |
|--------|----------|-------------|
| **GET** | `/config` | Datafeed configuration |
| **GET** | `/time` | Server time (seconds) |
| **GET** | `/symbols` | Resolve symbol info |
| **GET** | `/search` | Search symbols |
| **GET** | `/history` | Historical OHLCV data (includes `nv` and `tbv`) |
| **GET** | `/tracked-symbols` | List configured symbols |
| **GET** | `/daily-opens` | Daily open prices for all symbols |
| **WS** | `/ws` | Real-time K-line push via WebSocket |

<details>
<summary>📄 History Response Example</summary>

```json
{
  "s": "ok",
  "t": [1704067200, 1704153600],
  "o": [42000.0, 42050.0],
  "h": [42100.0, 42200.0],
  "l": [41900.0, 41950.0],
  "c": [42050.0, 42150.0],
  "v": [1000.5, 1200.3],
  "nv": [200.1, -150.5],
  "tbv": [600.3, 525.4]
}
```

</details>

<details>
<summary>📄 WebSocket Protocol</summary>

**Subscribe:**
```json
{ "type": "subscribe", "data": { "symbols": ["BTCUSDT", "ETHUSDT"] } }
```

**Kline Update (server → client):**
```json
{ "type": "kline", "data": { "symbol": "BTCUSDT", "timestamp": 1704067200000, "open": 42000.0, "high": 42100.0, "low": 41900.0, "close": 42050.0, "volume": 1000.5, "taker_buy_volume": 600.3, "net_volume": 200.1, "is_closed": false } }
```

**Keepalive:** `ping` / `pong`

</details>

---

### Canvas API (Drawing Persistence)

Save and load chart drawings per symbol to the local filesystem.

| Method | Endpoint | Description |
|--------|----------|-------------|
| **GET** | `/canvas/list` | List saved canvases for a symbol |
| **GET** | `/canvas/load` | Load canvas drawings |
| **POST** | `/canvas/save` | Save canvas drawings |
| **DELETE** | `/canvas/delete` | Delete a canvas |

## ⚙️ Core Components

### BinanceCollector
- Connects to Binance WebSocket combined streams for real-time 1m K-line data
- Supports up to **50 symbols per connection** (Binance limit); auto-splits into multiple connections
- **Auto-reconnect** with gap detection — backfills missed data on disconnect
- REST API sync with rate limiting (150ms interval, ~480 req/min)

### HistoricalDownloader
- **3-tier download strategy**: Monthly ZIP → Daily ZIP → REST API (fastest to slowest)
- Downloads from [Binance Data Archive](https://data.binance.vision/) for bulk historical data
- Concurrent downloads across proxy pool for maximum throughput
- CSV parsing from ZIP archives

### DatabaseHandler
- **TimescaleDB** hypertable for time-series optimization
- **Batch insert**: 100 candles or 5-second flush timeout
- `time_bucket` aggregation for multi-timeframe queries (1m → 1M)
- Gap detection and data integrity checks
- Data cutoff: only syncs data from **2024-01-01 UTC** onwards

### Scheduler
- Command-based control via `mpsc` channels:
  - `AddSymbol` — backfill + restart collector
  - `RemoveSymbol` — deactivate + restart collector
  - `RestartCollector` / `GetStatus` / `Shutdown`
- Manages full lifecycle: symbol tracking → historical backfill → real-time streaming

## 📦 Dependencies

| Crate | Purpose |
|-------|---------|
| `axum` | Web framework with WebSocket support |
| `sqlx` | Async PostgreSQL / TimescaleDB driver |
| `tokio` | Async runtime |
| `binance-sdk` | Official Binance connector (USDS futures + spot) |
| `tokio-tungstenite` | WebSocket client for Binance streams |
| `reqwest` | HTTP client for REST API & archive downloads |
| `tower-http` | CORS middleware |
| `serde` / `serde_json` | Serialization |
| `chrono` | Date/time handling |
| `csv` / `zip` | Historical data archive parsing |
| `thiserror` | Custom error types |
| `env_logger` | Logging |

## 📜 License

MIT


Generated By Claude Opus 4.6