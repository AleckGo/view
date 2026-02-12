# 📈 View Charting Frontend

A self-hosted [View Charting Library](https://www.tradingview.com/charting-library-docs/) frontend for Binance USDS-M Futures. Features real-time WebSocket streaming, multi-chart layouts, persistent drawing canvases, and a watchlist sidebar — all protected behind Google OAuth.

## ✨ Features

- **View Charting Library v29.4** — Professional-grade charting with full indicator and drawing tool support
- **Real-time Data** — Live candle updates via WebSocket from the backend API
- **Multi-chart Layouts** — Single, vertical split, horizontal split, and 1-left / 2-right layouts with resizable dividers
- **Watchlist Sidebar** — Drag-and-drop reordering, live price & change tracking, multiple custom lists
- **Canvas Persistence** — Save/load named drawing canvases per symbol, with auto-save every 5 minutes
- **Dark & Light Theme** — Full theme support synced with View widget
- **Google OAuth Protection** — Access restricted to authorized Google accounts with 144-hour session persistence
- **Responsive Login Page** — Adaptive background images for mobile / tablet / desktop
- **Docker Ready** — Nginx-based container, single-file deployment

## 📁 Project Structure

```
├── index.html          # Main application (View widget + watchlist + layouts)
├── login.html          # Standalone login page with Google Sign-In
├── auth.js             # AuthGuard class — session management & login UI
├── auth-config.js      # Google OAuth Client ID & API base URL configuration
├── .env.example        # Environment variable template
├── Dockerfile          # Nginx Alpine container
├── docker-compose.yml  # Docker Compose service definition
├── serve.py            # Python dev server with CORS support
├── package.json        # Charting Library package metadata (v29.4.0)
├── charting_library/   # View Charting Library assets
└── datafeeds/          # View UDF datafeed adapter
```

## 🚀 Quick Start

### Prerequisites

- A running [backend service](../backend/) providing the API
- (Optional) Google OAuth Client ID from [GCP Console](https://console.cloud.google.com/apis/credentials) for authentication

### Environment Setup

```bash
cp .env.example .env
# Edit .env with your backend API URL
```

`.env.example`:
```env
API_BASE_URL=https://api.yourdomain.com
```

### Configuration

Edit `auth-config.js` to set your API base URL and Google OAuth Client ID:

```js
window.API_CONFIG = { baseUrl: 'https://api.yourdomain.com' };

const AUTH_CONFIG = {
    clientId: 'YOUR_GOOGLE_CLIENT_ID.apps.googleusercontent.com',
    onSuccess: (user) => { /* ... */ },
    onError: (error) => { /* ... */ }
};
```

### Development

```bash
# Using Python dev server (with CORS headers)
python3 serve.py
# → http://localhost:8080

# Or any static file server
npx serve .
```

### Docker

```bash
# Build image
docker build -t frontend .

# Using docker-compose (connects to existing `cycle` network)
docker compose up -d
```

## 🔐 Authentication Flow

```
login.html                        index.html
    │                                  │
    ├─ Google Sign-In ──────┐          │
    │                       ▼          │
    │              JWT validation      │
    │              + email whitelist   │
    │                                  │
    │              localStorage ───────┤
    │              (auth_user,         │
    │               auth_token,        │
    │               auth_login_time)   │
    │                                  │
    │              redirect ───────────▶ Session check (144h max)           │
    │                                  │
    │                                  ├─ Valid → Load app                   │
    │                                  └─ Expired → Redirect to login.html
```

### Email Whitelist

Access is restricted at the client level via an email whitelist in `login.html`. Only emails in the `allowedEmails` array can log in — all others receive an "Access denied" error:

```js
const allowedEmails = ['your-email@gmail.com'];
```

> [!IMPORTANT]
> Update this whitelist with your authorized email addresses before deploying. This is a **client-side** check; for production use, combine with GCP Console's OAuth test user list for server-side enforcement.

## 🖥️ Multi-Chart Layouts

| Layout | Description |
|--------|-------------|
| **Single** | One full-screen chart |
| **Vertical** | Two charts stacked vertically |
| **Horizontal** | Two charts side by side |
| **1L + 2R** | One large chart on left, two stacked on right |

All dividers are draggable for custom sizing.

## 📦 Dependencies

| Dependency | Purpose |
|------------|---------|
| [View Charting Library](https://www.tradingview.com/charting-library-docs/) v29.4 | Charting engine |
| [Google Identity Services](https://developers.google.com/identity/gsi/web) | OAuth authentication |
| Nginx Alpine | Production static file serving |

## 📜 License

MIT


Generated By Claude Opus 4.6
