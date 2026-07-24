# ARBX P2P Monitor

A real-time P2P arbitrage monitoring tool that tracks USDT/USDC pricing across Bybit and Gate.io's peer-to-peer (NGN) markets, calculates cross-exchange spreads, and surfaces arbitrage opportunities as they appear.

## What it does

- Polls Bybit and Gate.io P2P order books every 60 seconds for NGN-denominated USDT/USDC ads
- Filters ads by liquidity range, payment method (bank transfer), completion rate, order history, and online status. So only usable, trustworthy quotes are considered
- Calculates the spread within each exchange (lowest sell vs. highest buy) and cross-exchange arbitrage opportunities (e.g. buy on Bybit, sell on Gate.io)
- Stores historical price snapshots in a local SQLite database for trend analysis
- Sends Telegram alerts when a spread crosses a configurable threshold
- Exposes a REST API for live prices, historical data, hourly analytics, and alert configuration

## Tech stack

- **Backend:** Node.js, Express
- **Database:** SQLite (via `better-sqlite3`)
- **Scheduling:** `node-cron` (runs the fetch/alert worker every minute)
- **HTTP client:** Axios
- **Deployment:** Railway (`railway.toml`)

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/live/:currency?` | Latest cached prices and opportunities (USDT/USDC) |
| POST | `/api/refresh/:currency?` | Force a fresh fetch, bypassing cache |
| GET | `/api/history` | Historical price snapshots (filterable by exchange, currency, timeframe) |
| GET | `/api/analytics/:currency?` | Hourly average price and volatility breakdown, best buy/sell hour insights |
| GET | `/api/alerts` | Current alert configuration |
| PUT | `/api/alerts` | Update alert threshold, Telegram webhook, or enable/disable |
| GET | `/api/health` | Health check |

## How it works

A background worker runs every 60 seconds, fetching live P2P ad data from both exchanges in parallel, computing the best available spread in each direction (Bybit → Gate.io and Gate.io → Bybit), and storing a snapshot. If any opportunity crosses the configured threshold, an alert fires via Telegram webhook.

## Setup

```bash
npm install
node server.js
```

Set `PORT` as an environment variable if not using the default (3000).

## Notes

This project was built as a mobile-first project (Termux/Acode) and deployed on Railway.
