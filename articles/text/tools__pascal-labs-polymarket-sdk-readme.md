---
title: "pascal-labs/polymarket-sdk - Python SDK README"
url: "https://raw.githubusercontent.com/pascal-labs/polymarket-sdk/main/README.md"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

# polymarket-sdk

Python SDK for Polymarket's CLOB (Central Limit Order Book) API. Handles authentication, order management, position tracking, market data, and real-time WebSocket price feeds — everything needed to go from "I have a signal" to "I have a position" programmatically.

## Why This SDK

Polymarket's official `py-clob-client` handles basic API calls but doesn't solve the operational problems of running a live trading system: connection drops during critical moments, HMAC authentication edge cases with connection pooling, batch order atomicity, or the need to correlate L2 orderbook state with fill execution. This SDK wraps those operational concerns into a single interface built from experience running automated strategies against the CLOB.

## Features

- **REST API Client** — Market data, order placement (single + batch up to 15), position queries
- **WebSocket Feed** — Real-time L2 orderbook with thread-safe price access
- **HTTP Connection Pooling** — Pre-warmed connections with HMAC-safe patching (150-300ms latency reduction)
- **Position Manager** — Track open positions, P&L, exposure limits
- **Market Discovery** — Auto-discover active markets by category (crypto, politics, sports)
- **Token Redemption** — Redeem winning positions (CTF + NegRisk contracts)

## Quick Start

```python
from polymarket_sdk import PolymarketClient

# Read-only (no wallet needed)
client = PolymarketClient(mode='paper')
prices = client.get_market_prices("market-slug")
orderbook = client.get_orderbook(token_id)
midpoint = client.get_midpoint(token_id)

# Trading (requires Polygon wallet)
client = PolymarketClient(
    private_key="0x...",
    mode='live'
)

# Place a limit order
client.place_order(
    token_id="0x...",
    side="BUY",
    size=10.0,       # USDC amount
    price=0.55,      # Price per share
    order_type="GTC"
)

# Batch orders (up to 15 per call)
client.place_orders_true_batch(orders, order_type='GTC')

# Position management
positions = client.get_positions_from_data_api()
balance = client.get_balances()
```

## WebSocket Feed

```python
from polymarket_sdk.websocket_feed import WebSocketPriceFeed

feed = WebSocketPriceFeed()
feed.start(up_token_id="0x...", down_token_id="0x...")

prices = feed.get_prices()  # (up_price, down_price)
spread = feed.get_spread()
depth = feed.get_orderbook_depth()
```

## API Architecture

```
    ┌─────────────────────────────────────────────────────┐
    │                    Your Strategy                     │
    └──────────┬──────────┬──────────┬────────────────────┘
               │          │          │
    ┌──────────▼────┐ ┌───▼────────┐ ┌▼───────────────────┐
    │  Gamma API    │ │  CLOB API  │ │  Data API          │
    │  No auth      │ │  HMAC-256  │ │  HMAC-256          │
    └───────────────┘ └──────────┘ └─────────────────────┘
                          │
               ┌──────────▼──────────┐
               │  WebSocket Feed     │
               │  No auth required   │
               └─────────────────────┘
```

## API Endpoints

| Endpoint | Purpose |
|----------|---------|
| `gamma-api.polymarket.com` | Market metadata, events, search |
| `clob.polymarket.com` | Orders, orderbook, midpoints |
| `data-api.polymarket.com` | Positions, balances, trade history |
| `ws-subscriptions-clob.polymarket.com` | WebSocket L2 feed |

## Configuration

```
POLYGON_PRIVATE_KEY=0x...
POLYGON_RPC=https://polygon-rpc.com
```

## Tech Stack

- `py-clob-client` — Polymarket's official CLOB client
- `eth-account` — Wallet signing (Polygon)
- `websockets` — Real-time orderbook feed
- `requests` + `urllib3` — Connection pooling

## Related Projects

- [market-maker-forensics](https://github.com/pascal-labs/market-maker-forensics) — Microstructure research
- [pulsefeed](https://github.com/pascal-labs/pulsefeed) — Multi-exchange price feeds
- [btc-short-term-alpha](https://github.com/pascal-labs/btc-short-term-alpha) — BTC binary strategy
- [tweet-volume-ensemble](https://github.com/pascal-labs/tweet-volume-ensemble) — 6-model ensemble fair value estimates

License: MIT
