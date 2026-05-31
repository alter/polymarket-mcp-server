---
title: "Polymarket WebSocket Guide: Channels, Subscriptions & Real-Time Orderbook (2026)"
url: "https://agentbets.ai/guides/polymarket-websocket-guide/"
source: "agentbets.ai"
date: "2026-05-30"
type: "tutorial"
theme: "tools"
lang: "en"
---

# Polymarket WebSocket Guide (verified for CTF Exchange V2, April 22, 2026)

## Four WebSocket Channels

### 1. Market Channel

URL: `wss://ws-subscriptions-clob.polymarket.com/ws/market`

- No authentication required
- Provides orderbook updates, price changes, trade data
- Heartbeat: Client sends PING every 10 seconds
- Subscription includes asset IDs and optional `custom_feature_enabled` flag

Message types: `book` (incremental), `price_change`, `tick_size_change`, `last_trade_price`, `best_bid_ask`, `new_market`, `market_resolved`

### 2. User Channel

URL: `wss://ws-subscriptions-clob.polymarket.com/ws/user`

- Requires API key, secret, and passphrase authentication
- Delivers: order fills, cancellations, status
- Heartbeat: Client sends PING every 10 seconds
- Subscribes by **condition ID** (not asset ID)

Message types: `trade` (appears twice per fill: MATCHED then CONFIRMED) and `order` status updates.

### 3. Sports Channel

URL: `wss://sports-api.polymarket.com/ws`

- No authentication needed
- Streams live game scores and status for all active sports events
- Server initiates ping; client responds with pong within 10 seconds
- No explicit subscription required

### 4. RTDS

URL: `wss://ws-live-data.polymarket.com`

- Optional gamma_auth authentication
- Streams crypto prices from Binance and Chainlink plus comments
- Heartbeat: Client sends PING every 5 seconds
- Topic-based subscriptions with symbol filters

## Key Technical Patterns

### Orderbook Reconstruction

Fetch REST snapshot for initial state, then apply incremental WebSocket updates. On disconnect, re-fetch REST snapshot (no sequence numbers available).

### Dynamic Subscriptions

Both market and user channels support runtime modifications using `"operation": "subscribe"` or `"operation": "unsubscribe"` without reconnecting.

### Production Resilience

Exponential backoff reconnection with maximum 60-second delay. Reset counter on successful connection.

## Asset IDs vs. Condition IDs

- Market channel uses **asset IDs** (individual outcomes)
- User channel requires **condition IDs** (entire market identifier)

## Rate Limits

WebSocket connections do not count against REST API rate limits. Using WebSockets instead of polling `GET /price` or `GET /book` is the single best way to reduce REST request count.

## Code Examples Available

The guide includes working Python and TypeScript examples for:
- Basic market streaming with heartbeat loops
- User channel authentication and subscription
- Orderbook class with snapshot loading and incremental updates
- Resilient connection with automatic reconnection
- RTDS crypto price streaming

## Security

Always store API keys in environment variables. Never hardcode private keys or API secrets.
