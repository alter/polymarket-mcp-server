---
title: "Polymarket API Tutorial: Python Authentication, Orders & WebSocket Streaming (2026)"
url: "https://agentbets.ai/guides/polymarket-api-guide/"
source: "agentbets.ai"
date: "2026-05-30"
type: "tutorial"
theme: "tools"
lang: "en"
---

# Polymarket API Tutorial Guide (2026, verified for CLOB V2)

## Architecture: Three Primary APIs + Bridge + Streaming

- **CLOB API** (`clob.polymarket.com`): Order book, prices, order management
- **Gamma API** (`gamma-api.polymarket.com`): Market discovery and metadata
- **Data API** (`data-api.polymarket.com`): User positions and trade history
- **Bridge API** (`bridge.polymarket.com`): Deposits and withdrawals
- **WebSocket channels**: Real-time market, user, and sports updates

## Authentication

Four signature types:
- EOA (type 0)
- POLY_PROXY (type 1)
- GNOSIS_SAFE (type 2)
- **POLY_1271** (type 3) — recommended for new API users

Authentication: EIP-712 signed messages + five L2 HMAC headers.

## Key Changes in CLOB V2 (April 28, 2026)

- New collateral: **pUSD** (replacing USDC.e) — ERC-20 backed 1:1 by USDC
- New order struct without embedded fees; protocol-set fees
- New `builderCode` attribution mechanism in order struct
- Updated rate limits: POST /order now 5,000/10s burst + 48,000/10min sustained
- V1 SDKs (py-clob-client, @polymarket/clob-client) no longer work against production

## Core Endpoints

Public (no auth):
- `GET /price` — Current token price
- `GET /book` — Full order book
- `GET /midpoint` — Bid-ask midpoint

Authenticated (trading):
- `POST /order` — Place limit orders
- `POST /orders` — Batch up to 15 orders
- `DELETE /order` — Cancel single order

## Official V2 SDK Packages

- Python: `py-clob-client-v2`
- TypeScript: `@polymarket/clob-client-v2` (uses viem)
- Rust: `polymarket_client_sdk_v2`

## WebSocket Channels

1. Market channel — orderbook snapshots, price changes, tick size changes, last trade prices, custom events. No auth.
2. User channel — order fills, cancellations, status. Requires API key/secret/passphrase.
3. Sports channel — live game scores and status. No auth.
4. RTDS — crypto prices from Binance and Chainlink. Optional gamma_auth.

Heartbeats: market/user channels need client PING every 10s; RTDS every 5s.

## Orderbook Reconstruction

Fetch REST snapshot for initial state, then apply incremental WebSocket updates. On disconnect, re-fetch REST snapshot.

## Rate Limits

- General: 15,000 req / 10s
- WebSocket connections do not count against REST limits

## Common Patterns

1. Price monitoring bots
2. Cross-market arbitrage (checking if YES + NO sum < $1.00)
3. WebSocket + LLM agent loops

## Important Notes

- Token IDs come from Gamma API (clobTokenIds field), not CLOB
- Neg-risk markets require `negRisk: true`
- EOA wallets need token allowances; proxy/deposit wallets don't
- No testnet available
- Builder Program enables gasless trading via Relayer Client
