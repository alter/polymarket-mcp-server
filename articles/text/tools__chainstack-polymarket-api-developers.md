---
title: "Polymarket API for Developers: Gamma API, Data, and Polygon RPC"
url: "https://chainstack.com/polymarket-api-for-developers/"
source: "chainstack.com"
date: "2026-05-30"
type: "tutorial"
theme: "tools"
lang: "en"
---

# Polymarket API for Developers (Chainstack)

## Platform Stats

Over 2.4 million traders across 214,735 markets with approximately $62 billion in total trading volume.

## API Architecture Layers

### Gamma API (Market Discovery)

- Public REST, no auth required
- `GET /markets` — fetch and filter active markets
- `GET /events` — fetch prediction market events
- `GET /tags` — tags/categories
- `GET /search` — search markets

Filtering: `limit`, `offset`, `order`, `active`/`closed`, tag-based filtering.

Token IDs come from Gamma API — specifically the `clobTokenIds` field (`clobTokenIds[0]` = YES, `clobTokenIds[1]` = NO).

Rate limit: ~60 requests/minute for unauthenticated access.

### CLOB Trading Engine

Hybrid: off-chain operator matching + on-chain settlement via EIP-712 signed messages.

Authentication:
- **L1**: `createOrDeriveApiKey()` via wallet signature
- **L2**: HMAC-signed headers with derived credentials

Order Types: GTC, GTD, FOK, FAK.

### Data API

User-level analytics at `https://data-api.polymarket.com/` — positions, trade history, PnL.

### WebSocket Channels

Real-time: market data updates, user order notifications, sports feeds, RTDS institutional data.

## Polygon PoS Foundation

- Chain ID 137
- ~110 TPS, ~$0.002 average tx cost
- 2-second block times (soft finality)
- ~30 minutes full L1 finality via Ethereum checkpoint

## Smart Contract Infrastructure

- Conditional Token Framework (CTF, ERC-1155) — YES/NO shares backed by USDC.e
- UMA's Optimistic Oracle for resolution (most markets)
- Chainlink Data Streams for short-duration crypto price markets

## Step-by-Step Integration

1. Set up Polygon wallet with POL gas token
2. Deposit USDC to profile address
3. Install dependencies (clob-client, ethers.js/viem)
4. Derive API credentials via `createOrDeriveApiKey()`
5. Configure USDC and CTF allowances via `setAllowances()`
6. Query markets through Gamma API
7. Retrieve order book and inspect liquidity
8. Place limit orders and track status
9. Subscribe to real-time WebSocket updates
10. Monitor on-chain settlement via Polygon RPC

## Developer Tools

- clob-client (TypeScript)
- real-time-data-client (TypeScript) — WebSocket handling
- rs-clob-client (Rust) — alloy signer support
- clob-order-utils — manual EIP-712 construction
- Polymarket Subgraphs (GraphQL) via Goldsky

## Public vs. Private RPC for Bots

Public endpoints: rate-limited, unreliable under load (problem when bot monitors OrderFilled events + WebSocket + submits transactions simultaneously).

Production recommendation: managed provider with dedicated throughput, WebSocket support, archive access.

Node options:
- **Global Node**: Good for development/testing
- **Dedicated Node**: Essential for 24/7 production
