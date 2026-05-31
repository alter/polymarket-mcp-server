---
title: "Polymarket CLOB API Introduction - Official Docs"
url: "https://docs.polymarket.com/developers/CLOB/introduction"
source: "docs.polymarket.com"
date: "2026-05-30"
type: "documentation"
theme: "tools"
lang: "en"
---

# Polymarket CLOB Trading Overview

## System Architecture

Polymarket operates a hybrid-decentralized trading system combining off-chain order matching with on-chain settlement via the Exchange contract. Uses EIP-712 signed messages for orders, with atomic settlement on Polygon. The operator cannot manipulate prices or execute unauthorized trades.

## Available SDKs (V2, current)

- **TypeScript**: `@polymarket/clob-client-v2`
- **Python**: `py-clob-client-v2`
- **Rust**: `polymarket_client_sdk_v2`

Direct REST API usage is possible but requires manual EIP-712 order signing and HMAC authentication.

## Two-Level Authentication

1. **Level 1 (L1)**: EIP-712 signatures with private key → derive API credentials
2. **Level 2 (L2)**: HMAC-SHA256 signatures using derived credentials for trading requests

## Signature Types

- **EOA** (Type 0): Self-funded standalone wallets (MetaMask, hardware)
- **POLY_PROXY** (Type 1): Existing Polymarket proxy infrastructure
- **GNOSIS_SAFE** (Type 2): Safe wallet integration
- **POLY_1271** (Type 3): Recommended for new users via deposit wallets with ERC-1271 validation

## Server Infrastructure

Matching engine in eu-west-2 (primary region). Direct co-location available with KYC/KYB.

## CLOB V2 Key Changes (April 28, 2026)

- New collateral: **pUSD** (ERC-20 backed 1:1 by USDC, replacing USDC.e)
- Order struct simplified: removed `nonce`, `feeRateBps`, `taker`; added `timestamp`, `metadata`, `builder`
- EIP-712 exchange domain version bumped from "1" to "2"
- Fees collected on-chain at match time (not embedded in signed order)
- Nonce system removed; order uniqueness via timestamp (milliseconds)
- Builder rewards via native `builderCode` in order struct (no more HMAC headers)

## Fee Structure

- Maker: 0% (free)
- Taker fees at peak (p=0.5) by category:
  - Crypto: 1.75%
  - Economics/Culture: 1.25%
  - Finance/Politics: 1.00%
  - Sports: 0.75%
  - Geopolitics: fee-free

## Key Endpoints

Public (no auth):
- `GET /price` — Current token price
- `GET /book` — Full order book
- `GET /midpoint` — Bid-ask midpoint

Authenticated (trading):
- `POST /order` — Place limit orders
- `POST /orders` — Batch up to 15 orders
- `DELETE /order` — Cancel single order

## Rate Limits

- General: 15,000 req / 10s
- POST /order: 5,000/10s burst + 48,000/10min sustained
- WebSocket connections do not count against REST limits

## WebSocket Channels

- **Market channel** (`wss://ws-subscriptions-clob.polymarket.com/ws/market`) — no auth, orderbook/price updates
- **User channel** (same host `/ws/user`) — requires API key, order lifecycle events
- **Sports channel** (`wss://sports-api.polymarket.com/ws`) — live game scores, no auth
- **RTDS** (`wss://ws-live-data.polymarket.com`) — crypto prices from Binance and Chainlink

## No Testnet

All API calls hit production.

## Builder Program

- Pass `builderCode` (bytes32) in every order struct for attribution
- Register at `polymarket.com/settings?tab=builder`
- Weekly USDC rewards + gasless relayer access
- Over $2.5M distributed in grants ($100–$75,000 per project)
