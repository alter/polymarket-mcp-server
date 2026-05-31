---
title: "NautilusTrader Polymarket Integration Documentation"
url: "https://nautilustrader.io/docs/latest/integrations/polymarket/"
source: "nautilustrader.io"
date: "2026-05-30"
type: "documentation"
theme: "tools"
lang: "en"
---

# NautilusTrader × Polymarket Integration

## Overview

NautilusTrader provides a venue integration for data and execution via Polymarket's CLOB API.

## Key Components

- **PolymarketWebSocketClient**: Low-level WebSocket connectivity (Rust-based)
- **PolymarketInstrumentProvider**: Binary option parsing and loading
- **PolymarketDataClient**: Market data management
- **PolymarketExecutionClient**: Trade execution gateway

Two implementations:
- Python adapter: uses official py-clob-client-v2
- Rust-native adapter: consolidation target, preferred for new implementations

## Installation

```bash
pip install --upgrade "nautilus_trader[polymarket]"
```

## Prerequisites

- Polygon-compatible wallet (MetaMask etc.)
- pUSD collateral funding
- Environment variables: `POLYMARKET_PK`, `POLYMARKET_FUNDER`, `POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`, `POLYMARKET_PASSPHRASE`

## Initial Setup

Set smart contract allowances once:

```bash
python nautilus_trader/adapters/polymarket/scripts/set_allowances.py
```

Requires POL tokens for gas fees.

## Supported Order Types

- **Market orders**: Quote quantities for BUY, base quantities for SELL
- **Limit orders**: Base-unit quantities, GTC/GTD support
- **Post-only**: Supported for GTC/GTD limit orders
- Batch submit: up to 15 independent limit orders per request

## Time-in-Force Mappings

- GTC/GTD: Limit orders only
- IOC → Polymarket FAK (Fill-And-Kill)
- FOK: Full fill or cancel

## Fee Structure

Formula: `fee = C × feeRate × p × (1 - p)` where p is share price, C is shares traded.

- Makers: zero fee + 20-25% rebates
- Takers: 0.03-0.072 by category

## WebSocket Limitations

Maximum **500 instruments per WebSocket connection** (undocumented). NautilusTrader defaults to 200 subscriptions per connection, auto-creates additional connections when exceeded.

## Rate Limits (as of 2026-05-06)

- General: 15,000 req / 10s
- POST /order: 3,500 burst, 36,000 sustained
- POST /orders (batch): 1,000 burst, 15,000 sustained
- Data loader: 100 req/minute default throttle

## Historical Data Loading

```python
loader = await PolymarketDataLoader.from_market_slug("market-slug")
trades = await loader.load_trades()
```

Fetches from Gamma API (market metadata), CLOB API (instrument details), Data API (historical trades, positions).

## Trade Statuses

- MATCHED: Matched and sent to executor service
- MINED: Observed mined into chain
- CONFIRMED: Strong probabilistic finality
- RETRYING: Transaction failed, being resubmitted
- FAILED: Failed, not being retried

## Reconciliation

Compares Polymarket active orders and user positions against local execution state. For inactive orders, recovers state from trade history when available.
