---
title: "polymarket-apis - Unified Polymarket Python Library (PyPI)"
url: "https://pypi.org/project/polymarket-apis/"
source: "pypi.org"
date: "2026-05-30"
type: "documentation"
theme: "tools"
lang: "en"
---

# polymarket-apis

Comprehensive Python library providing unified access to Polymarket v2 APIs with Pydantic data validation. Requires Python >=3.12.

Author: Razvan Gheorghe | GitHub: https://github.com/qualiaenjoyer/polymarket-apis

## Core Clients

### Market Data & Trading

- **PolymarketReadOnlyClobClient**: Order book operations — snapshots, spreads, midpoints, price history
- **PolymarketClobClient**: Full trading — order creation/cancellation, trade history, liquidity rewards management
- **PolymarketGammaClient**: Event and market information retrieval, advanced filtering and search

### Portfolio & Analytics

- **PolymarketDataClient**: Position tracking, trade analytics, activity logs, leaderboard rankings. PnL timeseries analysis across multiple timeframes.

### Blockchain Integration

- **PolymarketWeb3Client** and **PolymarketGaslessWeb3Client**: On-chain operations — token transfers, splits/merges, negative-risk conversions. Supports EOA, Email/Magic, Safe/Gnosis, and Deposit wallets.

### Real-Time Data

- **AsyncPolymarketWebsocketsClient**: Production-grade streaming with automatic reconnection, market snapshots, user events, sports updates. Recommended for production, bots, and multi-data-stream scenarios.
- **PolymarketWebsocketsClient**: Blocking wrapper for synchronous use.
- **PolymarketGraphQLClient**: Subgraph queries — activity, positions, PnL, orderbook, and more.

## Key Concepts

Platform hierarchy:
- **Events**: Propositions/questions (e.g., "Fed rate cuts in 2025")
- **Markets**: Specific options within an event
- **Outcomes**: Binary choices as tradable tokens

Unified order book: complementary outcomes create mathematical relationships — holding one "Yes" and one "No" token guarantees $1.00 payout regardless of resolution.

## Installation

```bash
pip install polymarket-apis
```
