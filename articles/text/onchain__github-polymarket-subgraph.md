---
title: "Polymarket Subgraph and GraphQL Analytics: Official and Community Resources"
url: "https://github.com/Polymarket/polymarket-subgraph"
source: "GitHub / Polymarket / The Graph / PaulieB14"
date: "2026-04-28"
type: "technical"
theme: "onchain"
lang: "en"
---

# Polymarket Subgraph: On-Chain Data Infrastructure

## Official Subgraph

Polymarket's public subgraph manifest for indexing on-chain trade, volume, user, liquidity, and market data. Provides a GraphQL query interface for aggregate calculations and event indexing.

**GitHub:** https://github.com/Polymarket/polymarket-subgraph

**IMPORTANT: Migration Notice (April 28, 2026)**
Polymarket migrated to new CTF Exchange contracts on 2026-04-28 and stopped supporting the old subgraph indexer. The old pipeline (Goldsky subgraph + GraphQL polling) no longer returns complete data.
- Previous version preserved at `v1-final` tag for historical analysis
- For new work: use v2 version
- Recommended approach: read `OrderFilled` events from CTF Exchange V2 contract on Polygon via direct JSON-RPC, then join order events with market metadata to produce labeled trades with price, USD amount, and BUY/SELL direction

## Official Subgraph Endpoints (via Goldsky)

### 1. Orderbook Subgraph
Tracks order fills, market depth, spreads, trading flow across CTF Exchange contracts.
- Real-time orderbook depth
- Bid-ask spreads
- Liquidity analysis
- Last trade prices

### 2. Positions Subgraph
- User positions by wallet
- Average entry price
- Realized PnL tracking
- Balance history

### 3. Activity Subgraph
- Trade history
- Split/merge events
- Redemption tracking
- User activity feeds

**Documentation:** https://docs.polymarket.com/developers/subgraph/overview

---

## Community Projects

### PaulieB14/Polymarket-Orders
GitHub: https://github.com/PaulieB14/Polymarket-Orders
- Indexes Polymarket's orderbook data and trading activity
- Tracks OrderFilled events from CTF Exchange contract
- Account analytics, individual trader statistics, price tracking, fee analytics

### PaulieB14/Polymarkets-Profit-and-Loss
GitHub: https://github.com/PaulieB14/Polymarkets-Profit-and-Loss
- Most comprehensive Polymarket subgraph with Goldsky-style P&L calculations
- Win rates, profit factors, max drawdown tracking

### Polymarket PnL Substreams (v0.2.0)
URL: https://substreams.dev/packages/polymarket-pnl/v0.2.0
- Real-time P&L tracking for Polymarket prediction markets
- Monitors all core contracts
- Calculates user positions, profits, losses as they happen

### PaulieB14/polymarket-subgraph-analytics
GitHub: https://github.com/PaulieB14/polymarket-subgraph-analytics
- Complete guide to building Polymarket analytics using subgraphs
- Hot markets, trading insights, real-time data

### warproxxx/poly_data
GitHub: https://github.com/warproxxx/poly_data
- Fetches, processes, and structures Polymarket data including markets, order events, trades

---

## Dune Analytics Dashboards

Key Dune dashboards for Polymarket on-chain analysis:
- Polymarket Leaderboard: https://dune.com/genejp999/polymarket-leaderboard
- Polymarket Trader Cashflow PnL: https://dune.com/defioasis/polymarket-pnl
- Polymarket Whale Dominance: https://dune.com/queries/6457591/lineage
- Polymarket Whale Order Observation: https://dune.com/andy_chelsea/polymarket-whale-order-observation
- Polymarket Whale Tracker: https://dune.com/brunoskl/polymarket-whale-tracker
- Polymarket CLOB Stats: https://dune.com/lifewillbeokay/polymarket-clob-stats
- Polymarket Overview: https://dune.com/datadashboards/polymarket-overview
- Polymarket Activity & Volume: https://dune.com/filarm/polymarket-activity
- Polymarket Analysis: https://dune.com/lujanodera/polymarket-analysis
- Original rchen8 dashboard: https://dune.com/rchen8/polymarket

---

## Official SDKs

- **polymarket-us-python**: Official Python SDK — https://github.com/Polymarket/polymarket-us-python
- **Polymarket/agents**: AI agent framework for autonomous trading — https://github.com/Polymarket/agents
- **Polymarket/examples**: Code examples — https://github.com/Polymarket/examples

---

## GraphQL Tutorial Resource

Polymarket GraphQL Tutorial 2025: 5 Subgraphs, The Graph, Goldsky API:
https://www.polytrackhq.app/blog/polymarket-graphql-subgraph-guide

The Graph official guide:
https://thegraph.com/docs/en/subgraphs/guides/polymarket/
