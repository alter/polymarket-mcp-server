---
title: "ImMike/polymarket-arbitrage — Polymarket & Kalshi Arbitrage Bot"
url: "https://github.com/ImMike/polymarket-arbitrage"
source: github
date: "2025-2026"
type: tool
theme: platforms
lang: en
---

# ImMike/polymarket-arbitrage — Multi-Strategy Arbitrage Bot

Monitors 10,000+ markets across Polymarket and Kalshi looking for inefficient markets on and between platforms.

## Architecture
```
Market APIs → Data Feed → Arbitrage Engine → Execution → Portfolio Tracking → Dashboard
```

### Components
- **Polymarket Client**: REST + WebSocket via Gamma API and CLOB orderbook
- **Kalshi Client**: REST API integration
- **Data Feed Manager**: Real-time orderbook aggregation
- **Cross-Platform Arbitrage Engine**: Detects price inefficiencies
- **Risk Manager**: Position limits, loss limits, kill switches
- **Execution Layer**: Order placement and management
- **FastAPI Dashboard**: Live web monitoring interface

## Three Arbitrage Detection Strategies

### 1. Cross-Platform Arbitrage
Identical predictions priced differently: Polymarket YES at $0.52 vs. Kalshi YES at $0.58 = 6% edge.
Uses text similarity matching (threshold: 0.6) to identify equivalent markets.
Executes simultaneous buy/sell across platforms.

### 2. Bundle Arbitrage
YES/NO pricing misalignment within a single platform:
- YES at $0.45 + NO at $0.52 = $0.97 → buy both → $0.03 guaranteed profit
- Detects: `ask_yes + ask_no < $1.00` (buy opportunity)
- Detects: `bid_yes + bid_no > $1.00` (sell opportunity)

### 3. Market Making
Captures spreads by placing competitive orders:
- Place bid above best bid when spread ≥ $0.05
- Place ask below best ask simultaneously

## Configuration
| Setting | Default | Purpose |
|---------|---------|---------|
| `trading_mode` | `dry_run` | Prevent real trades during testing |
| `min_edge` | 1% | Minimum profit threshold after fees |
| `max_position_per_market` | $200 | Per-market exposure limit |
| `cross_platform_enabled` | true | Enable Polymarket-Kalshi arb |

## Performance Notes
- Simulation: 99.6% win rate, $573 profit in testing
- Real markets: "highly efficient — arbitrage opportunities are rare and fleeting"
- Monitors 10,000+ markets simultaneously
