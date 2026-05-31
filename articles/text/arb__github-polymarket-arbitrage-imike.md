---
title: "polymarket-arbitrage: Polymarket and Kalshi Arbitrage Bot (Python)"
url: "https://github.com/ImMike/polymarket-arbitrage"
source: "github.com"
date: "2024-01-01"
type: "code"
theme: "arb"
lang: "en"
---

# polymarket-arbitrage: Polymarket and Kalshi Arbitrage Bot

**URL:** https://github.com/ImMike/polymarket-arbitrage

**Author:** ImMike

**Language:** Python

---

## Overview

A feature-rich, all-in-one Python bot that:
- Detects price differences between Polymarket and Kalshi for same predictions
- Identifies bundle arbitrage when YES + NO prices don't sum to ~$1.00
- Provides market making functionality
- Includes comprehensive risk management

---

## Key Modules

| Module | Function |
|---|---|
| `arb_engine.py` | Core arbitrage detection |
| `cross_platform_arb.py` | Cross-platform (Poly vs Kalshi) matching |
| Market API clients | Polymarket Gamma API + Kalshi REST API |
| Risk management | Position limits, loss limits, kill switch |
| Portfolio tracking | P&L accounting, position inventory |
| Web dashboard | FastAPI-based monitoring |

---

## AI-Powered Market Matching

Uses text similarity algorithms to automatically match similar predictions across platforms. Key challenge: same event described differently on each platform.

Example matching:
- Polymarket: "Will the Fed cut rates in December?"
- Kalshi: "Fed December rate cut: 25bps+"

The AI matcher handles linguistic variation to find genuine cross-platform arbitrage pairs.

---

## Arbitrage Types Supported

1. **Cross-platform arbitrage:** Same event priced differently on Polymarket vs Kalshi
2. **Bundle arbitrage:** YES + NO < $1.00 on single platform
3. **Market making:** Continuous quoting on both sides

---

## Risk Management Features

- Position limits per market
- Total loss limits (circuit breaker)
- Kill switch: auto-shutdown on predefined loss threshold
- Fee accounting: only signals opportunity when net spread > fees + gas

---

## Scale

Watches **10,000+ markets** simultaneously across both platforms.

---

## Important Disclaimer

"Real markets are highly efficient — arbitrage opportunities are rare!" The bot is more useful as a reference implementation than as a profitable out-of-the-box system in 2026.

---

## Related GitHub Repositories

- https://github.com/warproxxx/poly-maker — Automated market making bot with Google Sheets config
- https://github.com/elielieli909/polymarket-marketmaking — Market making reference
- https://github.com/AlexM800/poly-kalshi-arb — Scanner only (no execution)
- https://github.com/CarlosIbCu/polymarket-kalshi-btc-arbitrage-bot — BTC 1-hour price markets
- https://github.com/singhparshant/Polymarket — Rust market making + arbitrage experiments
- https://github.com/duzhi5368/FKPolyTools — Unified toolkit (TypeScript): arbitrage, copy trading, smart money analysis
- https://github.com/KJjjj0/polymarket-arbitrage — Frank-Wolfe algorithm arbitrage (Python)
- https://github.com/0xalberto/polymarket-arbitrage-bot — Single + multi-market arbitrage scanner
- https://github.com/taetaehoho/poly-kalshi-arb — Rust cross-platform arbitrage
- https://github.com/jiliangzhu/MarketPulse-X — Full-stack arbitrage monitoring system
