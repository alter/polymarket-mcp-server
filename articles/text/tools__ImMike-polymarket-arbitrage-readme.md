---
title: "ImMike/polymarket-arbitrage - Cross-Platform Arbitrage Bot"
url: "https://github.com/ImMike/polymarket-arbitrage"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

# polymarket-arbitrage

Cross-platform arbitrage detection system for Polymarket and Kalshi. Python. Scans 5,000+ markets looking for pricing inefficiencies.

## Three Types of Opportunities

1. **Cross-platform arbitrage**: Same prediction priced differently on Polymarket vs Kalshi
2. **Bundle arbitrage**: YES + NO prices don't sum to ~$1.00 (profit guaranteed regardless of outcome)
3. **Market making spreads**: Bid-ask spread capture

## Technical Architecture

- Dedicated API clients for Polymarket (Gamma API) and Kalshi (REST)
- Real-time data feed manager
- Arbitrage detection engine
- Live web dashboard

## Configuration & Risk Management

- Dry-run mode for safe testing
- Position limits
- Daily loss thresholds
- Kill switch mechanism
- Minimum edge: default 1%

## Data Modes

- **Simulation mode**: Offline data for testing and demos (shows 99.6% win rates)
- **Real mode**: Live market data from Polymarket Gamma API and Kalshi REST

## AI Market Matcher

Automatically matches similar predictions across platforms using text similarity — needed because the same event may have different wording on Polymarket vs Kalshi.

## Important Limitation

"Real prediction markets are highly efficient — arbitrage opportunities are rare and fleeting. The bot is designed to catch them when they occur, but don't expect constant profits."

GitHub: https://github.com/ImMike/polymarket-arbitrage
