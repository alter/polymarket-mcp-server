---
title: "polymarket-arbitrage: Python Bot Watching 10,000+ Markets for Cross-Platform Inefficiencies"
url: https://github.com/ImMike/polymarket-arbitrage
source: github.com
date: "2025"
type: repo
theme: strategies
lang: en
---

# polymarket-arbitrage: Polymarket/Kalshi Arbitrage Bot

**Author:** ImMike | **Language:** Python

## Overview

Python-based trading bot detects and exploits price inefficiencies across Polymarket and Kalshi prediction markets. Monitors **thousands of markets simultaneously** identifying arbitrage opportunities where identical predictions are mispriced between platforms or within single-platform order books.

## Core Features

### Cross-Platform Arbitrage
Identifies when the same prediction trades at different prices across Polymarket and Kalshi, enabling profitable simultaneous buy/sell operations.

### Bundle Arbitrage Detection
Spots instances where YES and NO token prices don't sum to approximately $1.00, creating guaranteed profit opportunities.

### Market Making
Places competitive bid/ask orders within wide spreads to capture trading flow.

### Risk Management
- Position limits
- Loss thresholds
- Kill-switch mechanisms

## Operating Modes

**Simulation Mode:** Generates synthetic market data with artificial mispricings for testing

**Real Mode:** Connects to Polymarket's Gamma API and CLOB API, scanning 5,000+ live markets

Note: "Real prediction markets are highly efficient" with rare arbitrage opportunities.

## Technical Architecture

```
Data Sources → Market Matcher → Cross-Platform Arb Engine → Order Execution → Portfolio Tracking
```

Uses text similarity algorithms to automatically match equivalent predictions across platforms.

## Project Structure

- `polymarket_client/`: REST and WebSocket integration with Polymarket
- `kalshi_client/`: Kalshi API integration
- `core/`: Trading logic — data feeds, arbitrage engines, execution, portfolio tracking
- `dashboard/`: FastAPI-based real-time web interface
- `utils/`: Configuration, logging, backtesting

## Trading Parameters

- Minimum edge threshold: 1% (configurable)
- Minimum spread for market-making: 5 cents
- Position limits: $200/market, $5,000 global exposure
- Daily loss limit: $500

## Key Strategy Examples

**Cross-Platform:** When "Will Trump win?" YES costs $0.52 on Polymarket but $0.58 on Kalshi → buy cheaper, sell expensive, capture 6-cent difference minus fees.

**Bundle:** Purchase both YES and NO at $0.97 combined → $1.00 at resolution → 3% guaranteed profit.

## Important Note

Documentation emphasizes that arbitrage opportunities in real markets are infrequent. Start in dry_run mode before deploying capital. Requires Polymarket API credentials; operates on Polygon network.
