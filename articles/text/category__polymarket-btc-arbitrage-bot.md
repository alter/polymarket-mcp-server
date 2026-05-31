---
title: "Polymarket-Kalshi BTC Arbitrage Bot"
url: "https://github.com/CarlosIbCu/polymarket-kalshi-btc-arbitrage-bot"
source: "github"
date: "2025"
type: "code"
theme: "category"
lang: "en"
---

# Polymarket-Kalshi BTC Arbitrage Bot

## Overview
This project monitors Bitcoin 1-Hour Price markets across two prediction platforms to identify risk-free arbitrage opportunities in the Bitcoin 1-Hour Price market between Polymarket and Kalshi. The bot detects situations where combined opposing position costs total less than $1.00.

## Architecture

**Tech Stack:**
- Backend: Python, FastAPI, Uvicorn
- Frontend: Next.js, TypeScript, Tailwind CSS, shadcn/ui

**Components:**
- Python API server (port 8000)
- Next.js dashboard (port 3000)
- Docker containerization for simplified deployment

## How the Bot Works

The system operates through four core steps:

1. **Data Collection**: Fetches current prices from Polymarket's CLOB and Kalshi's API in real-time
2. **Market Matching**: Correlates equivalent Bitcoin hourly expiration events between platforms
3. **Price Normalization**: Converts prices to standardized probability format (0.00-1.00)
4. **Arbitrage Detection**: Compares strike prices and evaluates two strategies:
   - If Polymarket strike exceeds Kalshi's: evaluates "Down on Poly + Yes on Kalshi"
   - If Polymarket strike is lower: evaluates "Up on Poly + No on Kalshi"

## Key Features

- Real-Time Monitoring: Fetches live prices every second
- Automatic opportunity identification when combined costs fall below $1.00
- Live dashboard displaying price changes and optimal trades
- Visual cost breakdown bars for quick assessment

## Market Context

Polymarket introduced a dynamic taker-fee model for its 15-minute crypto markets, aimed at neutralizing latency-based arbitrage strategies. The taker fee is highest when odds are closest to 50% — precisely where latency-driven strategies were most active. At that level, fees can reach approximately 3.15% on a 50-cent contract, exceeding the typical arbitrage margin and making the strategy unprofitable at scale.

The collected fees are redistributed daily to liquidity providers, incentivizing deeper order books and tighter spreads.
