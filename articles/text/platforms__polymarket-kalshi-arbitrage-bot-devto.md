---
title: "How I Built a 'Risk-Free' Arbitrage Bot for Polymarket & Kalshi"
url: "https://dev.to/realfishsam/how-i-built-a-risk-free-arbitrage-bot-for-polymarket-kalshi-4f"
source: dev.to
date: "2025"
type: article
theme: platforms
lang: en
---

# How I Built a "Risk-Free" Arbitrage Bot for Polymarket & Kalshi

Author: Samuel Tinnerholm

## Core Concept
The bot exploits "synthetic arbitrage" by identifying price inversions between Polymarket and Kalshi. Example: buy YES on Kalshi at 35¢ and NO on Polymarket at 63¢ — total cost 98¢, guaranteed $1.00 payout, $0.02 profit regardless of outcome.

Captured spreads ranged from 1.5% to 4.5% on high-volume markets.

## Technical Solution
The two platforms use fundamentally different architectures:
- Polymarket: crypto-native CLOB on Polygon (Chain ID 137)
- Kalshi: US-regulated REST API exchange

Solution: `pmxt` — a unified wrapper library inspired by CCXT that normalizes market data across both platforms.

## Key Implementation Components

### Market Unification
Bot fetches markets from both platforms, treats them as standardized objects.

### Spread Detection
Logic identifies profitable inversions where combined prices fall below $1.00 across both markets.

### Rotation Strategy
Rather than holding positions until expiration (poor capital efficiency), the bot enters during spread widening and exits immediately when spreads close or better opportunities emerge.

## Practical Limitations
- Execution latency between API calls
- Variable liquidity on secondary platforms
- Multi-day fiat withdrawal delays present real operational friction despite theoretical "risk-free" calculations

Both the bot and underlying `pmxt` library were open-sourced.
