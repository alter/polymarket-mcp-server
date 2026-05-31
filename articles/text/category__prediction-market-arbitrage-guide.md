---
title: "How Prediction Market Arbitrage Works (Polymarket, Kalshi)"
url: "https://www.trevorlasn.com/blog/how-prediction-market-polymarket-kalshi-arbitrage-works"
source: "trevorlasn"
date: "2025"
type: "blog"
theme: "category"
lang: "en"
---

# Prediction Market Arbitrage: Key Mechanics

## Core Concept

Arbitrage in prediction markets like Polymarket and Kalshi exploits pricing inefficiencies. Since each event has YES and NO contracts that each pay $1 if correct, the combined price should equal $1.00. When it doesn't, traders can profit risk-free.

## The Basic Strategy

"Buy YES and NO for less than a dollar. One of them pays out a dollar. Keep the difference." For example, if YES trades at $0.42 and NO at $0.55 (totaling $0.97), purchasing both contracts guarantees a $0.03 profit regardless of outcome.

This scales linearly: 1,000 pairs bought for $970 return $1,000 in payout, yielding $30 profit.

## Why Pricing Gaps Occur

- **News events**: Price movements happen unevenly across YES/NO sides
- **Low liquidity**: Stale prices in thin markets
- **Cross-platform differences**: The same event prices differently on Polymarket versus Kalshi

## Real Constraints

Three major obstacles:

1. **Fees**: erode small spreads significantly
2. **Slippage**: occurs when order book depth runs out at target prices
3. **Speed**: opportunities last "seconds, not minutes"

Bots executing in milliseconds find these opportunities viable, while manual traders rarely capture them before prices adjust.

## Cross-Platform Sports/Crypto Arbitrage

For Bitcoin hourly markets: if Polymarket "BTC up" trades at $0.45 and Kalshi "BTC up" for the same hour trades at $0.58, you can buy "down" on Polymarket ($0.55) + "no" on Kalshi ($0.42) = $0.97 combined → $0.03 risk-free profit per pair.

Typical edges: 2–5% on sports arb; 3–8% on politics arb. Windows close in 15–60 seconds.
