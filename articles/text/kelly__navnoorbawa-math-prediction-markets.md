---
title: "The Math of Prediction Markets: Binary Options, Kelly Criterion, and CLOB Pricing Mechanics"
url: "https://navnoorbawa.substack.com/p/the-math-of-prediction-markets-binary"
source: "Substack"
date: "2024"
type: "blog"
theme: "kelly"
lang: "en"
---

# The Math of Prediction Markets: Binary Options, Kelly Criterion, and CLOB Pricing Mechanics

## Core Architecture

Prediction markets operate as fully-collateralized binary options on central limit order books. The fundamental invariant — "YES + NO = $1.00" — creates deterministic payoff mechanics. These markets issue Arrow-Debreu securities through frameworks like Gnosis Conditional Tokens, ensuring 100% collateralization with zero counterparty risk.

On platforms like Polymarket (Polygon-based with USDC settlement) and Kalshi (CFTC-regulated with fiat USD), "Market Price ≈ Implied Probability under risk-neutral pricing." A YES contract trading at $0.72 reflects 72% market-assigned probability.

## P&L and Payoff Structure

Trading P&L follows standard mechanics: (Exit Price - Entry Price) × Position Size. A trader buying 5,000 YES shares at $0.28 and selling at $0.61 nets $1,650 profit.

Low-probability contracts ($0.05-$0.20) exhibit lottery-like convexity. Purchasing 10,000 YES at $0.12 risks $1,200 but yields $10,000 on success — an 8,800-dollar gain or complete loss scenario.

## Kelly Criterion Application

With true probability p, market price, and net odds b, the formula determines optimal bankroll allocation:

**f* = (bp - q) / b**

The worked example: forecast of 75% probability versus market price of 60%. Yields 37.5% of bankroll allocation, providing $0.15 edge per dollar risked (25% expected return).

Fractional Kelly (0.25x to 0.5x) reduces volatility while protecting against probability estimation errors.

## Market Inefficiencies

Research documents persistent mispricing patterns:
- **Longshot bias**: Retail traders systematically overpay for tail events
- **Recency bias**: Prices overreact to recent news then revert
- **Platform fragmentation**: Cross-platform arbitrage opportunities persist due to low institutional participation and thin liquidity

Clinton and Huang (2025) found significant price disparities across Polymarket, Kalshi, and PredictIt, with arbitrage peaks in final weeks before resolution.

## CLOB vs AMM Architecture

Polymarket's 2023 migration from Automated Market Maker to Central Limit Order Book improved price discovery. CLOB architectures provide linear slippage based on orderbook depth while eliminating impermanent loss risks inherent to AMM constant-product formulas.

## Key Takeaways

Systematic profitability requires: superior probability forecasting, Kelly-sized positions, exploitation of behavioral patterns in tail markets, cross-platform arbitrage monitoring, and avoidance of thin markets where informed traders congregate.
