---
title: "Kelly Criterion for Polymarket: Position Sizing Formula and Practical Application"
url: https://managebankroll.com/blog/polymarket-kelly-criterion-position-sizing
source: managebankroll.com
date: "2026"
type: blog
theme: strategies
lang: en
---

# Kelly Criterion for Polymarket Position Sizing

## The Core Formula

f* = (bp - q) / b

Where:
- f* = fraction of bankroll to wager
- b = net odds (what you win per dollar risked)
- p = probability of winning
- q = probability of losing (1 - p)

**In prediction markets:** If you buy a YES contract at price m, you risk m to win (1 - m).
Net odds: b = (1 - m) / m

A contract at 30 cents: b = 0.70/0.30 = 2.33 → the cheaper the contract, the higher Kelly tells you to bet IF you have edge.

## When Kelly = 0 (Don't Trade)

If a market is priced at $0.65 and you believe the true probability is 60%, Kelly yields a **negative number** → position size is **zero**. Don't trade this market.

Kelly explicitly tells you when you have no edge.

## Why NOT Full Kelly

Full Kelly maximizes long-run growth rate but creates **33% probability of halving your bankroll** before doubling.

**Kelly asymmetry:** Overbetting is MUCH more dangerous than underbetting. If optimal Kelly = 10% and you bet 20%, long-term growth rate is significantly damaged even at only 2x overbetting.

**Industry standard: Half Kelly or Quarter Kelly (0.25x–0.5x full Kelly)**
- Quarter-Kelly: less than 3% probability of halving bankroll

## Prediction Market Specific Formula

Simplified for Polymarket: f* = (P_true - P_market) / (1 - P_market)

**Example:** Market at $0.40 (40% implied), you believe true probability is 65%:
- Edge = 65% - 40% = 25%
- f* = (0.65 - 0.40) / (1 - 0.40) = 0.25/0.60 = 41.7% of bankroll
- At Half-Kelly: 20.8% of bankroll

## Managing Multiple Simultaneous Positions

Basic Kelly assumes a single sequence of independent bets, NOT a portfolio.

**Simple rule:** Never let total active exposure exceed **25–30% of bankroll** regardless of individual Kelly calculations.

**Correlation rule:** When a new trade is correlated with existing positions, **cut Kelly-recommended size in half**.

## Resolution Risk Adjustment

Polymarket uses UMA's optimistic oracle. Resolution language ambiguity (e.g., Khamenei "leaving office" — does death count?) can cause surprises.

When resolution criteria are ambiguous, **apply extreme conservatism to position sizing** — treat ambiguous markets as having additional variance.

## Real-World Validation

- "RN" on Polymarket: $6M+ profit trading sports markets — thousands of correctly sized bets, not concentrated bets
- "Distinct-baguette": grew $560 → $812,000 market-making crypto UP/DOWN contracts
- Neither achieved this through large concentrated bets but through mathematical discipline at scale

## Critical Insight

"91% of Polymarket traders lose not because they can't predict — but because they don't size."

Kelly tells you to bet LESS than you want to, always. The path to long-term wealth: individually modest bets, repeated consistently, with discipline most people don't have.

## Application to Kelly Calculator

For any Polymarket trade:
1. Estimate P_true (your probability estimate)
2. Note P_market (current market price)
3. Calculate f* = (P_true - P_market) / (1 - P_market)
4. Multiply by 0.25 (Quarter-Kelly) or 0.5 (Half-Kelly)
5. That fraction × bankroll = your position size in dollars
6. If result is negative, don't trade
