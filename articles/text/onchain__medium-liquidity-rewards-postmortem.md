---
title: "My Two-Week Deep Dive into Polymarket Liquidity Rewards: A Technical Postmortem"
url: "https://medium.com/@wanguolin/my-two-week-deep-dive-into-polymarket-liquidity-rewards-a-technical-postmortem-88d3a954a058"
source: "Medium (wanguolin)"
date: "2025-01-01"
type: "case_study"
theme: "onchain"
lang: "en"
---

# Polymarket Liquidity Rewards: Technical Postmortem

## How Market Making Works on Polymarket

A market maker simultaneously posts both buy (bid) and sell (ask) orders, earning the spread between them on every round-trip trade:
- Post YES buy orders slightly below midpoint (~$0.48)
- Post YES sell orders slightly above midpoint (~$0.52)
- Earn $0.04 per share on each round-trip

Result: income independent of whether outcome resolves YES or NO.

## Platform Fee Timeline

- **2025**: No fees on deposits, withdrawals, or trades — subsidized liquidity through maker incentives
- **January 2026**: Taker fees introduced in high-frequency crypto markets
- **February 18, 2026**: Fees rolled out to select sports markets

## Liquidity Rewards Program

The platform allocated **$12 million in liquidity provider rewards in 2025**.

Reward formula characteristics:
- Rewards participation across markets
- Boosts two-sided depth (single-sided orders still score)
- **Quadratic spread function**: heavily penalizes quotes far from adjusted midpoint
- Forces liquidity into the most informative part of the book

## Profitability Data

- **Professional market makers**: $150–300/day per market with $100K+ daily volume + LP rewards
- **Early days**: ~$10,000 USDC capital → $200–300 USDC/day at peak
- **2025 stable setup**: ~10% annualized in calm, long-dated markets
- Platform allocated $12M in LP rewards in 2025

## Optimal Market Selection Criteria

| Criterion | Requirement |
|-----------|-------------|
| Daily volume | $50,000+ |
| Duration | 30+ days |
| Probability stability | Gradual movement, no wild swings |

## Key Risk: Adverse Selection / News Events

"News events can instantly move markets 40–50 points in prediction markets. If you are quoting 0.50/0.52 and suddenly the market should be at 0.90, you will get filled on your 0.52 offers before you can cancel — locking in massive losses."

## Core Lesson

Treat liquidity rewards as a **bonus, not the main profit engine**. Unless you have strong independent alpha:
- Competition has intensified (2025–2026)
- Meta evolution reduced standalone reward income
- Success requires tight quoting + robust news monitoring + smart market selection

## Sports Markets Context

Sports markets account for over **60% of Polymarket open interest**. Monthly volume:
- October 2025: $3.02 billion
- November 2025: $3.74 billion (combined with Kalshi: ~$10B)
