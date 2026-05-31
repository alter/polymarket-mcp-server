---
title: "84% of Polymarket Traders Are Losing Money — On-Chain Profitability Analysis"
url: https://sergeenkov.com/polymarket-profitability/
source: sergeenkov.com
date: "2026-04"
type: blog
theme: strategies
lang: en
---

# Polymarket Trader Profitability Analysis (April 2026)

**Analyst:** Andrey Sergeenkov
**Dataset:** 2.5 million wallet addresses
**Data source:** On-chain transactions on Polygon via Dune Analytics
**Coverage through:** April 1, 2026

## Key Statistics

### Overall Profitability
- **84.1% of traders are losing money**
- Only **15.9% are profitable**

### By Profit Threshold
- >$1,000 total: only 2% of all traders
- >$10,000 total: 0.32% (~8,000 addresses)
- >$100,000 total: 0.033% (~840 addresses)

### Monthly Consistency
- Average monthly profit >$1,000: 1.25% of traders
- Average monthly profit >$5,000: 0.26% (~6,600 addresses)
- Average monthly profit >$10,000: 0.13% (~3,250 addresses)

## Retention Problem

Most profitable traders are transient:
- **53% of high-earners** were active for only **one month**
- **73%** active for **maximum two months**
- Among 6,600 traders averaging $5,000+/month, only **2.6% (172 addresses)** stayed active >1 year

## Probability of Consistent Income

Achieving $5,000/month (average US salary):
- Any single month: 0.98% of traders
- Two consecutive months: 0.1%
- Four consecutive months: 0.015%

## Methodology

PnL calculated from five on-chain event types:
1. Token purchases/sales (OrderFilled)
2. Winning token redemptions (PayoutRedemption)
3. Splitting USDC into YES/NO pairs (PositionsSplit)
4. Merging YES/NO back to USDC (PositionsMerge)
5. All USDC flows tracked

**Critical note:** Earlier studies (DeFi Oasis, Dec 2025) that excluded splits/merges overstated profitability because one category of expenses was invisible.

## Platform Context

- 2.5 million wallets analyzed
- ~$9.8B notional trading volume past 30 days (as of April 2026)
- Zero-sum game: explains why such a small minority profits
- April 2026 referral program could drive wave of retail signups → deepen loss problem without better education

## What This Means for Strategy

Consistent profitability requires:
1. Systematic approach (not intuition)
2. Mathematical discipline (Kelly sizing)
3. Sustainable edge (not curve-fitted to historical data)
4. Surviving long enough to let strategy work

The data suggests most "profitable" traders benefit from short-term variance, not genuine edge — they stop trading once variance turns against them.

## Dune Analytics Dashboards

- Profitable trader feed: dune.com/coinist/polymarket-profitable-trader-feed
- Activity & volume: dune.com/filarm/polymarket-activity
- Full analysis: dune.com/rchen8/polymarket
