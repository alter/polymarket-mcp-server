---
title: "NegRisk Market Rebalancing: How $29M Was Extracted From Multi-Condition Prediction Markets"
url: "https://medium.com/@navnoorbawa/negrisk-market-rebalancing-how-29m-was-extracted-from-multi-condition-prediction-markets-2f1f91644c5b"
source: "medium.com"
date: "2025-08-01"
type: "blog"
theme: "arb"
lang: "en"
---

# NegRisk Market Rebalancing: How $29M Was Extracted From Multi-Condition Prediction Markets

**Author:** Navnoor Bawa

**URL:** https://medium.com/@navnoorbawa/negrisk-market-rebalancing-how-29m-was-extracted-from-multi-condition-prediction-markets-2f1f91644c5b

---

## Overview

Based on IMDEA Networks analysis of 86 million Polymarket transactions (April 2024–April 2025). Breakdown of the $39.59M total arbitrage into its components, with NegRisk rebalancing as the dominant strategy.

---

## Core Concept: NegRisk Markets

NegRisk markets = N≥3 mutually exclusive conditions where Σ(prices) must equal 1.0

**The fundamental inefficiency:** Retail trading concentrates liquidity on 1-2 favored outcomes, leaving complementary probability spaces underpriced or overpriced — creating mispricings where the sum of all outcome probabilities != 1.0.

---

## Profit Breakdown

### Total: $39,587,585

| Strategy | Profit | % of Total |
|---|---|---|
| NegRisk rebalancing (buy NO) | $17,310,000 | 43.7% |
| NegRisk rebalancing (buy YES) | $11,090,000 | 28.0% |
| Single-condition (buy YES) | $5,900,000 | 14.9% |
| Single-condition (buy NO) | $4,680,000 | 11.8% |
| Selling strategies | $616,000 | 1.6% |

**NegRisk total: ~$28,990,000 (73% of all arbitrage)**

### Capital Efficiency Comparison
- NegRisk rebalancing: 8.6% of available opportunities → 73% of profits
- **29× capital efficiency advantage** vs. single-condition arbitrage
- Average profit per NegRisk opportunity: $43,800
- Average profit per single-condition opportunity: $1,500

---

## Top Performer Analysis

- **Best wallet:** $2,009,631.76 across 4,049 transactions
- Average: $496 per trade
- Strategy: systematic NegRisk rebalancing
- Behavior: highly automated (bot-like), consistent execution patterns

**Top 10 arbitrageurs earned $200K–$380K each** over the 12-month period.

---

## Why Mispricings Persist

1. Retail traders focus on 1-2 favorites, ignoring tail outcomes
2. New candidate/outcome additions to existing markets create temporary imbalances
3. Breaking news moves primary market faster than related markets
4. Market maker hesitancy in multi-outcome markets (more complex delta-hedging)

---

## Fee Considerations
- Polymarket 2% winner fee means spreads must exceed 2% to be profitable
- Typically spreads of 2.5% to 3% needed after fees
- NegRisk opportunities typically offered 3-5% net profit margin

---

## Implementation

**For YES-price-sum < 1.0:** Buy all YES via standard market (cost < $1.00, payout = $1.00)

**For YES-price-sum > 1.0:** Buy all NO via standard market (costs < $1.00 total when converted, payout = $1.00 from winning NO)

**NegRisk Adapter contract** enables the NO→YES conversion but this is for capital efficiency, NOT for creating arbitrage profit itself — true arbitrage must be executed through standard market interface.
