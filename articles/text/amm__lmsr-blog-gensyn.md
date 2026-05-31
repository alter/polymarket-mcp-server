---
title: "LMSR (Logarithmic Market Scoring Rule)"
url: "https://blog.gensyn.ai/lmsr-logarithmic-market-scoring-rule/"
source: "blog.gensyn.ai"
date: "2023-01-01"
type: "blog"
theme: "amm"
lang: "en"
---

# LMSR (Logarithmic Market Scoring Rule)

**Source:** Gensyn AI Blog

## What is LMSR?

The Logarithmic Market Scoring Rule (LMSR) is a mathematical formula used in prediction markets to determine the prices of different outcomes. It was invented by Robin Hanson and is becoming the de facto standard market maker for prediction markets.

## Key Properties

- **Bounded loss** that grows logarithmically in the number of outcomes
- **Infinite liquidity** — always a quote, always an instant fill
- **Modularity** — respects independence relationships between outcomes
- **Coherent prices** — in mutually exclusive markets, if one outcome's probability goes up, others adjust automatically
- **Prices as probabilities** — always between 0 and 1

## Cost Function Formula

Cost(q_YES, q_NO) = b * ln(e^(q_YES/b) + e^(q_NO/b))

Where b is the liquidity parameter controlling price sensitivity.

Price(YES) = e^(q_YES/b) / (e^(q_YES/b) + e^(q_NO/b))

## LMSR vs. Order Book

| Property | LMSR | Order Book |
|---|---|---|
| Counterparty needed | No | Yes |
| Liquidity | Continuous, algorithmic | Depends on participants |
| Price impact | Predictable (log curve) | Depends on depth |
| Thin market performance | Excellent | Poor |
| Manipulability | Harder (inventory-based) | Easier |

## Applications

Used by: Inkling Markets, Microsoft (internal forecasting), Gates Hillman Prediction Market (CMU), various research prediction markets. Now foundational for DeFi AMMs (Uniswap, etc. trace lineage to LMSR).
