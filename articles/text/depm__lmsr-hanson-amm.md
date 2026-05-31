---
title: "LMSR: Logarithmic Market Scoring Rule for Prediction Markets"
url: "https://blog.gensyn.ai/lmsr-logarithmic-market-scoring-rule/"
source: blog.gensyn.ai
date: "2023-01-01"
type: article
theme: depm
lang: en
---

# LMSR: Logarithmic Market Scoring Rule for Prediction Markets

**Source:** Gensyn AI Blog  
**URL:** https://blog.gensyn.ai/lmsr-logarithmic-market-scoring-rule/  
**Originator:** Robin Hanson (2003, 2007)  
**Also see:** https://arxiv.org/pdf/2102.07308 (Log-time Prediction Markets)

## Overview

The Logarithmic Market Scoring Rule (LMSR) is an automated market maker (AMM) mechanism specifically designed for prediction markets. Originally proposed by Robin Hanson as a way to fill liquidity gaps by running prediction markets using a cost function.

LMSR guarantees continuous liquidity—you can always buy or sell shares of an outcome without waiting for a counterparty. Prices update smoothly from the very first trade through final settlement.

## Core Cost Function

**C(q) = b · log(Σe^(q_i/b))**

Where:
- `q` = vector of net shares sold per outcome
- `b` = liquidity parameter controlling market depth
- Instantaneous prices derive from this function's partial derivative
- Prices always range between 0 and 1 and sum to 100%

The price of outcome i: **p_i = e^(q_i/b) / Σe^(q_j/b)**

## Key Properties

1. **Continuous Liquidity:** Unlike order-book platforms, always available as automated counterparty
2. **Probability-Like Prices:** Prices live between 0 and 1 and behave like probabilities; mutually coherent across outcomes
3. **Truthful Incentives:** Financially incentivizes market participants to give truthful opinions about future events (proper scoring rule)
4. **Bounded Risk:** Worst-case losses capped at b·log(n) where n is number of outcomes
5. **Smooth Price Impact:** Predictable slippage, no counterparty matching needed
6. **Path Independence:** Cost depends only on current and target share quantities, not the path taken

## The b Parameter

By adjusting the value of b, the market maker can control:
- **Higher b:** More market depth, slower price movement, larger worst-case loss
- **Lower b:** Thinner liquidity, faster price response, smaller worst-case loss
- Trade-off: information efficiency vs. market maker subsidy

## Comparison with Order Books

| Feature | CLOB (Order Book) | LMSR |
|---------|-------------------|------|
| Liquidity | Depends on participants | Always available |
| Price discovery | Immediate if depth exists | Continuous |
| Market maker loss | None (passive) | Bounded at b·log(n) |
| Manipulation resistance | Order book depth | Bounded loss |
| Best for | Liquid markets | Illiquid/thin markets |

## Variants

**LS-LMSR (Liquidity Sensitive LMSR)** (Othman et al., 2013): Adjusts b dynamically based on trading volume—liquidity grows with activity, improving efficiency.

Under LS-LMSR: profits are higher than standard LMSR, excluding edge cases. Volume typically increases under standard LMSR as b grows.

## Real-World Deployments

- **Gates Hillman Prediction Market** (Carnegie Mellon): LMSR on 365 outcome intervals for building opening date
- **Enterprise applications:** Product-sales forecasting, instructor ratings, political events
- **Gnosis Conditional Tokens:** LMSR AMM for conditional token markets
- **Zeitgeist (neo-swaps):** LMSR implementation as constant function market maker

## Comparison: LMSR vs. CPMM

Polymarket and modern platforms predominantly use **CLOB** (Central Limit Order Book) rather than LMSR, because:
- CLOBs enable tighter spreads for liquid markets
- LMSR works better for thin/new markets without participants
- Hybrid approaches (LMSR seeding → CLOB trading) are increasingly common

## Academic Reference

Carvalho, Silveira, Ely, Cajueiro (2023). "A Logarithmic Market Scoring Rule Agent-Based Model to Evaluate Prediction Markets." Journal of Evolutionary Economics. https://link.springer.com/article/10.1007/s00191-023-00822-w
