---
title: "Logarithmic Market Scoring Rules for Modular Combinatorial Information Aggregation"
url: https://mason.gmu.edu/~rhanson/mktscore.pdf
source: gmu-edu
date: "2007-02-01"
type: paper
theme: forecast
lang: en
---

# Logarithmic Market Scoring Rules for Modular Combinatorial Information Aggregation

**Author:** Robin Hanson
**Published:** Journal of Prediction Markets, 1(1), 3–15, February 2007
**PDF:** https://mason.gmu.edu/~rhanson/mktscore.pdf

## Overview

Introduces the Logarithmic Market Scoring Rule (LMSR), the most widely applied prediction market mechanism. Provides the theoretical foundation for automated market making in prediction markets.

## Key Concepts

**Market Scoring Rule (MSR):** A sequentially shared proper scoring rule. Traders arrive, report beliefs, and are paid based on the change in score between their prediction and the previous trader's prediction.

**LMSR Properties:**
1. **Bounded loss:** Market maker's maximum loss is bounded by b * ln(n) where b is the liquidity parameter and n is the number of outcomes
2. **Continuous liquidity:** Always willing to buy/sell at defined prices
3. **Probability-consistent prices:** Prices always lie in (0,1) and sum to 1
4. **Modularity (unique property):** A trade changing P(E|F) does not change P(F) — enables combinatorial prediction markets
5. **Incentive-compatible:** Traders cannot profit by misreporting beliefs

**Price function for binary LMSR:**
    p = exp(q) / (exp(q) + 1)

where q = quantity of YES shares sold / b (liquidity parameter)

**Cost function:**
    C(q) = b * ln(exp(q_yes/b) + exp(q_no/b))

## Connection to Proper Scoring Rules

LMSR is the market version of the logarithmic scoring rule. Every proper scoring rule corresponds to a market mechanism where traders improve the score and pocket the difference.

## Relevance to Prediction-Market Pricing

Polymarket uses an order book (not LMSR), but the LMSR pricing model remains the theoretical baseline for understanding prediction market price formation. The liquidity parameter b governs price sensitivity — understanding this informs strategies for trading in thin vs. liquid markets.

## Citation

Hanson, R. (2007). Logarithmic Market Scoring Rules for Modular Combinatorial Information Aggregation. *Journal of Prediction Markets*, 1(1), 3–15.
