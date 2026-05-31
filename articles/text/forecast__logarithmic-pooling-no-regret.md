---
title: "No-Regret Learning with Unbounded Losses: The Case of Logarithmic Pooling"
url: https://arxiv.org/abs/2202.11219
source: arxiv
date: "2022-02-22"
type: paper
theme: forecast
lang: en
---

# No-Regret Learning with Unbounded Losses: The Case of Logarithmic Pooling

**Authors:** Eric Neyman, Tim Roughgarden
**Submitted:** February 22, 2022; revised October 10, 2023
**arXiv:** 2202.11219

## Abstract

Addresses learning to aggregate probability distributions from multiple experts across time steps in an adversarial setting. Focuses on logarithmic pooling (a weighted average of log odds), the optimal aggregation method for minimizing log loss.

## Key Findings

1. **Algorithm:** Online mirror descent achieves learning of expert weights with "O(√T log T) expected regret as compared with the best weights in hindsight."

2. **Semi-adversarial framework:** Outcomes and forecasts must be consistent with calibrated expert predictions — a novel constraint maintaining adversarial flexibility while ensuring forecast consistency.

3. **Logarithmic pooling optimality:** For log loss (log score), logarithmic pooling (geometric mean of odds) is provably optimal in the class of aggregation methods.

4. **External Bayesianality:** Logarithmic pooling satisfies this axiom, meaning Bayesian updating before or after aggregation gives the same result — a uniquely desirable property.

## Key Formulas

Logarithmic pool with equal weights:
    logit(p_pool) = (1/n) * Σ logit(pᵢ)

Which equals:
    p_pool = Geometric_Mean_of_Odds converted to probability

## Relevance to Prediction-Market Pricing

When aggregating probability estimates from multiple models or sources for Polymarket markets, logarithmic pooling (geometric mean of odds) provides theoretical no-regret guarantees. The O(√T log T) regret bound means the approach is competitive with the best fixed weighting in hindsight.
