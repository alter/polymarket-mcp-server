---
title: "Logarithmic Regret in the Ergodic Avellaneda-Stoikov Market Making Model"
url: https://arxiv.org/abs/2409.02025
source: arxiv
date: "2024-09-03"
type: paper
theme: micro
lang: en
---

# Logarithmic Regret in the Ergodic Avellaneda-Stoikov Market Making Model

**Authors:** Jialun Cao, David Šiška, Lukasz Szpruch, Tanut Treetanthiploet

**Submitted:** September 3, 2024; Last revised July 14, 2025

**arXiv:** 2409.02025

## Abstract

Examines regret from learning the price sensitivity parameter in an ergodic market-making framework. Demonstrates that a maximum-likelihood estimator for the parameter achieves regret upper bound of order ln²T in expectation.

## Key Findings

1. **Regret scales as ln²T** — logarithmic regret is achievable in online market-making learning
2. **Maximum-likelihood estimator** for price sensitivity parameter k (in A-S intensity model)
3. **Hamilton-Jacobi-Bellman analysis** provides differentiability properties used in the proof
4. **Concentration inequalities** yield convergence rates for regularized estimators
5. Numerical experiments validate algorithm performance and robustness

## The Learning Problem

In A-S model, fill intensity: λ(δ) = A exp(-k δ)

The parameter k determines how sensitive order arrivals are to spread width. In practice:
- k is unknown and must be estimated online
- Overestimating k → quotes too tight → adverse selection
- Underestimating k → quotes too wide → no fills

**Regret** measures the cost of not knowing k from the start.

## Relevance to Polymarket CLOB Trading

- **Online calibration:** k parameter can be estimated in real-time on Polymarket using MLE — update as market conditions change
- **Logarithmic regret guarantee:** Provides theoretical comfort that the learning algorithm converges efficiently
- **Different k per market:** Different Polymarket markets (sports vs. politics vs. crypto) likely have different k values — market-specific calibration needed
- **Robustness:** The concentration inequality approach means estimates are reliable even in finite samples (applicable to thin Polymarket markets)
- **Ergodic setting:** Assumes stationary spread dynamics — valid for stable Polymarket markets; may break near resolution

**Classification:** Optimization and Control; Trading and Market Microstructure
