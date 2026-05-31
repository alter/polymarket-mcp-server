---
title: "An intuitive rearranging of the Yates covariance decomposition for probabilistic verification of forecasts with the Brier score"
url: https://arxiv.org/abs/2603.05544
source: arxiv
date: "2026-03-04"
type: paper
theme: forecast
lang: en
---

# An intuitive rearranging of the Yates covariance decomposition for probabilistic verification of forecasts with the Brier score

**Author:** Bruno Hebling Vieira (Methods of Plasticity Research, Department of Psychology, University of Zurich)
**Submitted:** March 4, 2026
**arXiv:** 2603.05544 [stat.ME, cs.LG, stat.AP]

## Abstract

Proposes "a simple algebraic rearrangement of the Yates covariance decomposition" that separates the Brier score into three independently non-negative terms.

## Key Findings

The reformulation identifies three independent components of the Brier score:

1. **Variance mismatch term** — assesses whether forecast variance aligns with outcome variance
2. **Correlation deficit term** — measures departure from perfect positive correlation with actual outcomes
3. **Calibration-in-the-large term** — evaluates if forecast mean matches outcome mean

**Optimality conditions for perfect forecasting:** Forecasts must simultaneously:
- Match the variance of outcomes
- Achieve perfect positive correlation with outcomes
- Match the mean of outcomes

Any deviation from these three conditions contributes positively to the Brier score penalty.

## Relevance to Prediction-Market Pricing

Provides a diagnostic framework for decomposing forecasting errors. For a Polymarket trading system:
- **Variance mismatch:** Are model predictions as variable as market outcomes?
- **Correlation deficit:** Do predictions move in the right direction relative to outcomes?
- **Calibration-in-the-large:** Is the mean predicted probability close to the mean resolution rate?

Each component suggests a different type of fix.
