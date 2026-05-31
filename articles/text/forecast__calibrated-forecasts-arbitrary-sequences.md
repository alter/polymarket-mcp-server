---
title: "Calibrated Probabilistic Forecasts for Arbitrary Sequences"
url: https://arxiv.org/abs/2409.19157
source: arxiv
date: "2024-09-27"
type: paper
theme: forecast
lang: en
---

# Calibrated Probabilistic Forecasts for Arbitrary Sequences

**Authors:** Charles Marx, Volodymyr Kuleshov, Stefano Ermon
**Submitted:** September 27, 2024; Last revised February 28, 2025 (v2)
**arXiv:** 2409.19157 [cs.LG, stat.ML]

## Abstract

Develops a framework ensuring valid uncertainty estimates regardless of how data evolves, leveraging Blackwell approachability from game theory to guarantee calibrated probabilistic forecasts.

## Key Findings

1. **Universal calibration guarantee:** Framework produces calibrated probabilistic forecasts for any compact outcome space (classification and bounded regression tasks), even under unpredictable data shifts, feedback loops, and adversarial scenarios.

2. **Recalibration while maintaining accuracy:** Methods recalibrate existing forecasters without degrading predictive accuracy.

3. **Algorithm:** Gradient-based algorithms plus specialized approaches for common use cases (energy systems tested).

4. **Theoretical foundation:** Blackwell approachability concepts provide formal guarantees on forecast calibration across arbitrary sequences.

## Relevance to Prediction-Market Pricing

The adversarial calibration guarantee is relevant for prediction markets where the data-generating process (market participants, information flows) is non-stationary and potentially adversarial. Provides a principled approach to maintaining calibration of a pricing model even as market conditions evolve.
