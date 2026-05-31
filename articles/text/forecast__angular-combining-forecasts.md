---
title: "Angular Combining of Forecasts of Probability Distributions"
url: https://arxiv.org/abs/2305.16735
source: arxiv
date: "2023-05-26"
type: paper
theme: forecast
lang: en
---

# Angular Combining of Forecasts of Probability Distributions

**Authors:** James W. Taylor, Xiaochun Meng
**Submitted:** May 26, 2023; Last revised February 24, 2025 (v2)
**arXiv:** 2305.16735

## Abstract

Addresses the debate between combining probability distributions "vertically" (combining probabilities — linear opinion pool) vs. "horizontally" (combining quantiles). Proposes "angular combining" as a middle ground with the angle as an optimizable parameter.

## Key Findings

1. **Angular combining:** The combining angle θ continuously interpolates between:
   - θ = 90°: Pure vertical (probability) combining — linear opinion pool
   - θ = 0°: Pure horizontal (quantile) combining

2. **Theoretical properties:**
   - Preserves the mean: combined distribution mean = average of component means
   - Produces lower variance than vertical averaging
   - Under certain conditions, higher variance than horizontal averaging

3. **Empirical applications tested:**
   - COVID-19 mortality forecasts
   - Macroeconomic survey data
   - Electricity price predictions

4. **Optimization:** Angle θ selected via proper scoring rules on validation data.

## Relevance to Prediction-Market Pricing

When combining multiple probability forecasts for binary outcomes (as in Polymarket), the angular combining framework provides a principled way to choose between probability averaging and quantile averaging. The optimal angle varies by domain — worth calibrating on historical Polymarket data.
