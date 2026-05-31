---
title: "Principled Extremizing of Aggregated Forecasts"
url: https://forum.effectivealtruism.org/posts/biL94PKfeHmgHY6qe/principled-extremizing-of-aggregated-forecasts
source: ea-forum
date: "2021-12-29"
type: blog
theme: forecast
lang: en
---

# Principled Extremizing of Aggregated Forecasts

**Author:** Jaime Sevilla
**Date:** December 29, 2021
**Publication:** EA Forum

## Content Summary

Recommends a theoretically grounded method for extremizing aggregated forecasts based on research by Eric Neyman and Tim Roughgarden.

## Core Recommendation

Apply an extremization factor of d ≈ √3 ≈ 1.73 for aggregations with more than 50 forecasters. The formula adjusts log-odds aggregates away from baseline values:

    log_O = d * (1/n) * Σ log(Oᵢ)

(when assuming neutral baseline assumptions)

## Theoretical Foundation

Neyman and Roughgarden's research demonstrates that extremizing works optimally under the "projective substitutes condition" — essentially meaning "diminishing marginal returns to more forecasts" apply. This provides principled justification beyond empirical overfitting concerns.

The √3 ≈ 1.73 factor emerges from worst-case analysis as a minimax-optimal extremization constant.

## Empirical Validation

Testing on 899 resolved Metaculus binary questions showed Neyman's method outperforming simpler aggregation approaches. When incorporating historical resolution rates as baselines, performance improved further.

## Key Uncertainties

- Approximation ratio may not be the ideal optimization metric
- Applying results from real-value estimation to discrete log-odds aggregation requires additional theoretical development
- Some hindsight bias possible when using historical base rates

## Relevance to Prediction-Market Pricing

The 1.73 extremization factor is a practical, theoretically-grounded rule for adjusting averaged probability forecasts. When aggregating multiple models' or forecasters' predictions about Polymarket markets, multiply log-odds by 1.73 before converting back to probability.
