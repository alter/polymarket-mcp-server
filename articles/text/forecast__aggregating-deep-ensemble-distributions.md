---
title: "Aggregating distribution forecasts from deep ensembles"
url: https://arxiv.org/abs/2204.02291
source: arxiv
date: "2022-04-05"
type: paper
theme: forecast
lang: en
---

# Aggregating Distribution Forecasts from Deep Ensembles

**Authors:** Benedikt Schulz, Lutz Köhler, Sebastian Lerch
**Submitted:** April 5, 2022; Last revised November 8, 2024
**arXiv:** 2204.02291 [stat.ML]

## Abstract

Addresses how to aggregate distribution forecasts from deep ensembles by combining insights from machine learning ensemble methods and statistical forecast combination literature.

## Key Findings

1. **Improved Performance:** "Combining forecast distributions from deep ensembles can substantially improve the predictive performance."

2. **Quantile Aggregation Framework:** Proposes a general quantile aggregation framework for deep ensembles that allows for corrections of systematic deficiencies (overdispersion, underdispersion).

3. **Superior Method:** Quantile-based approach "performs well in a variety of settings, often superior compared to a linear combination of the forecast densities (linear opinion pool)."

4. **Three Neural Network Approaches:** Systematically compared probability- and quantile-based aggregation across twelve benchmark datasets.

5. **Ensemble size effects:** Investigated the relationship between ensemble size and improvement gain.

## Methodology

Three neural network forecast types evaluated:
- Standard NNs
- Distributional Regression Networks (DRNs)
- Normalizing flows

Aggregation methods: linear opinion pool (probability combining) vs. quantile aggregation (quantile combining)

## Relevance to Prediction-Market Pricing

Provides practical guidance for combining multiple deep learning models' probability forecasts for Polymarket markets. The quantile aggregation framework is superior to naive averaging — relevant for any ensemble-based probability estimation system.
