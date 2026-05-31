---
title: "Statistical Enhanced Learning for Modeling and Prediction of Tennis Matches at Grand Slam Tournaments"
url: https://arxiv.org/abs/2502.01613
source: arxiv
date: "2025-02-01"
type: paper
theme: sports
lang: en
---

# Statistical Enhanced Learning for Tennis Match Prediction at Grand Slam Tournaments

**arXiv:** 2502.01613v2

## Abstract

Focuses on "statistically enhanced" covariates for modelling men's Grand Slam tennis matches, assessing whether these features improve predictive performance. Evaluates regression and ML models using three enhanced variables: Elo rating and two player age measurements.

## Key Enhanced Features

1. **Elo rating**: Dynamic skill estimate updated after each match
2. **Player age variables** (two formulations): career stage effects on performance
3. Compared against standard features (ATP ranking, head-to-head records, surface stats)

## Validation Methodology

Three strategies:
- Cross-validation
- Rolling window
- Expanding window

Metrics: classification rate, predictive Bernoulli likelihood, Brier score.

## Key Results

Enhanced covariates "demonstrate potential to improve the predictive performance" of statistical learning approaches.

Interpretability tools:
- **Partial dependence plots (PDP)**: Shows marginal effect of each feature
- **Individual conditional expectation (ICE)** plots: Heterogeneous treatment effects per player

Random forest benefits most from engineered covariates.

## Methodological Connection

Builds on methodology previously successful in **football analytics** — demonstrating transferability of statistical learning frameworks across sports.

## Transferable Insights

1. Elo is the highest-signal single feature for tennis prediction
2. Player age as a nonlinear feature (not linear) captures career arc effects
3. Expanding window validation is the correct approach for sequential sports data
4. PDP/ICE plots essential for understanding model behavior before deployment
5. Grand Slams have different dynamics than regular tour events — separate models warranted
