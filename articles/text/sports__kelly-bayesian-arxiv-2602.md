---
title: "Kelly Betting as Bayesian Model Evaluation: A Framework for Time-Updating Probabilistic Forecasts"
url: https://arxiv.org/abs/2602.09982
source: arxiv
date: "2026-02-01"
type: paper
theme: sports
lang: en
---

# Kelly Betting as Bayesian Model Evaluation

**Author:** Michael Beuoy  
**arXiv:** 2602.09982 [stat.ME] | February 2026 | 31 pages, 10 figures

## Abstract

Proposes evaluating time-varying probabilistic forecasts by treating each model as a "canonical Kelly bettor" in an iterative betting contest. Model bankroll growth or decline serves as the evaluation metric, enabling real-time updates of model credibility without awaiting final outcomes.

## Methodology

- Models compete in iterative Kelly betting contests
- Each model's **bankroll trajectory = evaluation metric**
- Direct mathematical and conceptual analogue to **Bayesian inference**: bankroll → Bayesian credibility
- Compared against: average log-loss, Brier score

## Key Finding

"This Kelly-based approach is in general more accurate than traditional average log-loss and Brier score methods at distinguishing a correct model from an incorrect model."

## Why This Matters

Standard metrics (log-loss, Brier) treat all predictions equally. Kelly-based evaluation naturally weights predictions by confidence and opportunity size — exactly what a rational bettor does.

Benefits:
1. **Real-time assessment** of competing probabilistic forecasts
2. **No waiting** for final outcomes to evaluate model quality
3. **Bayesian principles** integrated with Kelly growth criterion
4. **Superior discrimination** between accurate and inaccurate models vs. conventional evaluation

## Transferable Edge for Prediction Markets

- Track model "bankroll" trajectories as an internal metric during development
- Models that grow a Kelly bankroll consistently are better models than those with lower loss/Brier — even before seeing final resolutions
- Particularly useful for time-series sports models where you want to distinguish signal from noise early
