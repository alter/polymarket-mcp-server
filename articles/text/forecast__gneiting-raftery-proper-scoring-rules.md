---
title: "Strictly Proper Scoring Rules, Prediction, and Estimation"
url: https://sites.stat.washington.edu/raftery/Research/PDF/Gneiting2007jasa.pdf
source: uw-statistics
date: "2007-03-01"
type: paper
theme: forecast
lang: en
---

# Strictly Proper Scoring Rules, Prediction, and Estimation

**Authors:** Tilmann Gneiting, Adrian E. Raftery
**Published:** Journal of the American Statistical Association, vol. 102, pages 359–378, March 2007
**DOI:** 10.1198/016214506000001437
**PDF:** https://sites.stat.washington.edu/raftery/Research/PDF/Gneiting2007jasa.pdf

## Abstract

Reviews and develops the theory of proper scoring rules on general probability spaces. A scoring rule is **proper** if the forecaster maximizes the expected score by issuing their true belief distribution F rather than any other G ≠ F. It is **strictly proper** if the maximum is unique.

## Key Concepts

**Proper scoring rules:**
- Assess quality of probabilistic forecasts by assigning a numerical score based on forecast and actual outcome
- Strictly proper rules encourage honest assessment and reporting
- Used for both prediction problems (eliciting calibrated forecasts) and estimation problems (loss functions)

**Major proper scoring rules covered:**
1. **Brier Score** (quadratic score): S(F, y) = −(y − F̄)² for binary outcomes
2. **Log Score** (logarithmic score): S(F, y) = log f(y) — connects to Shannon entropy and Bayesian inference
3. **Continuous Ranked Probability Score (CRPS):** Generalizes Brier score to continuous outcomes
4. **Energy Score:** Multivariate generalization
5. **Interval Score:** Addresses prediction interval width and coverage

**Characterization Theorem:** Proves fundamental characterization theorem for strictly proper scoring rules on general measurable spaces.

**Applications covered:** Probabilistic weather forecasting, meteorology, economics, medicine, machine learning

## Key Results

- Characterization of strictly proper scoring rules via convex functions
- Connections between log score, Bayesian inference, and KL divergence
- Interval score as utility function for prediction intervals: balances width against coverage
- Scoring rules and minimum contrast estimation: proper scoring rules = valid loss functions

## Relevance to Prediction-Market Pricing

Foundational paper for understanding Brier score decomposition, CRPS, and log scoring used in prediction market evaluation. The LMSR (Hanson's prediction market mechanism) is directly derived from the log scoring rule. Required reading for anyone designing probability calibration for trading systems.

## Citation

Gneiting, T., & Raftery, A.E. (2007). Strictly proper scoring rules, prediction, and estimation. *Journal of the American Statistical Association*, 102(477), 359–378.
