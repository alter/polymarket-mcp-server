---
title: "Market Calibration for In-Play Football Forecasting (Weibull AFT Model)"
url: https://arxiv.org/html/2605.16066
source: arxiv
date: "2026-05-01"
type: paper
theme: sports
lang: en
---

# Market Calibration for In-Play Football Forecasting

**arXiv:** 2605.16066 | 2026

## Abstract

Demonstrates that combining a **Weibull accelerated failure time model** with calibration to Betfair Exchange prices achieves performance nearly matching market-derived probabilities while maintaining interpretable parameters. Testing across 140 Premier League matches yields **4.5% ROI** through Kelly-staked bets (Sharpe ratio: 5.94).

## Data

- 4 EPL seasons (2021–2025), 1,517 fixtures
- Sources: WhoScored.com (match events), FBref.com (post-shot xG), Betfair Exchange (minute-by-minute odds)
- Train: 1,377 matches; Test: 140 matches (latter half of 2024-25)

## Core Model: Weibull AFT

Goal inter-arrival times follow Weibull distributions:
- `T_H ~ Weibull(γ, λ_H)` (home team)
- `T_A ~ Weibull(γ, λ_A)` (away team)

Shape parameter γ > 1 means scoring probability increases over time — crucial for football dynamics.

**Team strength parameterization:**
```
log E[T_H] = μ + β_home + a_H + d_A
log E[T_A] = μ + a_A + d_H
```

## Key Innovations

### 1. Market Calibration (dominant driver)
- Uncalibrated model: 56.4% pre-match accuracy
- After calibration to Betfair odds: **61.4%** (matches Betfair itself: 70.6% in-play)
- Calibration minimizes squared error between model and market probabilities across 1X2 + over/under markets

### 2. Second Half Scoring Dynamics
- First half shape: γ_1H = 0.98
- Second half shape: γ_2H = 1.40 (42% increase in hazard)
- ΔBIC = -302.9 improvement

### 3. Shot Quality Covariate (PSxG)
- Post-shot expected goals (trajectory + location) as dynamic covariate
- Coefficient β̂_psxg = -0.10: above-average shot quality accelerates expected goal arrival
- ΔBIC = -53.4 improvement

## Results

| Model | In-Play Accuracy | RPS | Log-loss |
|---|---|---|---|
| **Calibrated Weibull** | **70.2%** | **0.1294** | **0.6933** |
| Maia calibrated | 69.4% | 0.1303 | 0.6963 |
| Betfair Exchange | 70.6% | 0.1254 | 0.6714 |
| Zou uncalibrated | 68.2% | 0.1412 | 0.7569 |

## Betting Simulation (Kelly staking, 2% commission)

- 17,458 bets placed
- **4.5% ROI** (158.15 unit profit)
- **Sharpe ratio: 5.94**
- Unit staking: -3.4% ROI (late-game odds near 1.0 make single incorrect bets costly)

## Core Finding

**"Calibration to market prices is the dominant driver of predictive accuracy"** — applies across structurally different models. This principle generalizes.

## Transferable Edge for Polymarket

1. Calibrate any model's team/player strength estimates to current contract prices
2. Use post-shot xG or equivalent quality metrics as real-time covariates
3. Kelly staking (not flat) is essential for late-market volatility management
4. In-play betting has ~60% of sports betting market share — the largest opportunity pool

## Code Availability

Authors make code available; funded by EPSRC.
