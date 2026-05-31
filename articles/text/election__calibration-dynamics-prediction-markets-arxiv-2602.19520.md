---
title: "Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets"
url: "https://arxiv.org/abs/2602.19520"
source: "arXiv"
date: "2026-02-23"
type: "academic_paper"
theme: "election"
lang: "en"
---

# Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets

**Author:** Nam Anh Le  
**ArXiv ID:** 2602.19520  
**Submitted:** February 23, 2026

## Abstract

Prediction markets are increasingly used as probability forecasting tools, yet their usefulness depends on calibration — specifically whether a contract trading at 70 cents truly implies a 70% probability. Using 292 million trades across 327,000 binary contracts on Kalshi and Polymarket, this paper shows that calibration is a structured, multidimensional phenomenon. The research identifies four distinct components explaining calibration variance.

## Data

- **Kalshi:** 64.7 million trades across 210,608 contracts
- **Polymarket:** 227.6 million trades across 116,000 contracts
- **Domains:** Sports, Politics, Crypto, Finance, Weather, Entertainment
- **Analysis dimensions:** Nine time bins × four trade-size categories

## Four-Component Decomposition (87.3% of calibration variance explained on Kalshi)

**1. Universal Horizon Effect (30.2% of variance)**
All domains show underconfidence about distant futures. Calibration slopes rise from 0.99 (within 1 hour) to 1.32 (beyond 1 month), meaning prices compress toward 50% as resolution approaches. This replicates across both exchanges.

**2. Domain-Specific Biases (14.6% of variance)**
Politics exhibits persistent underconfidence (+0.15 intercept), with prices chronically compressed. A 70-cent political contract actually represents ~83% probability one week before resolution. Weather and Entertainment show opposite patterns (−0.09), with overconfident pricing.

**3. Domain-by-Horizon Interactions (26.0% of variance)**
The largest component reveals genuinely different trajectories. Sports markets perform well short-term (0.90–1.10 slopes) but deteriorate beyond one month (1.74). Weather shows overconfidence initially before transitioning to underconfidence.

**4. Trade-Size Scale Effect (16.5% of variance)**
On Kalshi, large political trades exhibit slopes of 1.74 versus 1.19 for single-contract trades (Δ=0.53, 95% CI [0.29,0.75]). This effect does not replicate on Polymarket (Δ=0.11, [−0.15,0.39]), suggesting platform-specific microstructure rather than universal dynamics.

## Key Statistics by Domain

| Domain | Calibration Pattern |
|--------|-------------------|
| Politics | Underconfident (slopes 0.93–1.83) |
| Sports | Well-calibrated short-term (0.90–1.10) |
| Weather | Overconfident short-term (0.69–0.97) |
| Crypto/Finance/Entertainment | Near-calibrated or mixed |

**Cross-Platform Validation:** Political underconfidence confirmed on Polymarket (mean slope 1.31 vs Kalshi 1.64), validating this as a structural phenomenon rather than exchange-specific artifact.

## Mechanistic Interpretations

**Bilateral Cancellation:** Political markets attract passionate traders on opposing sides whose large bets cancel, compressing prices toward 50% despite divergent true probabilities.

**Signal Over-Reaction:** Weather's short-horizon overconfidence reflects traders over-interpreting meteorological forecasts.

**Information Convergence:** Sports' superior short-term calibration reflects continuous, quantifiable public information (scores, statistics, injury reports) enabling smooth price discovery.

## Practical Implications

Consumers treating prediction market prices as face-value probabilities systematically misinterpret them. Journalists reporting political market prices at face value understate the confidence warranted. The authors recommend domain-specific recalibration matrices and position limits in politically polarized markets.

## Model Performance

- **Frequentist model:** R² = 0.873 (87.3% variance explained)
- **Bayesian hierarchical model:** 96.3% posterior predictive coverage across 216 cells
- **Maximum parameter discrepancy between approaches:** 0.005
