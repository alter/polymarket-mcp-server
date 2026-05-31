---
title: "In-Play Football Forecasting with Market-Calibrated Weibull Models"
url: "https://arxiv.org/html/2605.16066"
source: "arxiv"
date: "2026-05"
type: "academic_paper"
theme: "category"
lang: "en"
---

# In-Play Football Forecasting with Market-Calibrated Weibull Models

arXiv:2605.16066 (May 2026)

## Abstract

This research addresses the challenge of predicting football match outcomes during live play by bridging the accuracy gap between statistical models and betting exchange prices. The authors developed an enhanced Weibull accelerated failure time model that incorporates two key innovations: calibrating team strength parameters to Betfair Exchange pre-match odds, and integrating post-shot expected goals (PSxG) as a dynamic covariate. When evaluated across 140 English Premier League matches, the calibrated model achieves classification accuracy of 70.2%—nearly matching Betfair's 70.6%—while preserving interpretable team-level parameters. A betting simulation against live odds generated **4.5% return on investment** using Kelly staking, suggesting exploitable inefficiencies in in-play markets.

## Introduction & Problem Context

In-play betting now represents approximately **60% of sports wagering markets**, yet forecasting models have consistently underperformed market prices. The researchers note that "goals are sufficiently rare that models must extract signal from non-scoring events" to meaningfully update predictions between goals.

## Methodology Overview

**Core Model Structure:** The Weibull AFT framework models goal inter-arrival times for home and away teams independently. The research discovered that "the hazard increases substantially faster in the second half," with γ₁H=0.98 (first half) versus γ₂H=1.40 (second half).

**Market Calibration:** Rather than fitting parameters solely to historical match data, the team calibrated scoring-rate parameters to simultaneously match both 1X2 outcome odds and over/under goal market prices using squared-error minimization.

**In-Play Features:** Post-shot expected goals (PSxG) as a time-varying covariate. Red card differences also influence scoring intensity estimates.

## Key Findings

**Predictive Performance:** The calibrated Weibull model achieved 70.2% classification accuracy, with log-loss of 0.693 compared to Betfair's 0.671. "Calibration to market prices is the dominant driver of predictive accuracy" rather than model architecture choice.

**Covariate Contributions:** Surprisingly, PSxG showed modest effects on probabilistic accuracy despite strong in-sample statistical significance. The dominant improvement came from market calibration itself.

**Economic Performance:** Under Kelly criterion staking, the model generated 4.5% ROI with Sharpe ratio 5.94 across 17,458 bets. This profitability persisted both during and outside time windows when goals were scored, suggesting the edge wasn't merely exploiting stale pricing.

## Methodological Contributions

The calibration approach, which "applies to any intensity-based goal arrival model," provides a general technique for integrating market information into probabilistic forecasting systems. The half-specific hazard parameters represent an important finding about temporal scoring dynamics in football.

## Limitations & Future Directions

Evaluation on 140 matches represents a small sample. Data source misalignment across providers and reliance on FBref's PSxG (subsequently discontinued) present practical constraints. Authors propose extending work through stronger in-play covariates (substitutions, expected threat from passes) and testing across multiple leagues.
