---
title: "Combining Historical Data and Bookmakers' Odds in Modelling Football Scores"
url: https://arxiv.org/abs/1802.08848
source: arxiv
date: "2018-02-01"
type: paper
theme: sports
lang: en
---

# Combining Historical Data and Bookmakers' Odds in Modelling Football Scores

**arXiv:** 1802.08848

## Abstract

The authors develop a **hierarchical Bayesian Poisson model** in which the scoring rates of teams are convex combinations of parameters estimated from historical data and bookmakers' betting odds. Nine years of European league data used to predict match outcomes for subsequent seasons.

## Methodology

- **Model type:** Hierarchical Bayesian Poisson regression
- **Data integration:** Convex combinations of two sources:
  1. Historical team performance statistics
  2. Parameters implied from betting market odds
- **Dataset:** Nine-year records from major European football leagues
- **Application:** Out-of-sample prediction

## Core Insight

Betting odds encode collective market intelligence that historical statistics alone cannot capture (injuries, lineup changes, tactical adaptations, late news). Combining both sources through convex weighting produces superior predictions.

**Convex combination formula:**
λ_team = α × λ_historical + (1-α) × λ_market

Where α is learned from data, allowing the model to adaptively weight how much to trust historical form vs. market pricing.

## Transferable Edges

1. **Market integration**: Odds-implied parameters carry information beyond historical statistics — always include market prices as a feature
2. **Bayesian shrinkage**: Hierarchical structure shrinks team-specific estimates toward the league mean, reducing overfitting on small samples
3. **Convex weighting**: A simple α blending parameter handles the trust-historical vs. trust-market tradeoff without requiring complex architecture
4. **Direct applicability to Polymarket**: When modeling a sports contract, blend your own probability model with the current market price as a convex combination
