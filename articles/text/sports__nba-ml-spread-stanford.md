---
title: "Predicting the Betting Line in NBA Games (Stanford CS229)"
url: https://cs229.stanford.edu/proj2013/ChengDadeLipmanMills-PredictingTheBettingLineInNBAGames.pdf
source: stanford
date: "2013-01-01"
type: paper
theme: sports
lang: en
---

# Predicting the Betting Line in NBA Games

**Source:** Stanford CS229 Machine Learning Project  
**Authors:** Bryan Cheng, Dade, Lipman, Mills

## Objective

Expand upon existing NBA outcome prediction models using time-varying approaches. Couple standard ML techniques with weighted causal data to predict points scored by each team — beating the spread.

## Methods

- **Support Vector Machine** with various feature sets
- **Time-varying offensive and defensive strength** estimation
- Opponent-adjusted statistics (normalize each stat by opponent quality)

## Key Features

- Team-level stats (offensive/defensive ratings)
- Pace metrics (possessions per game)
- Rest days, home/away
- ELO ratings prior to each game
- Cover percentage (historical ATS performance)

## Industry Context

Vegas lines predict game winners and margins with average miss of **~9 points per game** (MAE ≈ 9.35).

Best ML models in literature achieved:
- MAE: 9.18 (slightly better than Vegas)
- RMSE: 11.73
- R²: 0.88 for point spread prediction
- Game-winner prediction: 70.15% accuracy

"Models are theoretically profitable" when MAE < bookmaker line (9.18 vs 9.35).

## Critical Insight: Pace

"Pace is considered the single most important contextual feature in NBA modeling. A game played at 105 possessions per team has roughly 15% more scoring opportunities than one played at 92 possessions."

## Transferable Insights

1. Opponent-adjusted statistics outperform raw stats for prediction
2. Pace normalization is essential in basketball models
3. Time-varying strength estimates (not just season averages) are crucial
4. The Vegas line itself is a powerful feature — train model to beat it, not replicate it
5. MAE comparison to bookmaker line is the correct validation metric (not just accuracy)

## GitHub Implementations

- kyleskom/NBA-Machine-Learning-Sports-Betting: XGBoost + neural networks for moneyline and totals, Kelly criterion sizing
- NBA-Betting/NBA_Betting: Comprehensive point spread system with all contextual factors
