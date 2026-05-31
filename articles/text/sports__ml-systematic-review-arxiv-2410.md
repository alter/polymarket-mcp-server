---
title: "A Systematic Review of Machine Learning in Sports Betting: Techniques, Challenges, and Future Directions"
url: https://arxiv.org/abs/2410.21484
source: arxiv
date: "2024-10-28"
type: paper
theme: sports
lang: en
---

# A Systematic Review of Machine Learning in Sports Betting

**Authors:** René Manassé Galekwa, Jean Marie Tshimula, Etienne Gael Tajeuna, Kyamakya Kyandoghere  
**arXiv:** 2410.21484 | October 28, 2024 | CC BY 4.0

## Overview

Systematic review synthesizing 219 peer-reviewed articles (2010–2024) across IEEE Xplore, Springer, Science Direct, MDPI, arXiv, Google Scholar. Covers soccer, basketball, tennis, cricket, American football, baseball, horse racing, rugby, golf, hockey.

## Methodology

- Initial: 259 articles → 219 after PRISMA inclusion criteria
- Algorithms evaluated: SVMs, random forests, neural networks, XGBoost, LightGBM, LSTM
- Metrics: accuracy, RPS (Ranked Probability Score), ROI, Sharpe, calibration

## Sport-Specific Findings

### Soccer (Football) — 52 studies
- Bayesian approaches: RPS 0.2620, 92% accuracy
- XGBoost: RPS 0.197, 89.6% accuracy
- Deep neural networks: 99% accuracy on specific datasets
- Random forests: 1.58% return per match

### Basketball — 13 studies
- XGBoost: 91.82% accuracy
- Logistic regression: 60.82–93.20% accuracy

### Tennis — 9 studies
- Markov chain models: **3.8% ROI**
- Logistic regression / ANN: **4.35% ROI**
- Random forests: 3.3% profit per match
- Serve strength identified as the most impactful predictor

### Cricket — 15 studies
- XGBoost: 94.23% accuracy (without tuning)

## Key Themes

1. **Value betting** (positive expected value identification) is the central bettor strategy
2. **Calibration** of probabilities outperforms raw accuracy
3. **Kelly Criterion** is the dominant staking algorithm across studies
4. **Market correlation problem**: An accurate model loses money if it correlates with the bookmaker's model — decorrelation is essential

## Common Data Sources
- football-data.co.uk, Kaggle, ESPN, Transfermarkt, ATP/WTA, NBA API, OddsPortal

## Universal Features
- Performance metrics (goals, shots, assists)
- Player data (rankings, age, form)
- Team indicators (win rates, scoring averages)
- Contextual factors (home/away, injuries, weather)
- Market data (betting odds, implied probabilities, line movements)

## Future Directions
- Multimodal data integration (tracking data, biometrics)
- Portfolio management frameworks (MPT applied to bet selection)
- Real-time in-play feature extraction
- Explainability (SHAP, ICE plots)

## Conclusion

ML significantly improves prediction accuracy over traditional methods. Multiple algorithms show comparable performance; sustainable profitability depends on calibration, market efficiency exploitation, and continuous model adaptation.
