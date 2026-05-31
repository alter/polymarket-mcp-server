---
title: "Application of the Kelly Criterion to Prediction Markets"
url: "https://arxiv.org/abs/2412.14144"
source: "arXiv"
date: "2024-12-18"
type: "academic_paper"
theme: "kelly"
lang: "en"
---

# Application of the Kelly Criterion to Prediction Markets

**Author:** Bernhard K Meister  
**arXiv ID:** 2412.14144 [q-fin.PM]  
**Submitted:** December 18, 2024  
**Length:** 6 pages

## Abstract

The paper examines how the Kelly Criterion applies to betting and prediction markets. Mean beliefs generally differ from prices in prediction markets, and logarithmic utility theory is employed to analyze risk-return adjustments. Key contributions include proposing a modified payout structure and investigating a simple asset pricing model based on biased coin flips, using Kullback-Leibler divergence to demonstrate how errors in estimating bias and calculating investment fractions affect portfolio growth rates.

## Key Topics Covered

- Kelly Criterion application to prediction market betting
- Logarithmic utility framework for portfolio analysis
- Kullback-Leibler (KL) divergence as a measure of misjudgment effects
- Modified payout structures for betting markets
- Biased coin flip model for asset pricing
- Impact of miscalculations on portfolio growth

## Key Findings

**Price-Probability Divergence**: Market prices of simple binary betting markets do not normally match the probabilities that such events occur. This occurs because:
- Investors incorporate risk aversion into their bets
- Asymmetric payouts create leverage differences between sides
- Capital constraints and market structure affect pricing

**Mathematical Framework**: Using logarithmic utility maximization, the paper derives the optimal investment fraction when an investor's subjective belief (q) differs from market price (p).

**Modified Payout Structure**: The author proposes adjusting payout ratios using a parameter α to make markets more attractive near price extremes, potentially improving liquidity by making high-probability bets more appealing when prices cluster near boundaries.

**KL Divergence Analysis**: Using Kullback-Leibler divergence, the paper quantifies performance losses from:
- Misestimating probability: linear sensitivity to prediction errors
- Miscalculating investment fraction: quadratic sensitivity around optimal allocation

**Prediction Markets vs Standard Finance**: Unlike conventional financial markets, prediction market prices are bounded not just below but also above — this alters dynamics and investment possibilities.

## Practical Formula for Binary Prediction Markets

For binary prediction markets where you buy at price c and receive $1 if correct:
- `kelly_fraction = (p - c) / (1 - c)` for buying YES
- `kelly_fraction = (q - (1-c)) / c` for buying NO

Example: market prices YES at 0.60, estimated true probability is 0.72. Kelly says risk 30% of bankroll: `kelly = (0.72 - 0.60) / (1 - 0.60) = 0.30`.

## Conclusion

The research bridges information theory and behavioral finance, showing how mathematical tools reveal gaps between theoretical probabilities and market prices, with practical implications for prediction market design.
