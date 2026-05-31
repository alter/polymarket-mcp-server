---
title: "Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets"
url: "https://arxiv.org/abs/2602.19520"
source: arXiv (econ.GN)
date: "2026-02-01"
type: academic_paper
theme: manip
lang: en
---

# Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets

**Author:** Nam Anh Le  
**arXiv:** 2602.19520  
**Submitted:** February 2026  
**PDF:** https://arxiv.org/pdf/2602.19520

## Dataset

- **292 million trades** across **327,000 contracts**
- Platforms: Kalshi and Polymarket
- 6 domains × 9 time-to-resolution bins × 4 trade-size categories = 216 analysis cells

## Method

Logistic recalibration: `logit(P(outcome=1)) = a + b·logit(price)`

- b = 1.0 → perfect calibration
- b > 1 → underconfidence (prices compressed toward 50%)
- b < 1 → overconfidence (prices at extremes)

## Key Findings

### Four Components Explain 87.3% of Calibration Variance

| Component | Variance Explained |
|-----------|-------------------|
| Universal horizon effect | 30.2% |
| Domain-by-horizon interactions | 26.0% |
| Trade-size scale effect | 16.5% |
| Domain-specific biases | 14.6% |

### 1. Universal Horizon Effect
All markets show **underconfidence at long horizons**: calibration slopes rise from 0.99 (within 1 hour) to 1.32 (beyond 1 month). Distant contracts systematically understate confident outcomes.

### 2. Domain-Specific Biases
- **Politics:** Persistent underconfidence (+0.15 intercept). A 70¢ political contract one week before resolution → true probability closer to **83%**, not 70%. Confirmed on **both Kalshi and Polymarket** → "structural property of political prediction markets, not single-platform artefact."
- **Weather/Entertainment:** Overconfidence (−0.09 intercept each)

### 3. Favorite-Longshot Bias
FLB documented and worsens with time to expiration.

### 4. Trade-Size Effects (Platform Divergence)
- Kalshi: Large political trades (>100 contracts) → slope 1.74 vs. 1.19 for single-contract trades
- Polymarket: This effect barely appears (Δ = 0.11 vs. Δ = 0.53 on Kalshi) — platform-specific microstructure difference

## Explanatory Mechanisms Proposed

1. **Bilateral Cancellation (Politics):** Opposing partisan bets pull prices toward 50%; intensified when large positions from conviction traders accumulate
2. **Signal Over-reaction (Weather):** Markets overreact to meteorological signals at short horizons relative to base rates
3. **Information Convergence (Sports):** Continuous, quantifiable public information produces smooth price convergence at short-to-medium horizons

## Practical Implications

Consumers treating prediction market prices as face-value probabilities are systematically misled. Direction and magnitude of error depend on domain, time horizon, and trading volume. Market designers might consider:
- Position limits in political markets
- Equal-weight aggregation over wealth-weighted pricing
- Displaying calibration track records alongside quotes

## Crosswalk with Favorite-Longshot Literature

Consistent with Griffith (1949), Ali (1977), Thaler & Ziemba (1988), and Restocchi et al. (2018) — the favorite-longshot bias is the most robust finding in prediction market research across horse racing, sports betting, and now modern event contracts.
