---
title: "Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets"
url: https://arxiv.org/abs/2602.19520
source: arxiv
date: "2026-02-01"
type: paper
theme: sports
lang: en
---

# Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets

**Author:** Nam Anh Le  
**arXiv:** 2602.19520 | 2026 Preprint

## Data

- **292 million trades** across 327,000 binary contracts on Kalshi and Polymarket
- 64.7M Kalshi trades (210,608 contracts) + 227.6M Polymarket trades (116,000 resolved contracts)
- 6 domains, 9 time-to-resolution bins, 4 trade-size categories = 216 analysis cells

## Four-Component Calibration Decomposition

Four factors explain **87.3% of calibration variance** on Kalshi:

| Component | Variance Explained |
|---|---|
| Universal horizon effect | 30.2% |
| Domain-by-horizon interactions | 26.0% |
| Trade-size scale effect | 16.5% |
| Domain-specific intercepts | 14.6% |

## Key Empirical Patterns

### Universal Horizon Effect
- All domains show **underconfidence at long time horizons**
- Calibration slopes rise from 0.99 (within 1 hour) to 1.32 (beyond 1 month)
- Prices compress toward 50% — favorites systematically underpriced at long horizons

### Domain Intercepts (replicated on both Kalshi and Polymarket)
| Domain | Calibration Slope (reliable horizons) | Pattern |
|---|---|---|
| **Politics** | 1.31 | Persistent underconfidence |
| **Sports** | 1.08 | Near-perfect short-term, underconfident >1 month |
| **Crypto** | 1.05 | Near calibrated |
| **Weather** | <1.0 (short) | Initial overconfidence |

### Sports Markets Specifically
- Near-perfect calibration at **short-to-medium horizons** (slopes 0.90–1.10)
- Deteriorate significantly **beyond one month** (slope 1.74)
- "Continuous, quantifiable, publicly observable information enables accurate price discovery at short horizons"

### Trade-Size Effect (Kalshi only)
- Political markets: large trades have 1.74 slope vs 1.19 for single contracts (gap = 0.53)
- Does NOT replicate on Polymarket — suggests platform-specific microstructure

## Practical Implication for Prediction Market Traders

1. **Sports contracts within 1 week of resolution**: prices are approximately fair — small edge to be found
2. **Sports contracts >1 month out**: markets systematically underprice favorites — potential systematic edge
3. **Political contracts**: prices at 70 cents actually imply ~83% true probability — correction factor needed
4. **Weather short-horizon**: overconfident — fade extreme prices

## Statistical Validation

- Bayesian hierarchical model: **96.3% posterior predictive coverage**
- Maximum parameter discrepancy between frequentist/Bayesian: 0.005
- Core findings replicate cross-platform

## Conclusion

"The wisdom of crowds is real, but it has a structure." Prediction market consumers treating prices as face-value probabilities systematically misinterpret them. Direction and magnitude of error depends on domain, time horizon, and trade composition.
