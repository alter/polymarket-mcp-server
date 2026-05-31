---
title: "Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets"
url: https://arxiv.org/abs/2602.19520
source: arxiv
date: "2026-02-23"
type: paper
theme: forecast
lang: en
---

# Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets

**Author:** Nam Anh Le
**Submitted:** February 23, 2026
**arXiv:** 2602.19520 [stat.AP]

## Abstract

The paper analyzes 292 million trades across 327,000 binary contracts on prediction market platforms Kalshi and Polymarket. Key finding: "calibration decomposes into four components (a universal horizon effect, domain-specific biases, domain-by-horizon interactions and a trade-size scale effect) that together explain 87.3% of calibration variance."

## Introduction

Prediction markets are celebrated for their ability to aggregate dispersed information into well-calibrated probability estimates. However, the question of whether calibration is uniform across domains and time horizons has received limited empirical attention. This paper fills that gap using the largest cross-platform dataset yet assembled.

## Main Findings

- **Universal horizon effect:** All markets exhibit underconfidence at long horizons, with prices compressing toward 50%; this bias dissipates as resolution approaches.
- **Domain-specific biases:** Political markets show persistent underconfidence across both Kalshi and Polymarket; financial/crypto markets are closer to calibrated.
- **Domain-by-horizon interactions:** The horizon effect is strongest in political markets and weakest in sports.
- **Trade-size scale effect:** On Kalshi, large trades correlate with amplified underconfidence in political markets; this does not replicate on Polymarket, suggesting platform-specific microstructure differences.
- **Bayesian validation:** Bayesian hierarchical modeling confirms frequentist findings with 96.3% posterior predictive coverage.

## Methodology

- Dataset: 292 million individual trades; 327,000 binary contracts; two major platforms (Kalshi, Polymarket)
- Calibration measured via reliability diagrams, ECE, and Brier decomposition
- Bayesian hierarchical models used for posterior validation
- Horizon bucketed into: >30 days, 7–30 days, 1–7 days, <1 day

## Conclusion

"Consumers of prediction market prices who treat them as face-value probabilities will systematically misinterpret them, with the direction of misinterpretation depending on what is being predicted, when, and by whom." The decomposition framework provides a principled basis for adjusting raw market prices before use as probability inputs in trading systems.

## Relevance to Prediction-Market Pricing

Directly applicable to Polymarket trading: raw prices should be adjusted by domain and horizon. Political markets at long horizons systematically understate the probability of favorites and overstate the probability of longshots.
