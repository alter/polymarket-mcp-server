---
title: "Making and Evaluating Calibrated Forecasts"
url: https://arxiv.org/abs/2510.06388
source: arxiv
date: "2025-10-07"
type: paper
theme: forecast
lang: en
---

# Making and Evaluating Calibrated Forecasts (Truthful Calibration Errors for Multi-Class Prediction)

**Authors:** Yuxuan Lu, Yifan Wu, Jason Hartline, Lunjia Hu
**Submitted:** October 7, 2025; revised May 17, 2026
**arXiv:** 2510.06388
**License:** CC BY 4.0

## Abstract

Addresses calibration measurement in multi-class prediction systems. Many standard calibration errors lack truthfulness — predictors might appear better calibrated by distorting probabilities rather than reporting them accurately. Introduces perfectly truthful calibration errors for multi-class settings.

## Key Findings

1. **Truthfulness problem:** Many standard calibration measures are non-truthful — they incentivize reporting distorted rather than true probabilities to achieve lower calibration error.

2. **Perfectly truthful calibration errors:** Extended from binary (Haghtalab et al. [2024]) to multidimensional linear properties covering both full and classwise calibration.

3. **Decision-theoretic implications:** Truthful calibration errors preserve Blackwell dominance — more informative calibrated predictors receive no larger expected error.

4. **Binning robustness:** "Non-truthful confidence-based errors can reverse model rankings when the number of bins changes," while truthful errors provide more stable rankings across different binning choices.

## Relevance to Prediction-Market Pricing

Standard calibration metrics (ECE with fixed bins) may incentivize gaming — a forecaster tuned to look calibrated may not actually be. Truthful calibration measures provide guarantees that minimize actual miscalibration rather than its appearance, critical for reliable probability estimates in trading systems.
