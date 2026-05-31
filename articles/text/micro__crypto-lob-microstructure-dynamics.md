---
title: "Exploring Microstructural Dynamics in Cryptocurrency Limit Order Books: Better Inputs Matter More Than Stacking Another Hidden Layer"
url: https://arxiv.org/abs/2506.05764
source: arxiv
date: "2025-06-06"
type: paper
theme: micro
lang: en
---

# Exploring Microstructural Dynamics in Cryptocurrency Limit Order Books: Better Inputs Matter More Than Stacking Another Hidden Layer

**Author:** Haochuan Wang

**Submitted:** June 6, 2025; Revised June 9, 2025

**arXiv:** 2506.05764

## Abstract

Examines whether additional deep learning layers improve cryptocurrency price forecasting beyond what data preprocessing alone achieves. Evaluated approaches from logistic regression and XGBoost to neural architectures like DeepLOB, using Bitcoin/USDT LOB data. Tested two data filtering pipelines: Kalman filter and Savitzky-Golay filter.

## Key Findings

1. **Simpler models can match or exceed complex networks** — architectural complexity is not the bottleneck
2. **Feature engineering and preprocessing matter more** than model depth for this task
3. Logistic regression with well-preprocessed features ≈ DeepLOB performance
4. **Kalman filter preprocessing** outperforms Savitzky-Golay for noise reduction in LOB data
5. Both binary and ternary labeling schemes tested — ternary (up/flat/down) handles the flatness problem better

## Practical Implication

The central insight: **good microstructure features + simple model > raw LOB data + complex model**

Feature engineering steps that matter:
1. Mid-price noise filtering (Kalman > SG)
2. Label smoothing (ternary labels to handle sideways markets)
3. Relative price features (not absolute levels)
4. Multi-level OFI aggregation

## Relevance to Polymarket CLOB Trading

- **Feature engineering priority:** For Polymarket LOB forecasting, invest in feature engineering before model complexity
- **Kalman filter for Polymarket mid-price:** Apply Kalman filter to smooth noisy YES/NO mid-price signal
- **Ternary labeling:** For prediction market direction signals, add "stable" class (prices near 0.50 are often sideways)
- **Logistic regression baseline:** Start with logistic regression on good features — may be sufficient for Polymarket given limited data history
- **Avoid overfitting:** Polymarket markets are shorter-lived than equity LOBs — simpler models generalize better

**Classification:** Quantitative Finance; Machine Learning
