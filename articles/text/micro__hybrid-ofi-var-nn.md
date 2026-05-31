---
title: "Hybrid Vector Auto Regression and Neural Network Model for Order Flow Imbalance Prediction in High Frequency Trading"
url: https://arxiv.org/abs/2411.08382
source: arxiv
date: "2024-11-13"
type: paper
theme: micro
lang: en
---

# Hybrid Vector Auto Regression and Neural Network Model for Order Flow Imbalance Prediction in High Frequency Trading

**Authors:** Abdul Rahman, Neelesh Upadhye

**Submitted:** November 13, 2024

**arXiv:** 2411.08382

## Abstract (verbatim)

"In high frequency trading, accurate prediction of Order Flow Imbalance (OFI) is crucial for understanding market dynamics and maintaining liquidity. This paper introduces a hybrid predictive model that combines Vector Auto Regression (VAR) with a simple feedforward neural network (FNN) to forecast OFI and assess trading intensity."

## Key Findings

1. **Hybrid VAR + FNN outperforms standalone models** for OFI forecasting
2. **VAR captures linear dependencies** in multi-dimensional OFI time series
3. **FNN captures nonlinear residual patterns** — what VAR misses
4. Tested on both synthetic and real Binance data
5. Superior accuracy for OFI prediction in HFT environments

## Model Architecture

**Stage 1 — VAR:** Models linear interdependencies between:
- Bid OFI
- Ask OFI  
- Net OFI
- Price return

**Stage 2 — FNN on residuals:** Captures nonlinear patterns that VAR cannot explain

**Combined prediction:** VAR forecast + FNN correction = more accurate OFI estimate

## Relevance to Polymarket CLOB Trading

- **OFI forecasting for Polymarket:** VAR captures linear autocorrelation in YES/NO order flow; FNN captures event-driven nonlinearities
- **Multi-variate OFI:** Model YES-buy OFI, NO-buy OFI, and net OFI jointly — cross-prediction helps
- **Practical implementation:** VAR is fast to fit; FNN correction can be run in real-time
- **Signal generation:** 1-step ahead OFI forecast → directional signal for maker quote adjustment or taker order timing
- **Training data:** Use Polymarket CLOB feed historical data (30B+ events available via Dubach dataset)

**Classification:** Quantitative Finance (Trading and Market Microstructure); Machine Learning
