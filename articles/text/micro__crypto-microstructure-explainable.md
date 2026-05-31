---
title: "Explainable Patterns in Cryptocurrency Microstructure"
url: https://arxiv.org/abs/2602.00776
source: arxiv
date: "2026-01-31"
type: paper
theme: micro
lang: en
---

# Explainable Patterns in Cryptocurrency Microstructure

**Authors:** Bartosz Bieganowski, Robert Ślepaczuk

**Submitted:** January 31, 2026

**arXiv:** 2602.00776

## Abstract

The researchers document consistent patterns in cryptocurrency order book microstructure across multiple digital assets using Binance Futures data covering BTC, LTC, ETC, ENJ, ROSE from January 2022 through October 2025. CatBoost modeling with time-series validation applied.

## Key Findings

1. **Stable cross-asset patterns:** Feature rankings and predictive patterns remain stable across assets spanning an order of magnitude in market capitalization
2. **Portable microstructure representation** of short-horizon returns — features transfer across assets
3. Connected to classical microstructure theory: order flow imbalance, bid-ask spreads, adverse selection
4. **Flash crash analysis:** demonstrates how algorithmic trading amplifies systemic risks, empirically supporting classical microstructure theories
5. Both "taker" and "maker" backtests validated through trading strategy implementation

## Feature Library

Unified feature set covering:
- Spreads and relative prices
- Order flow imbalances (multi-level)
- Deviations of buy/sell VWAP from mid

## Relevance to Polymarket CLOB Trading

- Cross-asset portability: microstructure features transferable across Polymarket markets (different topic categories still share LOB mechanics)
- Flash crash pattern recognition applicable to Polymarket spread blowouts near resolution
- Taker vs. maker performance decomposition — which side of the spread is better on Polymarket
- CatBoost with LOB features: implementation pattern applicable to Polymarket feature engineering

**Classification:** Trading and Market Microstructure; Computational Finance; Statistical Finance
