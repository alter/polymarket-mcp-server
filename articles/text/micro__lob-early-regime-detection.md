---
title: "Early Detection of Latent Microstructure Regimes in Limit Order Books"
url: https://arxiv.org/abs/2604.20949
source: arxiv
date: "2026-04-22"
type: paper
theme: micro
lang: en
---

# Early Detection of Latent Microstructure Regimes in Limit Order Books

**Authors:** Prakul Sunil Hiremath, Vruksha Arun Hiremath

**Submitted:** April 22, 2026

**arXiv:** 2604.20949

## Abstract

Limit order books can transition rapidly from stable to stressed conditions. Standard early-warning signals such as order flow imbalance and short-term volatility are inherently reactive.

The authors propose a three-phase causal model of microstructure regime evolution and develop a trigger-based detection method using MAX aggregation of complementary signal channels with adaptive thresholding.

## Key Findings

1. **Mean lead-time +18.6 ± 3.2 timesteps** before regime transition, with perfect precision across 200 simulations
2. **Outperforms classical change-point and microstructure baselines** (OFI alone, volatility alone)
3. **Performance degrades in low signal-to-noise and short build-up regimes** — aligned with theoretical predictions
4. **Validated on Bitcoin/USDT order book data** (preliminary)
5. Theoretical guarantees for identifiability of latent build-up regime derived

## Three-Phase Causal Model

The model describes how LOBs transition:
1. **Latent build-up phase** — imbalance builds in hidden liquidity without visible price impact
2. **Cascade phase** — visible deterioration in spread and depth
3. **Stressed phase** — dramatic spread widening, flash crash risk

## Relevance to Polymarket CLOB Trading

- Regime detection directly applicable to Polymarket markets: spreads can widen abruptly near resolution events or news shocks
- Lead-time of 18+ timesteps is actionable for adjusting quotes or taking positions
- MAX aggregation of OFI + spread + depth provides more stable signal than OFI alone
- Particularly valuable for predicting spread blowouts in longshot markets (where the longshot spread premium exists)

**Classification:** Quantitative Finance (Trading and Market Microstructure)
