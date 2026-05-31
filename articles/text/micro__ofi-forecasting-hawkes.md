---
title: "Forecasting High Frequency Order Flow Imbalance"
url: https://arxiv.org/abs/2408.03594
source: arxiv
date: "2024-08-07"
type: paper
theme: micro
lang: en
---

# Forecasting High Frequency Order Flow Imbalance

**Authors:** Aditya Nittur Anantha, Shashi Jain

**Submitted:** August 7, 2024

**arXiv:** 2408.03594

## Abstract

Market information events are generated intermittently and disseminated at high speeds. The paper addresses "asymmetric and possibly dependent" bid and offer event flows. Hawkes processes are applied to model OFI while capturing lagged dependencies between bid and offer sides.

## Key Findings

1. **Hawkes process with Sum of Exponentials kernel gives the best OFI forecast** among tested approaches
2. Methodology to forecast near-term OFI distributions (not just point estimates)
3. Technique for comparing an arbitrarily large number of forecasting models
4. Tested on National Stock Exchange tick data

## OFI Forecasting Framework

- OFI arrival rates are not independent — buy orders predict more buy orders (self-excitation)
- Cross-excitation: bid events excite ask events and vice versa
- Hawkes intensity: λ(t) = μ + ∫₀ᵗ φ(t-s) dN(s)
- Sum-of-Exponentials kernel captures multi-scale memory in order flow

## Relevance to Polymarket CLOB Trading

- Forecasting near-term OFI gives a directional signal on binary market price movements
- Hawkes process calibratable on Polymarket order-book event stream
- The 59% trade-direction accuracy on Polymarket (Dubach 2026) may partly reflect OFI measurement error — Hawkes-corrected OFI could improve inference
- Cross-excitation between YES and NO order flow particularly relevant for binary markets where YES buys = NO sells

**Classification:** Quantitative Finance - Trading and Market Microstructure (q-fin.TR)
