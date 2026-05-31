---
title: "A Note on Almgren-Chriss Optimal Execution Problem with Geometric Brownian Motion"
url: https://arxiv.org/abs/2006.11426
source: arxiv
date: "2020-06-19"
type: paper
theme: micro
lang: en
---

# A Note on Almgren-Chriss Optimal Execution Problem with Geometric Brownian Motion

**Authors:** Bastien Baldacci, Jerome Benveniste

**Submitted:** June 19, 2020; Revised June 23, 2020

**arXiv:** 2006.11426

## Abstract

Explicitly solves the Almgren-Chriss optimal liquidation problem where the stock price process follows a geometric Brownian motion (GBM) rather than arithmetic BM. Uses functional analysis techniques on cash variables. Extends to stochastic drift and portfolio liquidation.

## Key Findings

1. **Explicit solution** for optimal execution under GBM price dynamics
2. **Functional analysis framework** using cash variable transformation
3. Extends to **stochastic drift** in price processes
4. Applicable to **multi-asset portfolio liquidation** scenarios
5. More realistic than arithmetic BM since GBM prevents negative prices

## Why GBM Matters for Prediction Markets

Binary prediction market prices (0 to 1) follow dynamics closer to GBM (bounded, multiplicative) than arithmetic BM (unbounded, additive). Prices near 0 or 1 have lower volatility (in arithmetic terms) even though probability risk remains high.

The Baldacci-Benveniste solution provides an explicit framework for execution under GBM-like dynamics — more appropriate for Polymarket YES/NO price processes.

## Relevance to Polymarket CLOB Trading

- **GBM price model for Polymarket:** YES price process is bounded [0,1] and multiplicative near boundaries — GBM more appropriate than arithmetic BM
- **Optimal execution formula:** Explicit solution allows computing optimal order schedule without numerical ODE solving
- **Stochastic drift extension:** Polymarket prices have stochastic drift (news flow) — this extension handles that
- **Portfolio context:** When liquidating a portfolio of correlated Polymarket positions (e.g., election candidates), the multi-asset solution is directly applicable

**Classification:** Quantitative Finance (Trading and Market Microstructure; Computational Finance)
