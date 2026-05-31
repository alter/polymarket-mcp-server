---
title: "Limit Order Book Dynamics in Matching Markets: Microstructure, Spread, and Execution Slippage"
url: https://arxiv.org/abs/2511.20606
source: arxiv
date: "2025-11-25"
type: paper
theme: micro
lang: en
---

# Limit Order Book Dynamics in Matching Markets: Microstructure, Spread, and Execution Slippage

**Author:** Yao Wu

**Submitted:** November 25, 2025

**arXiv:** 2511.20606

## Abstract

A market microstructure framework that models matching decisions as a limit order book system with rigid bid-ask spreads. Individual preferences are represented by a latent preference state matrix, where the spread between an agent's internal ask price and the market's best bid creates a structural liquidity constraint.

## Key Findings

1. **Threshold Impossibility Theorem:** Linear compensation cannot close preference gaps (spreads) unless it induces a categorical identity shift — structural limits to spread compression
2. **Dynamic discrete choice execution model:** Matches occur only when market-to-book ratio crosses a time-decaying liquidity threshold — analogous to order execution under inventory pressure
3. **Persistent execution slippage** is a structural feature of matching markets, not a temporary inefficiency
4. **Regional preference orderings invariant** across scenarios — preferences don't change just because compensation changes
5. 33 pages, 7 figures, 5 experiments — empirically validated

## Relevance to Polymarket CLOB Trading

- The "Threshold Impossibility Theorem" has an analog in prediction markets: you cannot always get good execution near a binary boundary (0.95+) regardless of how much you're willing to pay — the book may simply not offer it
- Time-decaying liquidity threshold: as resolution approaches, the threshold for match/execution rises — explaining why spreads widen near resolution
- Execution slippage model applicable for slippage estimation in position building/unwinding

**Classification:** Quantitative Finance (Trading and Market Microstructure)
