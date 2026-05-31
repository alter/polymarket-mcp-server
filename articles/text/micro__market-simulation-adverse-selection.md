---
title: "Market Simulation under Adverse Selection"
url: https://arxiv.org/abs/2409.12721
source: arxiv
date: "2024-09-19"
type: paper
theme: micro
lang: en
---

# Market Simulation under Adverse Selection

**Authors:** Luca Lalor, Anatoliy Swishchuk

**Submitted:** September 19, 2024; Last revised March 31, 2025

**arXiv:** 2409.12721

## Abstract

Examines how fill probabilities and adverse fills impact trading strategy simulations. Focuses on a stochastic optimal control market-making problem. Tested on four major CME futures contracts: ES (E-mini S&P 500), NQ (E-mini Nasdaq 100), CL (Crude Oil), ZN (10-Year Treasury Note).

## Key Findings

1. **Fill probabilities and adverse fills can significantly affect performance** — simulations ignoring these overstate performance
2. **Critical gap in prior research:** Previous studies simulated price processes and market orders independently — overstates strategy profitability
3. **Adverse fills** = orders filled at disadvantageous prices ("picked off") — immediately out-of-the-money post-fill
4. **Improved simulation framework** yields performance estimates closer to real-world conditions
5. Practical tool for evaluating market-making strategies pre-deployment

## Adverse Fill Definition

An adverse fill occurs when:
1. A limit order fills (maker gets passive execution)
2. Immediately after filling, the position is out-of-the-money
3. This is because an informed counterparty "picked off" the order

In Avellaneda-Stoikov terms: your limit order fills because an informed trader (with better information) is hitting it at an unfavorable time.

## Relevance to Polymarket CLOB Trading

- **Polymarket backtesting:** Standard backtest ignoring fill probabilities overstates maker P&L by a significant margin
- **Adverse fills on Polymarket:** Particularly severe near news events or resolution announcements — informed traders pick off stale quotes
- **Simulation framework:** Build Polymarket-specific simulation with realistic fill model before deploying maker strategies
- **CME futures analog:** ES/NQ futures are liquid, similar to high-volume Polymarket markets — calibrate simulation parameters accordingly
- **The key insight:** You need both fill probability AND adverse fill probability to get realistic P&L estimates

**Classification:** Quantitative Finance (Trading and Market Microstructure; Computational Finance)
