---
title: "Event-Based Limit Order Book Simulation under a Neural Hawkes Process: Application in Market-Making"
url: https://arxiv.org/abs/2502.17417
source: arxiv
date: "2025-02-24"
type: paper
theme: micro
lang: en
---

# Event-Based Limit Order Book Simulation under a Neural Hawkes Process: Application in Market-Making

**Authors:** Luca Lalor, Anatoliy Swishchuk

**Submitted:** February 24, 2025

**arXiv:** 2502.17417

## Abstract

An event-driven LOB simulation model using Neural Hawkes processes (LSTM-based) to capture temporal dynamics between LOB event types. The framework generates realistic midprice movements and was tested within a Deep Reinforcement Learning market-making system.

## Key Findings

1. **Neural Hawkes Process** = LSTM-based Hawkes that captures long- and short-term interactions between event types
2. **Captures 12 commonly observed LOB events** (limit order add/cancel at each price level, market orders)
3. **Simulated fill distributions closely align with real data** — validated for market-making simulation
4. **Trade order fills closely resemble real-market trade execution** quality
5. Builds on prior work: Market Simulation under Adverse Selection (arXiv:2409.12721) by same authors

## LOB Event Types Modeled

The 12 event types modeled:
- Limit order additions at bid (levels 1-5)
- Limit order additions at ask (levels 1-5)
- Market buy orders
- Market sell orders

Neural Hawkes captures cross-excitation between these event types (e.g., market buy excites more limit sell additions).

## Relevance to Polymarket CLOB Trading

- **Simulation for strategy testing:** Neural Hawkes LOB simulator can be calibrated on Polymarket data
- **Backtesting realism:** Standard backtests overstate performance; this simulation accounts for fill probabilities and adverse fills
- **Market making bot training:** RL agent trained in this simulation achieves more realistic performance estimates
- **Event type mapping for Polymarket:** YES-buy limit order, NO-buy limit order, YES market order, NO market order — same framework applies

**Classification:** Computational Finance (q-fin.CP); Mathematical Finance (q-fin.MF)
