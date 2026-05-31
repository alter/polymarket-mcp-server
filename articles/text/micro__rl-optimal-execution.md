---
title: "Optimal Execution with Reinforcement Learning"
url: https://arxiv.org/abs/2411.06389
source: arxiv
date: "2024-11-10"
type: paper
theme: micro
lang: en
---

# Optimal Execution with Reinforcement Learning

**Authors:** Yadh Hafsi, Edoardo Vittori

**Submitted:** November 10, 2024; Revised November 1, 2025

**arXiv:** 2411.06389

## Abstract

Investigates developing an optimal execution strategy using RL for traders buying and selling inventory within a set timeframe. Employs the ABIDES multi-agent market simulator with LOB features. The RL agent outperforms standard strategies (TWAP, VWAP).

## Key Findings

1. **RL agent outperforms TWAP and VWAP** in execution quality
2. **High-frequency operation** using LOB state data enables enhanced control
3. **ABIDES simulator** provides diverse LOB depth levels for robust training environment
4. Custom MDP formulation for execution within finite time horizon
5. Practical foundation for real-world trading applications demonstrated

## TWAP as Baseline Interpretation

In Almgren-Chriss terms, TWAP corresponds to zero risk aversion — the trader is indifferent to price volatility and only minimizes market impact by trading evenly. VWAP is volume-adaptive TWAP. RL goes beyond both by learning state-dependent strategies.

## MDP Formulation

- **State:** Remaining inventory, time remaining, current LOB snapshot, recent price history
- **Action:** Order size and placement (limit vs. market, price level)
- **Reward:** Implementation shortfall improvement vs. TWAP benchmark
- **Transition:** ABIDES simulator with realistic LOB dynamics

## Relevance to Polymarket CLOB Trading

- **Position building/unwinding on Polymarket:** When building large YES/NO position, RL agent can optimize execution schedule
- **Finite horizon:** Polymarket markets have resolution date — natural finite horizon for execution
- **LOB state features for Polymarket:** Use YES-buy/sell imbalance, spread, depth at 5 levels, time-to-resolution
- **ABIDES calibration:** Train ABIDES-style simulator on Polymarket historical data before deploying RL agent
- **Benchmark:** Compare RL execution to naive market order or TWAP — quantify improvement

**Classification:** Trading and Market Microstructure (q-fin.TR); Machine Learning (cs.LG)
