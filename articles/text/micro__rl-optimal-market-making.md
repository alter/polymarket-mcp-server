---
title: "Optimal Market Making by Reinforcement Learning"
url: https://arxiv.org/abs/2104.04036
source: arxiv
date: "2021-04-08"
type: paper
theme: micro
lang: en
---

# Optimal Market Making by Reinforcement Learning

**Authors:** Matias Selser, Javier Kreiner, Manuel Maurette

**Submitted:** April 8, 2021

**arXiv:** 2104.04036

## Abstract

Reinforcement Learning applied to the classic quantitative finance Market Making problem. An agent provides liquidity by placing buy and sell orders while maximizing a utility function. The optimal agent finds a balance between price risk of inventory and profits from bid-ask spread capture.

## Key Findings

1. **Deep Q-Learning recovers the optimal (Avellaneda-Stoikov) agent** — DQN can learn optimal inventory management
2. **Reward function design** creates policy ordering equivalent to original utility function — careful reward engineering required
3. Agent learns to balance price risk vs. spread profit effectively
4. Benchmark comparison to symmetric (no inventory management) agent shows significant improvement

## Training Setup

- Simulated market environment with Brownian motion mid-price
- Poisson order arrivals (Avellaneda-Stoikov assumptions)
- Reward = terminal wealth minus inventory risk penalty
- DQN with experience replay and target network

## Relevance to Polymarket CLOB Trading

- **RL market maker for Polymarket:** Same framework applicable — state = (inventory, time-to-resolution, mid-price, spread, OBI)
- **Inventory management is key:** In binary markets, inventory at resolution is either +1 (profit) or 0 (loss) — not continuous
- **Custom reward design needed:** Binary settlement changes the P&L structure vs. continuous market
- **State space for Polymarket:** Add resolution probability momentum, time-to-resolution, market category
- A DQN agent trained on Polymarket simulator (with Neural Hawkes LOB from arXiv:2502.17417) could yield viable maker strategy

**Classification:** Machine Learning (cs.LG); Quantitative Finance (q-fin.TR)
