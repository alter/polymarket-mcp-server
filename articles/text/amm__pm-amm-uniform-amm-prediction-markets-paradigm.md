---
title: "pm-AMM: A Uniform AMM for Prediction Markets"
url: "https://www.paradigm.xyz/2024/11/pm-amm"
source: "paradigm.xyz"
date: "2024-11-05"
type: "blog"
theme: "amm"
lang: "en"
---

# pm-AMM: A Uniform AMM for Prediction Markets

**Authors:** Ciamac Moallemi, Dan Robinson

**Source:** Paradigm Research

## Core Innovation

The pm-AMM (prediction market AMM) is a specialized automated market maker designed for outcome tokens — assets that resolve to $1 if an event occurs and $0 otherwise. The research addresses: "what does it mean for an AMM to be optimized for a particular type of asset?"

## Gaussian Score Dynamics Model

The framework models prediction markets on whether an underlying random walk will exceed zero at expiration:
- Score process: dZ_t = sigma * dB_t
- Outcome token price: P_t = Phi(Z_t / (sigma * sqrt(T-t)))
- Price dynamics independent of volatility parameter sigma

## Static pm-AMM Invariant

Core invariant equation:

**(y - x) * Phi((y - x) / L) + L * phi((y - x) / L) - y = 0**

Where x, y = reserves of complementary outcome tokens; L = liquidity/scaling parameter; Phi = standard normal CDF; phi = standard normal PDF.

## Loss-vs-Rebalancing (LVR) Framework

Instantaneous LVR rate:

**LVR_t = -1/2 * [phi(Phi^-1(P_t))^2 / (T-t)] * V''(P_t) >= 0**

## Uniformity Property

A uniform AMM maintains LVR proportional to portfolio value: "LVR_t = alpha * V_t for some constant alpha > 0." The static pm-AMM achieves this property uniquely for Gaussian score dynamics — analogous to how geometric mean market makers (Uniswap, Balancer) are uniform for geometric Brownian motion assets.

## Dynamic pm-AMM Invariant

Time-dependent invariant to maintain constant expected LVR:

**(y - x) * Phi((y - x) / (L * sqrt(T-t))) + L * sqrt(T-t) * phi((y - x) / (L * sqrt(T-t))) - y = 0**

E[LVR_t] = V_0 / (2T) throughout the market lifetime.

## Key Findings

1. Expected pool value decay: static variant shows sqrt(remaining time) decay; dynamic shows linear decay
2. Liquidity concentration: pm-AMM concentrates liquidity near 50% probability
3. LPs lose approximately half initial capital to arbitrage by expiration under dynamic variant
4. Outperforms constant product market makers and LMSR in consistency across price points

## Significance

The first AMM specifically designed for prediction market outcome tokens using rigorous stochastic framework. Provides a generalizable methodology for deriving uniform AMMs for any asset class with a specified price process.
