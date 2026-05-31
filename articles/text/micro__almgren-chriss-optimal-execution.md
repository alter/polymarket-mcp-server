---
title: "Optimal Execution of Portfolio Transactions (Almgren-Chriss Model)"
url: https://www.smallake.kr/wp-content/uploads/2016/03/optliq.pdf
source: journal
date: "2001-01-01"
type: paper
theme: micro
lang: en
---

# Optimal Execution of Portfolio Transactions

**Authors:** Robert Almgren, Neil Chriss

**Published:** Journal of Risk, Vol. 3, pp. 5-40, 2001

**PDF:** https://www.smallake.kr/wp-content/uploads/2016/03/optliq.pdf

## Overview

The Almgren-Chriss model is the foundational framework for optimal trade execution. It minimizes a combination of volatility risk and transaction costs arising from permanent and temporary market impact.

## Model Components

**Temporary price impact:** Execution cost per share ~ η × (shares/time), reverts after order
**Permanent price impact:** Lasting price shift ~ γ × total shares traded, reflects information

**Objective:** Minimize E[Cost] + λ × Var[Cost]

This yields an **efficient frontier** in the space of time-dependent liquidation strategies — exactly analogous to the Markowitz mean-variance frontier.

## Key Results

1. **Optimal trading trajectory:** For linear impact, the optimal strategy is to trade exponentially decelerating quantities (front-loaded to avoid volatility risk)
2. **TWAP = zero risk aversion:** When λ=0, optimal strategy is equal-time-slice (TWAP)
3. **Liquidity-adjusted VaR (L-VaR):** Extends VaR to incorporate liquidation costs
4. **Risk-speed trade-off:** Risk-averse traders front-load; liquidity-conscious traders back-load

## Relevance to Polymarket CLOB Trading

- Directly applicable when building or unwinding a large YES/NO position across multiple orders
- Permanent vs temporary impact decomposition: in prediction markets, most impact is temporary (thin book, mean-reverting)
- L-VaR concept: for sizing positions, need to account for the cost of exiting before resolution
- Time-varying liquidity extension (arxiv:2410.04867): applicable since Polymarket spreads widen near resolution

**Key extensions:**
- arXiv:2006.11426 — geometric Brownian motion version (explicit solution)
- arXiv:2410.04867 — time-varying liquidity (well-posedness + manipulation prevention)
- arXiv:2411.06389 — RL agent outperforms TWAP/VWAP benchmarks
