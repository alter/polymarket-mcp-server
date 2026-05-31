---
title: "Continuous Auctions and Insider Trading (Kyle 1985)"
url: https://www.jstor.org/stable/1913210
source: journal
date: "1985-01-01"
type: paper
theme: micro
lang: en
---

# Continuous Auctions and Insider Trading

**Author:** Albert S. Kyle

**Published:** Econometrica, Vol. 53, No. 6, pp. 1315-1335, 1985

**Related arXiv:** https://arxiv.org/pdf/2006.09518 — Optimal Transport and Risk Aversion in Kyle's Model

## Overview

Kyle's model is the foundational framework for strategic informed trading and price discovery. It introduces Kyle's Lambda (λ) — the price impact coefficient — which remains the standard measure of market illiquidity.

## Core Setup

**Three agent types:**
1. **Informed trader** — knows true value ṽ ~ N(p₀, σ²_v); trades strategically to exploit information
2. **Noise traders** — trade u ~ N(0, σ²_u); provide cover for informed trader
3. **Market makers** — observe aggregate order flow y = x + u; set price p(y) = E[ṽ | y]

**Linear equilibrium:**
- Informed trader: x = β(ṽ - p₀)
- Market maker: p(y) = p₀ + λy
- **Kyle's Lambda: λ = ½ × √(σ²_v / σ²_u)**

## Key Results

1. **Kyle's Lambda = price impact coefficient** — dp/dy, how much price moves per unit of order flow
2. **Market depth 1/λ** — amount of order flow needed to move price by $1
3. **More noise trading → lower lambda → deeper market** — noise traders provide cover
4. **Information is revealed gradually** over the trading day; Lambda is constant (martingale property)
5. **Informed trader optimally distributes orders** to not reveal information too quickly

## Kyle's Lambda on Polymarket

From the Tsang & Yang (2026) study of Polymarket 2024 election markets:
- Lambda started at **0.53** early in the market (thin, illiquid)
- Dropped to **0.01** as market matured and approached resolution
- This trajectory confirms the Kyle model's prediction: liquidity improves as information is incorporated

## Relevance to Polymarket CLOB Trading

- **Lambda as execution cost estimate:** Larger lambda → higher cost to move the market → worse execution
- **Informed trader detection:** High lambda periods indicate information-driven order flow; widen spreads if making markets
- **Optimal execution:** Distribute orders to minimize lambda-driven price impact (Almgren-Chriss style)
- **Cross-market comparison:** Lambda varies by Polymarket category — political events vs. sports events will have different lambda
- **Estimation:** Regress midprice changes on net order flow over rolling windows to compute real-time lambda
