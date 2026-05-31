---
title: "High-frequency trading in a limit order book (Avellaneda-Stoikov Market Making Model)"
url: https://people.orie.cornell.edu/sfs33/LimitOrderBook.pdf
source: journal
date: "2008-01-01"
type: paper
theme: micro
lang: en
---

# High-Frequency Trading in a Limit Order Book (Avellaneda-Stoikov, 2008)

**Authors:** Marco Avellaneda, Sasha Stoikov

**Published:** Quantitative Finance, 2008

**PDF:** https://people.orie.cornell.edu/sfs33/LimitOrderBook.pdf

## Overview

The Avellaneda-Stoikov (A-S) model is the canonical framework for optimal market making. It treats market making as a stochastic control problem where the market maker maximizes expected utility of terminal wealth while penalizing inventory risk.

## Core Formulation

**Mid-price process:** dS = σ dW (arithmetic Brownian motion)

**Order arrival model:** Market orders arrive as Poisson processes with intensities:
- λ^a = A exp(-k δ^a) for the ask side
- λ^b = A exp(-k δ^b) for the bid side

where δ^a, δ^b are the distances from mid to quoted ask/bid.

**Reservation price (indifference price):**
r = s - q γ σ² (T - t)

where q = inventory, γ = risk aversion, σ = volatility, T-t = time remaining.

**Optimal spread:**
δ^a + δ^b = γ σ² (T - t) + (2/γ) ln(1 + γ/k)

## Key Insights

1. **Reservation price**: quotes centered on risk-adjusted mid, not raw mid — quotes skewed to reduce inventory
2. **Wider spread = higher inventory risk premium** — spread grows with volatility and risk aversion
3. **Inventory skew**: when long inventory, lower ask quote to attract sells; when short, raise bid
4. **Time decay**: spread narrows as T-t → 0 (less inventory risk near horizon)

## Extensions

- **GLFT (Guéant-Lehalle-Fernandez-Tapia):** Closed-form solution with inventory bounds, linear ODEs
- **RL extensions:** Deep Q-learning recovers optimal agent (Selser et al. 2021)
- **With OFI alpha:** AS + order flow imbalance signal for fair price adjustment (hftbacktest approach)
- **Flow toxicity extension:** Busca (2024) — what happens when counterparty has forecast alpha

## Relevance to Polymarket CLOB Trading

- Directly applicable: Polymarket CLOB has bid/ask queues, Poisson-like order arrivals, inventory risk
- Reservation price skewing applies for YES/NO binary positions
- Key difference: binary settlement (0 or 1) changes the terminal inventory cost — needs modification
- Spread formula calibratable using Polymarket historical spread and fill data
