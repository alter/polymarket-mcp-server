---
title: "Risk-Neutral Pricing and Hedging of In-Play Football Bets"
url: https://arxiv.org/abs/1811.03931
source: arxiv
date: "2018-11-01"
type: paper
theme: sports
lang: en
---

# Risk-Neutral Pricing and Hedging of In-Play Football Bets

**arXiv:** 1811.03931

## Abstract

In-play football bets are traded live during a football game. Prices are driven by goals scored such that:
- Prices **move smoothly between goals**
- Prices **jump to new level when goals are scored**

This mirrors financial markets where option price changes follow underlying instrument price changes.

## Core Framework

Applies the **Fundamental Theorems of Asset Pricing** to the in-play football betting market. In-play bets can be priced in the **risk-neutral framework** — the same mathematical foundation used for options pricing.

## Goal Process Models Referenced

1. **Maher (1982)**: Independent Poisson distribution for football scores
2. **Dixon and Coles (1997)**: Correlated Poisson, time-weighted
3. **Jottreau (2009)**: Stochastic intensity model — goals driven by Poisson processes with **Cox-Ingersoll-Ross (CIR)** stochastic intensities

## Key Insight: Football Bets as Derivatives

An in-play match winner bet functions as a **binary option** on the final score process:
- The underlying = goal scoring process (stochastic intensity)
- The contract = pays $1 if team wins, $0 otherwise
- Pricing = risk-neutral expectation of future goal processes

This makes all options pricing theory directly applicable: Greeks (delta, gamma), hedging strategies, dynamic replication.

## Transferable Insights

1. In-play sports contracts ARE financial derivatives — price them accordingly
2. CIR process is appropriate for modeling time-varying goal/score intensities
3. Dynamic hedging of in-play positions is theoretically possible
4. Between-goal smooth price drift = predictable, tradeable with the Weibull model
5. Goal-time jumps = the main source of risk to manage
