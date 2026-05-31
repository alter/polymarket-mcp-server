---
title: "Bid, Ask and Transaction Prices in a Specialist Market with Heterogeneously Informed Traders (Glosten-Milgrom 1985)"
url: https://www.sciencedirect.com/science/article/pii/0304405X85900443
source: journal
date: "1985-01-01"
type: paper
theme: micro
lang: en
---

# Bid, Ask and Transaction Prices in a Specialist Market with Heterogeneously Informed Traders

**Authors:** Lawrence R. Glosten, Paul R. Milgrom

**Published:** Journal of Financial Economics, 14(1), 71-100, 1985

**Extension (arXiv):** https://arxiv.org/pdf/1902.10743 — "From Glosten-Milgrom to the whole limit order book"

## Overview

The Glosten-Milgrom (GM) model is the canonical framework for adverse selection in market making. It explains how bid-ask spreads arise from information asymmetry between informed and uninformed traders.

## Core Mechanics

**Three trader types:**
1. Informed traders — know true asset value v*
2. Noise traders — trade for liquidity, no private information
3. Market maker (specialist) — sets bid/ask to break even in expectation

**Equilibrium:**
- Bid = E[v | sell order arrives] — lower because sells signal bad news
- Ask = E[v | buy order arrives] — higher because buys signal good news
- Spread = Ask - Bid > 0 even with risk-neutral, competitive market maker

**Bayesian learning:**
The market maker updates beliefs about v* after each trade via Bayes' rule. Transaction prices form a martingale.

## Key Results

1. **Adverse selection creates positive spread** even with zero inventory cost and perfect competition
2. **Spread proportional to informed trader fraction:** more informed traders → wider spread
3. **Serial correlation in transaction prices** — function of adverse selection component of spread
4. **Spread decomposition:** adverse selection component + processing costs
5. **Bid-ask bounce:** observed returns diverge from realizable returns by the spread

## Extensions

- **From GM to full LOB** (Huang et al. 2019, arXiv:1902.10743): derives whole order book shape from GM-style agent interactions; allows quantifying queue position value
- **Privacy-noise extension** (Nakamura 2026, arXiv:2605.19742): closed-form GM with noisy direction observation — directly applicable to MPC-based matching or situations where trade direction is uncertain
- **Learning market maker** in GM model — Bayesian updating strategies

## Relevance to Polymarket CLOB Trading

- Adverse selection framework directly applicable: informed traders (with better probability estimates) impose costs on market makers
- Binary market version: informed trader knows P(YES) = 0.85 while market is at 0.70 → buys YES, adverse-selects maker
- GM equilibrium spread calibratable for Polymarket: estimate informed fraction μ from order flow imbalance
- Resolution creates an "end-of-game" informed trader problem — spreads widen as resolution approaches because adverse selection risk spikes
- The Bartlett (2026) Kalshi study implements adapted GM decomposition on binary prediction market data
