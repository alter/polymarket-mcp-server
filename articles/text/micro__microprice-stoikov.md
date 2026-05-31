---
title: "The Micro-Price: A High Frequency Estimator of Future Prices"
url: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2970694
source: ssrn
date: "2017-01-01"
type: paper
theme: micro
lang: en
---

# The Micro-Price: A High Frequency Estimator of Future Prices

**Author:** Sasha Stoikov

**Published:** SSRN, 2017/2018

**Slides:** https://www.ma.imperial.ac.uk/~ajacquie/Gatheral60/Slides/Gatheral60%20-%20Stoikov.pdf

**Extended paper:** https://arxiv.org/abs/2411.13594 — High resolution microprice estimates using hyperdimensional vector Tsetlin Machines

## Overview

The micro-price is a better estimator of short-term fair price than the mid-price or weighted mid-price. It is constructed as the limit of expected future mid-prices conditional on current LOB state.

## Three Competing Estimators

| Metric | Martingale? | Uses Imbalance? | Noise Level |
|--------|-------------|-----------------|-------------|
| Mid-Price | Approximately | No | Low but coarse |
| Weighted Mid-Price | No | Yes | High, counter-intuitive |
| Micro-Price | Yes (by construction) | Yes | Lower than WMP |

**Weighted Mid-Price problem:** If Pb=10.00, Qb=9, Pa=10.02, Qa=27 → WMP=10.005. If a sell order arrives at 10.01, WMP jumps UP to 10.009 — counter-intuitive since a sell should push price down.

## Micro-Price Construction

Micro-price = adjustment to mid-price using:
- Order book imbalance I = Qb/(Qb+Qa)
- Bid-ask spread s = Pa - Pb
- Recursive estimation from historical top-of-book state transitions

**micro-price = mid + f(I, s)**

The function f is estimated from historical data as the expected future mid-price change given current (I, s) state.

## Key Properties

1. **Martingale by construction** — the best unbiased predictor of future mid-price
2. **Less noisy than weighted mid-price** — filters out spurious imbalance signals
3. **Better predictor of short-term price moves** than mid or weighted mid
4. **Uses both spread and imbalance** — captures more LOB information than either alone

## Relevance to Polymarket CLOB Trading

- **Fair price estimation:** Use micro-price as fair value for YES/NO contracts instead of raw mid
- **Mid-price on Polymarket:** Can be 0.50 mid even when order book shows 80 YES bids vs 20 NO bids — micro-price corrects for this
- **Imbalance signal:** When I is high (more bid volume), micro-price > mid — adjust fair value up before quoting
- **Machine learning extension** (Blakely 2024, arXiv:2411.13594): Tsetlin Machine improves micro-price by incorporating higher-rank imbalance signals from multiple book levels
- **Application in Polymarket arena strategies:** Use micro-price as the reference for BB_fade and BO_fade signal generation
