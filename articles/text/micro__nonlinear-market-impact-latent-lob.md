---
title: "A Fully Consistent, Minimal Model for Non-Linear Market Impact"
url: https://arxiv.org/abs/1412.0141
source: arxiv
date: "2014-11-29"
type: paper
theme: micro
lang: en
---

# A Fully Consistent, Minimal Model for Non-Linear Market Impact

**Authors:** Jonathan Donier, Julius Bonart, Iacopo Mastromatteo, Jean-Philippe Bouchaud

**Submitted:** November 29, 2014; Final revision March 1, 2015

**Published:** Quantitative Finance, Vol 15, No 7, 2015

**arXiv:** 1412.0141

## Abstract

A theory of non-linear price impact using a linear (latent) order book approximation, inspired by diffusion-reaction models. The framework computes average price trajectories during large trades and explains the universally observed square-root impact law.

## Key Findings

1. **Latent order book is locally linear** — consistent with reaction-diffusion models and general arguments
2. **Square-root impact law explained:** market impact scales as √(metaorder size) — universal empirical law
3. **Price decomposed into two components:**
   - Transient 'mechanical' impact — temporary, reverts after trade
   - Permanent 'informational' impact — lasting price shift
4. **Framework is free of price manipulation** — no round-trip trading profits
5. **Predicts price behavior when trading is interrupted or reversed**

## Latent Order Book Concept

The visible LOB is a tiny fraction of actual supply/demand:
- Latent liquidity is ~1000x larger than revealed liquidity
- Market makers intermediate between large latent imbalances
- The "reaction-diffusion" model captures how latent orders replenish the visible book

## Impact Laws

**Temporary impact:** I_temp ~ √(Q/V) × σ (Q=metaorder size, V=daily volume, σ=volatility)
**Permanent impact:** I_perm ≈ ⅔ × I_peak (relaxes to 2/3 of peak impact)

## Relevance to Polymarket CLOB Trading

- **Polymarket has very low visible liquidity** — latent order book concept particularly relevant
- **Square-root impact law calibration:** For large YES/NO position builds, expected slippage ~ √(size / daily volume)
- **Mechanical vs. informational decomposition:** When reversing a position, mechanical impact reverts (favorable) but informational impact is permanent
- **Optimal execution for Polymarket:** If building a large position, pace orders to allow latent book to replenish (reduce temporary impact)
- **Spread recovery after large trades:** Mid-price returns partially toward pre-trade level — watch for mean-reversion opportunity

**Classification:** Quantitative Finance (Trading and Market Microstructure)
