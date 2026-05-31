---
title: "The Market Maker's Dilemma: Navigating the Fill Probability vs. Post-Fill Returns Trade-Off"
url: https://arxiv.org/abs/2502.18625
source: arxiv
date: "2025-02-25"
type: paper
theme: micro
lang: en
---

# The Market Maker's Dilemma: Navigating the Fill Probability vs. Post-Fill Returns Trade-Off

**Authors:** Jakob Albers, Mihai Cucuringu, Sam Howison, Alexander Y. Shestopaloff

**Submitted:** February 25, 2025; Revised November 23, 2025

**arXiv:** 2502.18625

## Abstract

Analysis of live Bitcoin perpetual trading data from Binance to examine order book mechanics and price persistence. Discovery of "a fundamental trade-off: a negative correlation between maker fill likelihood and post-fill returns."

## Key Findings

1. **Negative correlation between fill probability and post-fill returns** — orders most likely to fill are those adversely selected
2. **Queue positions and sizes significantly influence trading outcomes** — queue depth matters
3. **Profitable market-making requires contrarian positioning** against prevailing order book imbalance
4. **Commonly recommended strategies prove unprofitable** under real conditions with realistic fill modeling
5. **"Reversals" model:** contrarian strategies at the market touch (top-of-queue) can generate positive returns

## The Fill-Adverse-Selection Trade-Off

If imbalance is heavily skewed toward buys (positive OBI):
- Your ask limit order is more likely to fill (buyers are aggressive)
- BUT: after your ask fills, price will likely continue up → you're short at bad price
- The higher your fill probability, the worse your adverse selection

This is the **worst-kept secret of HFT**: queue priority is simultaneously an advantage and a curse.

## Contrarian Strategy Logic

- When OBI > threshold → prices likely to reverse → place bids (contrarian to imbalance)
- Contrarian to imbalance reduces fill probability but increases P&L per fill
- Need to balance fill rate (revenue) vs. adverse selection (losses)

## Relevance to Polymarket CLOB Trading

- **Direct application to Polymarket maker strategies:** High YES-buy imbalance → fills on ask likely but adverse
- **Contrarian alpha:** When order book imbalance is extreme, fading the direction is profitable after fill
- **Queue position matters:** Being early in the YES-sell queue when YES price is high is both more likely to fill AND more adversely selected
- **Practical approach:** Use OBI to adjust fair price (micro-price style), not just to determine direction
- This explains why simple OBI-following market making underperforms: you need to be contrarian on WHEN to post

**Classification:** Trading and Market Microstructure (q-fin.TR)
