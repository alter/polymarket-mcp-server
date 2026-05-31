---
title: "From Glosten-Milgrom to the Whole Limit Order Book and Applications to Financial Regulation"
url: https://arxiv.org/abs/1902.10743
source: arxiv
date: "2019-02-27"
type: paper
theme: micro
lang: en
---

# From Glosten-Milgrom to the Whole Limit Order Book and Applications to Financial Regulation

**Authors:** Weibing Huang, Sergio Pulido, Mathieu Rosenbaum, Pamela Saliba, Emmanouil Sfendourakis

**Submitted:** February 27, 2019; Last revised March 29, 2025

**arXiv:** 1902.10743

## Abstract (verbatim)

"We build an agent-based model for the order book with three types of market participants: informed trader, noise trader and competitive market makers. Using a Glosten-Milgrom like approach, we are able to deduce the whole limit order book (bid-ask spread and volume available at each price) from the interactions between the different agents. More precisely, we obtain a link between efficient price dynamic, proportion of trades due to the noise trader, traded volume, bid-ask spread and equilibrium limit order book state."

## Key Findings

1. **Derives the full LOB shape** from Glosten-Milgrom agent interactions — not just the spread but volume at each price level
2. **Link between:** efficient price dynamics, noise trader proportion, traded volume, bid-ask spread, and equilibrium LOB state
3. **Regulatory application:** Forecasts consequences of tick size changes on microstructure
4. **Queue position valuation:** Enables quantitative valuation of queue position in the book
5. **Structural connection:** Noise trader proportion determines LOB depth at each price level

## Key Relationship (simplified)

LOB depth at price p ∝ probability of informed trade at price p × noise trader volume

The deeper the market at a price level, the more the noise traders dominate order flow at that level.

## Relevance to Polymarket CLOB Trading

- **Full LOB from GM:** Predicts where liquidity will sit across the YES/NO book given estimated informed trader fraction
- **Tick size change effects:** If Polymarket changes minimum tick size (e.g., 0.01 → 0.001), this model predicts the microstructure consequences
- **Queue position value:** Quantify the dollar value of being first vs. last in the queue at a given Polymarket price level
- **Noise trader identification:** High noise trader proportion → deeper book at mid → better execution for informed strategies

**Classification:** Quantitative Finance (Trading and Market Microstructure)
