---
title: "Dynamic Function Market Maker"
url: "https://arxiv.org/abs/2307.13624"
source: "arxiv"
date: "2023-07-25"
type: "paper"
theme: "amm"
lang: "en"
---

# Dynamic Function Market Maker

**Authors:** Arman Abgaryan, Utkarsh Sharma

**arXiv ID:** 2307.13624

## Abstract

We propose an adaptive and automated Dynamic Function Market Maker (DFMM) that addresses challenges in AMM design. The DFMM protocol includes a data aggregator and an order routing mechanism. It synchronizes price-sensitive market information, asserting the principle of one price, and ensuring market efficiency. The data aggregator includes a virtual order book, asserting efficient asset pricing by staying synchronized with information from external venues. The protocol's rebalancing and order routing method optimizes inventory risk through arbitrageurs. DFMM incorporates protective buffers with non-linear derivative instruments to manage risk and mitigate losses caused by market volatility.

## Key Contributions

- Automated protocol synchronizing external market data through virtual order books
- Risk optimization via arbitrageur incentives and rebalancing mechanisms
- Non-linear derivative buffers for volatility protection
- Unified algorithmic accounting system bridging segregated liquidity pools
- Protocol-driven settlement eliminating subjective risk assessments

## Relevance to Prediction Markets

DFMM's dynamic adaptation to external price signals is directly relevant to prediction markets where AMM prices should track real-world probability estimates. The arbitrageur-driven rebalancing mechanism is similar to how prediction market AMMs are expected to stay calibrated.
