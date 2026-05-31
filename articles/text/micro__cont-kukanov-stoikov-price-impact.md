---
title: "The Price Impact of Order Book Events"
url: https://arxiv.org/abs/1011.6402
source: arxiv
date: "2010-11-29"
type: paper
theme: micro
lang: en
---

# The Price Impact of Order Book Events

**Authors:** Rama Cont, Arseniy Kukanov, Sasha Stoikov

**Submitted:** November 29, 2010 (final revision April 13, 2011)

**Published:** Journal of Financial Econometrics, Winter 2014, Volume 12, Issue 1, pages 47-88

**arXiv:** 1011.6402

**SSRN:** https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1712822

## Abstract

The authors study the price impact of order book events — limit orders, market orders, and cancellations — using NYSE Trades and Quotes (TAQ) data for fifty U.S. stocks. They show that, over short time intervals, price changes are mainly driven by the **order flow imbalance (OFI)**, defined as the imbalance between supply and demand at the best bid and ask prices.

## Key Findings

1. **Linear relationship between OFI and price changes**, with slope inversely proportional to market depth — R² ≈ 70%
2. **Robust across stocks and time scales**, including intraday seasonality effects
3. **Implies the square-root law:** the linear OFI model + scaling argument implies the empirically observed sqrt relation between price moves and volume
4. The OFI-price relationship is more reliable than volume-to-price correlations
5. **Application to adverse selection:** OFI can be used to measure adverse selection in limit order executions

## OFI Definition

OFI aggregates changes in the best bid and ask queues:
- When bid queue increases → positive contribution
- When ask queue decreases → positive contribution (implies buy pressure)
- Net imbalance over interval → OFI

## Relevance to Polymarket CLOB Trading

- **Directly applicable to Polymarket CLOB:** OFI can be computed from Polymarket's order book feed events
- Linear OFI → price impact model calibratable on Polymarket historical data
- Adverse selection measurement via OFI applicable to binary markets
- Caveat: Polymarket's 59% trade direction accuracy (Dubach 2026) means OFI computation needs on-chain ground truth for calibration

**Classification:** Quantitative Finance (Trading and Market Microstructure)
