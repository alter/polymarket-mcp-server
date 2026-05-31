---
title: "Understanding the Worst-Kept Secret of High-Frequency Trading (Volume Imbalance)"
url: https://arxiv.org/abs/2307.15599
source: arxiv
date: "2023-07-28"
type: paper
theme: micro
lang: en
---

# Understanding the Worst-Kept Secret of High-Frequency Trading

**Authors:** Sergio Pulido, Mathieu Rosenbaum, Emmanouil Sfendourakis

**Submitted:** July 28, 2023; Revised July 23, 2024

**arXiv:** 2307.15599

## Abstract (verbatim excerpt)

"Volume imbalance in a limit order book is often considered as a reliable indicator for predicting future price moves."

The researchers analyze how volume imbalance relates to price movements through a market-making optimization model. When mid-price is about to jump up, OBI → 1; when about to jump down, OBI → -1.

## Key Findings

1. **It is optimal to quote a predictive imbalance** — market makers endogenously create the imbalance-price signal as part of their optimal strategy
2. **Mathematical framework:** Coupled system of PDEs, with existence and uniqueness of classical solutions proven
3. **Volume imbalance patterns emerge as endogenous market-maker optimization** — not an independent market signal, but a structural artifact
4. **Regulatory tool:** Results help regulators select appropriate tick sizes
5. **Worst-kept secret:** The fact that imbalance predicts price moves is widely known but poorly understood theoretically — this paper provides the theoretical foundation

## The Endogeneity Result

This is a crucial insight: **OBI predicts price moves BECAUSE market makers optimally quote to create imbalance before price jumps.** It is not an independent signal — it is the optimal behavior.

Implication for strategy:
- OBI is a leading indicator of mid-price changes
- But it is partly self-fulfilling (market makers' optimal response)
- In prediction markets where fewer market makers are present, OBI may be less endogenous and more genuinely informative

## Relevance to Polymarket CLOB Trading

- Validates OBI as price predictor — but warns about the endogeneity
- In Polymarket (fewer systematic market makers than equity markets), OBI may be more informative because it's less "manufactured"
- **Tick size regulation:** If Polymarket changes tick size, OBI-price relationship changes — important for strategy recalibration
- **Contrarian vs. momentum:** Since imbalance is partly self-fulfilling, following it may work only in the short run before mean reversion

**Classification:** Trading and Market Microstructure; Optimization and Control
