---
title: "Deep Learning for VWAP Execution in Crypto Markets: Beyond the Volume Curve"
url: https://arxiv.org/abs/2502.13722
source: arxiv
date: "2025-02-19"
type: paper
theme: micro
lang: en
---

# Deep Learning for VWAP Execution in Crypto Markets: Beyond the Volume Curve

**Author:** Remi Genet

**Submitted:** February 19, 2025; Revised April 2, 2025

**arXiv:** 2502.13722

## Abstract

A deep learning framework that directly optimizes the VWAP execution objective by bypassing the intermediate step of volume curve prediction. Uses automatic differentiation and custom loss functions to calibrate order allocation and minimize VWAP slippage.

## Key Findings

1. **Direct VWAP optimization outperforms volume-curve-based methods** — even a naive linear model achieves lower slippage when directly optimizing
2. **Strategies optimized for VWAP diverge from accurate volume curve predictions** — the intermediate step is a confound
3. **Particularly valuable in volatile markets** like crypto where volume curve prediction errors are large
4. **Transferable to equities** — principles not limited to crypto
5. Uses automatic differentiation via PyTorch — practical implementation path

## VWAP Execution Context

**VWAP = Volume Weighted Average Price** — the benchmark for institutional execution
- Traditional approach: forecast volume curve → trade proportional to volume
- New approach: directly minimize E[execution price - VWAP] via DL with differentiable execution simulator

**Why VWAP matters for Polymarket:**
- For large positions, target VWAP-like execution to minimize slippage
- In thin Polymarket markets, volume curves are highly non-stationary

## Relevance to Polymarket CLOB Trading

- **Position building:** When taking large YES/NO position, use direct optimization to minimize execution slippage vs. naive uniform slicing
- **Crypto context directly applicable:** Polymarket CLOB has similar liquidity profile to crypto futures
- **Practical implementation:** Auto-diff framework in PyTorch for Polymarket order schedule optimization
- **Benchmark alternative:** Use TWAP as baseline; direct optimization should meaningfully outperform

**Classification:** Statistical Finance (q-fin.ST); Machine Learning (cs.LG)
