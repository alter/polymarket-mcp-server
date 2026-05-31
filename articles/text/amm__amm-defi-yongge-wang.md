---
title: "Automated Market Makers for Decentralized Finance (DeFi)"
url: "https://arxiv.org/abs/2009.01676"
source: "arxiv"
date: "2020-09-03"
type: "paper"
theme: "amm"
lang: "en"
---

# Automated Market Makers for Decentralized Finance (DeFi)

**Author:** Yongge Wang

**arXiv ID:** 2009.01676 | Last revised May 18, 2024

## Abstract

This paper compares mathematical models for automated market makers including logarithmic market scoring rule (LMSR), liquidity sensitive LMSR (LS-LMSR), constant product/mean/sum, and others. It shows that LMSR may not be a good model for DeFi applications, while LS-LMSR has several advantages over constant product/mean based AMMs. However, LS-LMSR requires complicated computation (logarithm and exponentiation) and the cost function curve is concave, limiting DeFi utility.

## Key Contributions

- Comparative analysis of LMSR, LS-LMSR, constant product, constant mean, constant sum AMMs
- Shows LMSR's limitations for DeFi applications
- Proposes constant circle/ellipse-based cost functions that are computationally efficient (multiplication + square root only)
- Demonstrates advantages over constant product: increased robustness against front-running and slippage
- Convex cost curves that align with supply-and-demand principles

## Comparative Table

| AMM Type | Properties | DeFi Suitability |
|---|---|---|
| LMSR | Bounded loss, infinite liquidity | Poor — not designed for DeFi |
| LS-LMSR | Adaptive liquidity, bounded loss | Moderate — complex computation |
| Constant Product | Simple, gas-efficient | High — widely deployed |
| Constant Circle/Ellipse | Efficient, convex curves | Proposed as improvement |
