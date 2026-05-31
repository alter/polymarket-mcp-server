---
title: "High Resolution Microprice Estimates from Limit Orderbook Data using Hyperdimensional Vector Tsetlin Machines"
url: https://arxiv.org/abs/2411.13594
source: arxiv
date: "2024-11-18"
type: paper
theme: micro
lang: en
---

# High Resolution Microprice Estimates from Limit Orderbook Data using Hyperdimensional Vector Tsetlin Machines

**Author:** Christian D. Blakely

**Submitted:** November 18, 2024

**arXiv:** 2411.13594

## Abstract

Proposes an error-correcting model for the microprice — a high-frequency estimator of future prices — that incorporates orderbook imbalance data from multiple price levels. The approach adjusts baseline microprice estimates by analyzing "recent dynamics of higher price rank imbalances" and uses a computationally fast estimator via hyperdimensional vector Tsetlin machine framework.

## Key Findings

1. **Multi-level imbalance improves microprice** — higher-rank (deeper book) imbalances add predictive signal beyond top-of-book
2. **Tsetlin Machine** provides computationally fast estimation — suitable for real-time HFT use
3. **Error-correcting framework:** Start with Stoikov microprice, then correct errors using higher-level book data
4. **Robust future price predictions** from higher-order orderbook information

## Tsetlin Machine (Brief Explanation)

A Tsetlin Machine is a rule-based ML model using automata:
- Very fast inference (microsecond scale)
- Interpretable rules
- Competitive accuracy vs. neural networks for binary classification
- **Hyperdimensional version:** Uses vector encoding for richer representation

## The Multi-Level Microprice

Standard Stoikov microprice uses only top-of-book (level 1):
- I₁ = Q_bid_1 / (Q_bid_1 + Q_ask_1)

Extended version incorporates levels 2-N:
- I_k = Q_bid_k / (Q_bid_k + Q_ask_k) for k=1...N
- Tsetlin machine learns correction function f(I₁, I₂, ..., I_N)

## Relevance to Polymarket CLOB Trading

- **Multi-level OBI for Polymarket:** Use YES/NO quantities at multiple price levels for a richer fair-value estimate
- **Fast inference requirement:** For sub-second Polymarket quote updates, Tsetlin Machine's speed advantage matters
- **Error correction framework:** Apply to existing Polymarket mid-price or OBI signal to reduce estimation error
- **Interpretability:** Tsetlin rules can be examined to understand which book levels drive the correction — useful for model debugging

**Classification:** Trading and Market Microstructure; Machine Learning
