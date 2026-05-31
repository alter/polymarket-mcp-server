---
title: "ClusterLOB: Enhancing Trading Strategies by Clustering Orders in Limit Order Books"
url: https://arxiv.org/abs/2504.20349
source: arxiv
date: "2025-04-29"
type: paper
theme: micro
lang: en
---

# ClusterLOB: Enhancing Trading Strategies by Clustering Orders in Limit Order Books

**Authors:** Yichi Zhang, Mihai Cucuringu, Alexander Y. Shestopaloff, Stefan Zohren

**Submitted:** April 29, 2025; Revised May 10, 2025

**arXiv:** 2504.20349

## Abstract

A method to segment market-by-order (MBO) data into groups using K-means++ clustering. Each market event is augmented with six time-dependent features. Orders are assigned to clusters representing distinct participant behavior types.

## Key Findings

1. **Three distinct participant types identified** via clustering: directional, opportunistic, and market-making participants
2. Processes one year of NASDAQ data across small, medium, and large-tick stocks
3. **Cluster-derived strategies outperform benchmarks** without clustering — superior Sharpe and returns
4. OFI decomposed by cluster type is more predictive than aggregate OFI
5. Framework works across add, cancel, and trade events

## Three Participant Clusters (Inferred)

- **Directional traders:** Large orders, hold direction, price impact
- **Opportunistic traders:** React to imbalance, short-term, mean-revert
- **Market makers:** Two-sided, small size, spread capture

## Feature Engineering (6 per event)

Time-dependent features per order event include:
- Order size relative to queue
- Time since last same-side event
- Price distance from mid
- Volume imbalance at level
- Order cancellation rate
- Trade-to-order ratio

## Relevance to Polymarket CLOB Trading

- **Polymarket participant segmentation:** Apply K-means to Polymarket order events to identify which orders are from market makers, informed traders, noise traders
- **OFI by participant type:** Informed-trader OFI is more predictive than aggregate OFI
- **Strategy construction:** Target directional cluster's order flow as alpha signal; market-maker cluster's flow as noise
- **MBO data on Polymarket:** Full order book event stream available via CLOB API — L3-equivalent data available

**Classification:** Trading and Market Microstructure; Machine Learning
