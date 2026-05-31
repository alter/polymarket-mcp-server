---
title: "The Anatomy of a Decentralized Prediction Market: Microstructure Evidence from the Polymarket Order Book"
url: https://arxiv.org/abs/2604.24366
source: arxiv.org
date: "2026-04-30"
type: paper
theme: strategies
lang: en
---

# The Anatomy of a Decentralized Prediction Market: Microstructure Evidence from the Polymarket Order Book

## Overview

Researchers studied the microstructure of Polymarket — the largest on-chain prediction market — using a continuous tick-level archive of the public order-book feed covering **30 billion events over 52 days**, joined to the authoritative on-chain trade record.

Pre-registered stratified panel of **600 markets**.

## Eight Stylized Facts

1. **Longshot spread premium**: Bid-ask spreads are elevated for low-probability contracts
2. **Depth profile closer to uniform than top-of-book**: Liquidity distribution is more uniform than concentrated at best prices
3. **Null block-clock alignment effect**: No significant timing relationship between blockchain timestamps and order clustering
4. **Broad maker-wallet diversity with concentrated tail**: Wide participation with power-law concentration
5. **Category-conditional effective-spread differences**: Effective spreads vary significantly by market category
6. **Sub-50ms median archive-ingestion delay with multi-second tail**: Technical latency characteristics
7. **Self-counterparty wash share with median 1% and 22% upper tail**: Wash trading is much lower than unregulated crypto venues
8. **Cross-sectional depth profile explained by market duration, price level, and volume**: No residual time-to-close effect

## Critical Measurement Finding

**Trade direction inferred from Polymarket's public order-book feed agrees with on-chain ground truth on only ~59% of buckets** (panel mean 0.615, 95% CI [0.58, 0.65]).

This is well below the ~80% Lee-Ready accuracy on Nasdaq. Orderbook events alone do not identify the trade-aggressor sign reliably — feed-inferred sign matches the on-chain record on only ~59% of comparable buckets, just above the 50% chance baseline.

## Trading Implications

Researchers relying on Polymarket microstructure analysis must source trade direction from on-chain events rather than feed-based inference to avoid substantial classification errors.

Microstructure is what determines the trading cost of holding an informational position. A prediction market with noisy microstructure produces noisier prices than headline-level aggregation literature implicitly assumes.

## Key Data Infrastructure

- 30B order-book events archived
- 52-day coverage
- Joined with authoritative on-chain trade record
- Pre-registered stratified panel methodology
