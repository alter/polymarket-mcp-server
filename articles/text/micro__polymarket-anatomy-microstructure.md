---
title: "The Anatomy of a Decentralized Prediction Market: Microstructure Evidence from the Polymarket Order Book"
url: https://arxiv.org/abs/2604.24366
source: arxiv
date: "2026-04-27"
type: paper
theme: micro
lang: en
---

# The Anatomy of a Decentralized Prediction Market: Microstructure Evidence from the Polymarket Order Book

**Author:** Philipp D. Dubach

**Submission Date:** April 27, 2026 (v1); May 14, 2026 (v2)

**arXiv:** 2604.24366

## Abstract

The research examines Polymarket's microstructure using 30 billion order-book events across 52 days. The study identifies eight key patterns:

1. A longshot spread premium — contracts with extreme probabilities have wider spreads
2. Depth profile closer to uniform than to top-of-book concentration
3. Broad maker-wallet diversity with a concentrated tail
4. Sub-50 ms median archive-ingestion delay with multi-second outliers
5. Self-counterparty wash trading at median 1% (upper tail 22%)
6. Category-conditional effective-spread differences
7. No significant time-to-close effects once market duration and volume controls are applied
8. Cross-sectional depth profile explained by market duration, price level, and volume

**Key measurement finding:** Trade direction inferred from Polymarket's public order-book feed agrees with on-chain ground truth on only ~59% of buckets — substantially below the ~80% Lee-Ready accuracy benchmark on Nasdaq.

Effective half-spread calculations differ markedly between feed-derived versus on-chain trade direction. A replication package was released with Zenodo DOI.

## Relevance to Polymarket CLOB Trading

This is the definitive empirical study of Polymarket's microstructure. Key takeaways:
- Longshot spread premium: binary markets near 0 or 1 have wider spreads — informed liquidity provision opportunity
- Wash self-counterparty at 22% upper tail matters for volume interpretation
- Lee-Ready direction inference is unreliable (59% vs 80%) — order flow toxicity metrics need careful calibration on Polymarket
- Depth profile is uniform, not concentrated at top-of-book — different from equity LOB models

**Classification:** Quantitative Finance (Trading and Market Microstructure)
