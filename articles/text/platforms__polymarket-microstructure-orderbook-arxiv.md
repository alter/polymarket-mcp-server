---
title: "The Anatomy of a Decentralized Prediction Market: Microstructure Evidence from the Polymarket Order Book"
url: "https://arxiv.org/html/2604.24366v1"
source: arxiv
date: "2026-04"
type: paper
theme: platforms
lang: en
---

# The Anatomy of a Decentralized Prediction Market: Microstructure Evidence from the Polymarket Order Book

ArXiv: 2604.24366v1

## Overview
Analysis of 30 billion order-book events across 52 days, joined with authoritative blockchain trade records.

## Critical Methodological Finding
Trade direction inferred from Polymarket's public feed achieves only ~59% agreement with on-chain ground truth (volume-weighted) — barely exceeding random chance. Effective spreads and Kyle's lambda estimates flip signs on 67% and 60% of markets respectively in the first observation window.

**Implication**: Any Polymarket microstructure result that depends on trade direction MUST source it from on-chain OrderFilled events, not the public feed.

## Eight Stylized Facts

1. **Longshot Premium**: Half-spreads widen dramatically for low-probability outcomes (1,300–1,800 basis points) — suggests liquidity constraints rather than behavioral bias.

2. **Depth Distribution**: Order-book depth follows a "uniform geometric grid" pattern rather than concentrated top-of-book structure.

3. **Block Timing**: Quote updates show minimal clustering around Polygon block boundaries despite high statistical significance from large sample sizes.

4. **Maker Concentration**: Decentralized liquidity provision (median ~32 effective makers) — contrasts with equity markets dominated by HFT firms.

5. **Category Effects**: Effective spreads vary modestly by market category (Crypto, Sports, Geopolitics, Other).

6. **Ingestion Latency**: 41.5ms median archive delay with multi-second tails.

7. **Wash Trading**: Self-counterparty wash activity: 1% median, 22% maximum — substantially lower than cryptocurrency token exchanges (25–70%).

8. **Depth Decay**: Markets approaching resolution show reduced depth; within-category log-log slope of 0.55 on seconds-to-close.

## Key Constraint for Decentralized CLOB Venues
When platforms broadcast post-match order-book snapshots without exposing taker identity, direction-dependent measures become unreliable. The author provides a replication package enabling researchers to join off-chain and on-chain data sources correctly.
