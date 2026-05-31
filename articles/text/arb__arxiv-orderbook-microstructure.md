---
title: "The Anatomy of a Decentralized Prediction Market: Microstructure Evidence from the Polymarket Order Book"
url: "https://arxiv.org/abs/2604.24366"
source: "arxiv.org"
date: "2026-04-01"
type: "academic_paper"
theme: "arb"
lang: "en"
---

# The Anatomy of a Decentralized Prediction Market: Microstructure Evidence from the Polymarket Order Book

**Author:** Philipp D. Dubach

**Date:** April 2026

**HTML:** https://arxiv.org/html/2604.24366v1

---

## Abstract

This paper analyzes Polymarket's limit-order-book microstructure using 30 billion WebSocket events across 52 days, joined with authoritative on-chain trade records from the Polygon blockchain. It examines 600 pre-registered markets to understand trading costs, liquidity provision, and market structure in decentralized prediction markets.

---

## Data & Methodology

- **Period:** February 28 – March 27, 2026 (28-day calibration window)
- **Events:** 30 billion WebSocket events
- **On-chain trades:** 255 million OrderFilled events from CTF Exchange V1
- **Sample:** 600-market pre-registered panel
- **Grid:** 60-second sampling for tractability
- **Spread decomposition:** Glosten-Harris approach (97/100 top markets converged)

---

## Eight Stylized Facts (Key Findings)

### 1. Longshot Spread Premium
- Half-spreads climb from ~400 basis points (mid-probability) to 1,300–1,800 bps for low-probability outcomes
- Reflects inventory-risk constraints on binary contracts near resolution

### 2. Depth Distribution
- L2 orderbook profile resembles uniform geometric grid (not concentrated top-of-book)
- Median concentration ratio: 0.137 (near uniform benchmark of 0.10)
- Different from equity market HFT-concentrated order books

### 3. Block-Clock Alignment
- Quote-update timing shows negligible clustering around Polygon block boundaries
- Despite statistical significance from large event counts — practically irrelevant

### 4. Maker Concentration
- Median effective maker count: ~32 per market
- Broad decentralization contrasting with equity market HFT concentration
- But thin tail: few dominant wallets by volume

### 5. Category Differences
- Effective spreads vary by market category: Crypto, Sports, Geopolitics, Other have distinct profiles

### 6. Archive Latency
- Median ingestion delay: 41.5 milliseconds
- Multi-second tail — matters for latency arbitrage strategies

### 7. Wash Trading
- Direct self-counterparty wash share: 1% median
- 22% upper tail — substantially lower than cryptocurrency token exchanges

### 8. Depth Decay Near Resolution
- Markets approaching resolution show declining depth at 0.55 log-log slope
- After controlling for volume and category effects

---

## Critical Methodological Finding (Most Important)

**Trade-direction inference accuracy:** Only ~59% agreement between public order-book feed and on-chain ground truth — barely above random.

When feed-inferred directions replaced with authoritative on-chain data across top-100 markets:
- Effective half-spread flipped sign on **67% of markets** (first window)
- Kyle's lambda flipped sign on **60%**
- These rates fall below ~80% accuracy of Lee-Ready algorithms on equity exchanges

**Implication:** Any Polymarket microstructure work depending on aggressor sign MUST use on-chain OrderFilled events, not the public WebSocket feed's `change_side` field.

---

## Implications for Market Makers and Arbitrageurs

- The longshot spread premium creates opportunities for market makers willing to quote tail probabilities
- Shallow depth near resolution constrains size for arbitrageurs
- 41.5ms median latency matters for sub-second arbitrage strategies
- Wash trading is minimal, unlike crypto token markets — cleaner signal
- Decentralized maker base means no single dominant HFT to front-run you
