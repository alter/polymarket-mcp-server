---
title: "The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election"
url: "https://arxiv.org/abs/2603.03136"
source: arxiv
date: "2026-03-03"
type: paper
theme: depm
lang: en
---

# The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election

**Authors:** Kwok Ping Tsang, Zichao Yang  
**arXiv:** 2603.03136  
**Submitted:** March 3, 2026; revised May 7, 2026  
**Also:** https://arxiv.org/abs/2604.24366 (companion paper on order book microstructure)

## Abstract

The researchers analyze Polymarket's 2024 presidential election market using blockchain data. They create "a transaction-level accounting framework with two components: a volume decomposition that separates exchange-equivalent turnover from share minting and burning, and trader-level disagreement measures."

## Key Context

Polymarket processed over **$3.6 billion** in trading volume on the presidential race alone. Unlike earlier prediction market platforms (Iowa Electronic Markets, Intrade, PredictIt), Polymarket settles matched trades on the **Polygon blockchain**, providing a publicly auditable ledger.

Because blockchain-based prediction markets involve heterogeneous trade mechanisms—share minting, burning, and conversion alongside conventional exchange—naive aggregation of on-chain flows misrepresents actual trading volume.

## Volume Decomposition Framework

The framework produces three complementary measures of market activity:
1. **Exchange-equivalent trading volume** (actual trading)
2. **Net inflow** (capital entering the market)
3. **Gross market activity** (all on-chain transactions)

**Key finding:** Naive aggregation reported $958M of October Trump-market volume, compared with $391M under the paper's decomposition—a **2.5x overstatement**.

## Market Quality Findings

Market quality improved dramatically over time:
- **Arbitrage half-lives:** Dropped from hours to under a minute
- **Kyle's lambda (price impact):** Dropped from 0.53 to 0.01
- Consistent with increasing market efficiency and tightening spreads

## Key Market Episodes Documented

1. **Biden's withdrawal** — structural shift in probabilities
2. **September presidential debate** — information event, rapid repricing
3. **Whale traders in October** — capital flowed into both sides simultaneously, consistent with **heterogeneous-beliefs trading** rather than one-sided manipulation

## Manipulation Assessment

During October's large-account episode, the simultaneous capital flow into both YES and NO sides is inconsistent with manipulation—it reflects genuine belief disagreement among large traders, not coordinated price pushing.

## Generalizability

The volume decomposition framework generalizes to other tokenized prediction markets, offering a methodology for analyzing any market using conditional token mechanics (mint/burn/trade).

## Companion Paper

"The Anatomy of a Decentralized Prediction Market: Microstructure Evidence from the Polymarket Order Book" (arXiv:2604.24366) examines order book dynamics in depth.
