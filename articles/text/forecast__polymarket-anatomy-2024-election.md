---
title: "The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election"
url: https://arxiv.org/abs/2603.03136
source: arxiv
date: "2026-05-07"
type: paper
theme: forecast
lang: en
---

# The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election

**Authors:** Kwok Ping Tsang (Virginia Tech), Zichao Yang (Zhongnan University of Economics and Law)
**Published:** May 7, 2026
**arXiv:** 2603.03136v2

## Abstract

Analyzes Polymarket's 2024 presidential election market using on-chain Polygon data. Developed a transaction-level accounting framework distinguishing exchange-equivalent turnover from share minting and burning. Naive aggregation reported $958M in October Trump-market volume versus $391M under proper decomposition. Market quality improved dramatically — arbitrage deviations narrowed from hours to under one minute, and Kyle's λ fell from 0.53 to 0.01.

## Main Findings

**Four Key Results:**

1. **Trading Activity Evolution:** Capital flows and disagreement peaked in October, aligning with the political calendar as fresh money entered both Trump and Democratic-side markets simultaneously.

2. **Arbitrage Efficiency:** Price deviations from the YES+NO=$1 identity converged rapidly as liquidity deepened — half-lives fell from several hours early-year to under 60 seconds by October/November.

3. **Trader Behavior:** Participation concentrated during European and U.S. business hours; most traders held single-candidate directional positions rather than hedging across outcomes.

4. **Price Impact Decline:** Kyle's λ (measuring manipulation vulnerability) dropped over tenfold — from ~0.53 in July to ~0.01 in October — indicating substantially reduced market susceptibility to large trades.

## Methodology

**Two Core Contributions:**

- **Volume Decomposition:** A transaction-level framework separating secondary-market exchange volume from primary-market share minting/burning, avoiding double-counting endemic to naive on-chain aggregation.
- **Disagreement Measures:** Three trader-level metrics — exposure dispersion, headcount polarization, and volume-weighted polarization — constructed from order-level data to quantify cross-sectional trader disagreement.

## Conclusion

The market matured over time. Initial thinness (high price impact, wide arbitrage bands, limited participation) gave way to deeper liquidity and faster price discovery. The framework generalizes to other tokenized prediction markets.

## Relevance to Prediction-Market Pricing

Demonstrates how to correctly measure Polymarket trading volumes (naive on-chain aggregation overstates by 2.4x). The Kyle's λ time-series reveals optimal windows for liquidity-sensitive trading strategies. Arbitrage half-life reduction from hours to <60 seconds quantifies market efficiency gains.
