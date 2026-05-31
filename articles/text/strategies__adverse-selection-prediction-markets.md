---
title: "Adverse Selection in Prediction Markets: Evidence from Kalshi"
url: https://law.stanford.edu/2026/04/21/adverse-selection-in-prediction-markets-evidence-from-kalshi/
source: stanford.edu
date: "2026-04-21"
type: paper
theme: strategies
lang: en
---

# Adverse Selection in Prediction Markets: Evidence from Kalshi

**Source:** Stanford Law School / SSRN, April 2026
**Dataset:** 41.6 million trades

## Methodology

Researchers adapted Kyle's λ and the Glosten-Harris decomposition (standard equity market microstructure tools) to measure adverse selection in prediction markets.

## Key Findings

### Adverse Selection Levels
- **Single-name markets** (specific companies/individuals) exhibit greater informed price impact than broad-based markets
- **Effective spreads** are only modestly wider despite higher adverse selection
- **Market makers earn twice as much per contract** in single-name markets vs. broad-based

### The Behavioral Surplus Puzzle
Why do market makers earn more despite higher adverse selection? 

**Frequency-magnitude decomposition reveals:** Traders systematically **overbet YES in markets that predominantly settle NO** — generating a behavioral surplus that cross-subsidizes adverse selection.

This is the longshot bias in action, measured precisely.

### Order Flow Toxicity
Adapting the **VPIN (Volume-synchronized Probability of Informed Trading) metric**:
- One-sided order flow predicts maker losses in single-name markets
- Does NOT predict maker losses in broad-based markets
- Suggests a new microstructure equilibrium concept for bilateral settlement markets

## Trading Implications

### For Market Makers
- Single-name markets: Higher adverse selection risk but higher gross spread income
- Broad-based markets: Lower adverse selection, more stable but lower income
- VPIN can be used as a real-time filter — stop market-making when VPIN spikes

### For Informed Traders
- Informed traders systematically extract value from single-name markets
- The behavioral surplus (YES overbetting) provides the funding mechanism
- Markets with concentrated informed traders and dispersed retail bettors offer the best directional edge

### For Longshot Traders
- The study directly measures why longshot betting is negative EV
- Systematic YES overbetting on low-probability single-name markets confirms behavorial bias at scale

## Related Research

**ACM AI in Finance paper:** High-Frequency Market Making with Adverse Selection Control via Reinforcement Learning (dl.acm.org/doi/10.1145/3490354.3494398)
- Proposes Book Exhaustion Rate (BER) as direct adverse selection measurement
- RL agent trained to avoid large adverse-selection losses
- Tested on CME S&P 500 and 10-year Treasury futures

**Paradigm Prediction Market Challenge winner (GitHub: octavi42/prediction-market-maker)**
- Placed #2 in hackathon
- Implements volatility-adjusted quote filtering
- Inventory management via skew to prevent catastrophic losses
