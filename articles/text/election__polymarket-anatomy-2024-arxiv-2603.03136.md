---
title: "The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election"
url: "https://arxiv.org/abs/2603.03136"
source: "arXiv"
date: "2026-03-03"
type: "academic_paper"
theme: "election"
lang: "en"
---

# The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election

**Authors:** Kwok Ping Tsang and Zichao Yang  
**ArXiv ID:** 2603.03136  
**Submitted:** March 3, 2026 (revised May 7, 2026)

## Abstract

Using on-chain Polygon data, the authors analyze Polymarket's 2024 U.S. Presidential Election market and develop a transaction-level accounting framework with two components: a volume decomposition that separates exchange-equivalent turnover from share minting and burning, and trader-level disagreement measures. Naive aggregation reports $958M of October Trump-market volume, compared with $391M under their decomposition. Market quality improved as arbitrage-deviation half-lives fell from hours to under a minute and Kyle's λ dropped from 0.53 to 0.01. During October's large-account episode, capital flowed into both sides simultaneously, consistent with heterogeneous-beliefs trading rather than one-sided manipulation.

## Key Problem Identified

Polymarket reported $3.6 billion in presidential-race volume, but this headline figure misrepresents actual trading activity. The platform combines three distinct mechanisms: peer-to-peer share exchanges, share minting (creation), and share burning (redemption). "Naive aggregation reports $958M of October Trump-market volume, compared with $391M under our decomposition," revealing that raw on-chain data double-counts transactions.

## Methodological Contributions

**Volume Decomposition Framework:**
- Exchange-equivalent volume: Secondary-market turnover only
- Net inflow: Fresh capital entering or leaving markets
- Gross market activity: Combined measure including both

**Trader-Level Disagreement Metrics:**
- Exposure dispersion (inequality of position sizes)
- Headcount polarization (balance of trader counts)
- Volume-weighted polarization (balance of dollar volumes)

## Major Findings

**Market Maturation Over Time:**
Kyle's lambda—measuring price sensitivity to order flow—fell dramatically from 0.53 (July) to 0.01 (October), indicating reduced vulnerability to price manipulation as liquidity deepened.

**Arbitrage Convergence:**
Pricing deviations from the YES+NO=$1 identity narrowed from multi-hour half-lives to under one minute by October, showing improved market efficiency.

**Trading Patterns:**
- Trump market dominated, reaching $391M monthly exchange-equivalent volume in October
- October large-account episode ($25–46M pro-Trump bets) coincided with simultaneous Democratic-side capital inflows, suggesting heterogeneous beliefs rather than manipulation
- Participation concentrated in European and U.S. business hours

**Trader Behavior:**
Nearly 40% of traders operated exclusively in Trump YES shares, indicating predominantly directional rather than hedging strategies. Multi-market participation occurred in roughly 19% of traders.

## Capital Flow Dynamics

The research identified a rising 90-day rolling correlation between Trump and Democratic-side net inflows in October, peaking at 0.76. This bilateral capital movement contradicts simple manipulation narratives, instead reflecting disagreement-driven trading where sophisticated participants bet on opposing outcomes.

## Price Impact Evidence

At peak illiquidity (July end), a $1 million net buy order could shift implied probabilities by approximately 13 percentage points. By October, the same trade moved prices by only 0.25 percentage points, demonstrating the market's dramatic improvement in depth.

## Broader Significance

This framework generalizes beyond Polymarket to any tokenized prediction market with endogenous share supply, enabling consistent cross-platform comparison and more accurate activity measurement as blockchain-based prediction markets proliferate.
