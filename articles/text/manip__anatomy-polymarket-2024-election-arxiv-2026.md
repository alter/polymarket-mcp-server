---
title: "The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election"
url: "https://arxiv.org/abs/2603.03136"
source: arXiv (econ.GN)
date: "2026-03-03"
type: academic_paper
theme: manip
lang: en
---

# The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election

**Authors:** Kwok Ping Tsang, Zichao Yang  
**arXiv:** 2603.03136  
**Submitted:** March 3, 2026; revised May 7, 2026  
**HTML:** https://arxiv.org/html/2603.03136v2  
**SSRN:** https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6336679

## Abstract

Uses on-chain Polygon blockchain data to analyze Polymarket's 2024 U.S. Presidential Election market. Develops transaction-level accounting framework separating exchange-equivalent turnover from share minting/burning. Produces trader-level disagreement measures and price-discovery quality metrics.

## Volume Overcount Problem

Naive aggregation of October Trump-market volume: **$958 million**.  
After proper decomposition: **$391 million** in exchange-equivalent turnover.

Raw on-chain flows mix secondary-market P2P trades with share minting and burning (primary issuance), inflating naive volume figures by ~2.4×.

## Market Quality Over Time

| Metric | Early 2024 | Oct–Nov 2024 |
|--------|-----------|--------------|
| Kyle's lambda (λ) | 0.53 | 0.01 |
| Arbitrage half-life | Several hours | Under 1 minute |

Kyle's lambda: a $1 million net buy order could move implied probability by **~13 percentage points** in early months; fell to ~0.1pp by October.

## October "Whale" Episode

The reported $25–46 million pro-Trump positions in October coincided with simultaneous Democratic-side capital inflows. Pattern is **consistent with heterogeneous-beliefs trading** (genuine disagreement) rather than one-sided price manipulation.

Trading activity: 71.8% of traders participated in Trump YES markets; high-activity traders concentrated in European/U.S. business hours.

## Key Findings

1. Market matured dramatically over the election cycle — not just in size but in efficiency
2. Arbitrage deviations from YES+NO = $1 pricing identity nearly eliminated by late 2024
3. The "whale" October episode does not show manipulation signatures; both sides received capital
4. Framework generalizes to other tokenized prediction markets

## Related Work Referenced

- Rahman et al. (2025): Polymarket outperforms polls, particularly in swing states
- Ng et al. (2026): Cross-platform price discovery Polymarket vs Kalshi vs PredictIt vs Robinhood (SSRN 5331995)
- Rahman, Al-Chami, Clark (2026): SoK on decentralized prediction market microstructure (arXiv:2510.15612)
