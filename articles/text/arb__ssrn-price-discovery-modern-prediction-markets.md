---
title: "Price Discovery and Trading in Modern Prediction Markets"
url: "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5331995"
source: "ssrn.com"
date: "2026-04-01"
type: "academic_paper"
theme: "arb"
lang: "en"
---

# Price Discovery and Trading in Modern Prediction Markets

**Authors:** Hunter Ng, Lin Peng, Yubo Tao, Dexin Zhou

**SSRN ID:** 5331995

**URL:** https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5331995

---

## Abstract

First evidence on price discovery dynamics across modern prediction markets. Examines a unique dataset of common contracts traded on Polymarket, Kalshi, PredictIt, and Robinhood during the period leading up to the 2024 U.S. presidential election.

---

## Key Findings

### Price Discovery Leadership
- **Polymarket leads Kalshi** in price discovery, particularly when liquidity and trading activity are high
- Implication: **economically meaningful arbitrage opportunities** exist for those monitoring both platforms
- More liquid prediction markets substantially outperform polls in predicting election results
- Yet significant price disparities persist across platforms

### Cross-Platform Arbitrage Signal
- Net order imbalance from large trades strongly predicts subsequent returns
- The platform experiencing greater directional order flow from large trades tends to **lead price discovery**
- Arbitrageurs can use this signal: watch for large Polymarket moves, expect Kalshi to follow

### Structural Factors
- Platform structure, liquidity, and informed trading interaction shape prices
- Kalshi (US-regulated, USD) often lags Polymarket (offshore, USDC) by minutes during high-information events
- This lag is the primary source of exploitable cross-platform arbitrage

---

## Implications for Cross-Platform Arbitrage

1. Monitor Polymarket as price-discovery leader
2. When Polymarket moves significantly on news, check Kalshi lag
3. Execute cross-platform position while lag persists (historically: minutes)
4. Risk: different settlement oracles may not converge to same resolution

---

## Related Papers Cited

- Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets (arXiv:2602.19520)
  - Trade-size scale effect: large trades on Kalshi show amplified underconfidence in politics
  - Does NOT replicate on Polymarket — platform-specific microstructure
  - "Consumers of prediction market prices who treat them as face-value probabilities will systematically misinterpret them"
