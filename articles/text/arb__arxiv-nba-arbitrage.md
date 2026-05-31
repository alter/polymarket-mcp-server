---
title: "Arbitrage Analysis in Polymarket NBA Markets"
url: "https://arxiv.org/abs/2605.00864"
source: "arxiv.org"
date: "2026-04-22"
type: "academic_paper"
theme: "arb"
lang: "en"
---

# Arbitrage Analysis in Polymarket NBA Markets

**Authors:** Guang Cheng, Jiaxin Yang, Haoxuan Zou

**Submitted:** April 22, 2026

**Classification:** Quantitative Finance - Trading and Market Microstructure (q-fin.TR)

**HTML:** https://arxiv.org/html/2605.00864
**PDF:** http://www.stat.ucla.edu/~guangcheng/Arbitrage%20Analysis%20Cheng.pdf

---

## Abstract

Empirical analysis of algorithmic arbitrage opportunities in Polymarket's NBA prediction markets, examining over 75 million limit order book snapshots across 173 games to assess pricing inefficiencies.

---

## Data & Methodology

- **Collection period:** February 4 – March 4, 2026
- **Data source:** Polymarket's Central Limit Order Book (CLOB) API on Polygon blockchain
- **Volume:** 75,088,497 LOB snapshots across 173 NBA games
- **Polling cadence:** 3.6 to 5.5 second intervals (Level 1 best bid/ask)
- **Markets analyzed:** 3,042 markets total
- **Market pair analysis:** 8.59 million market states for combinatorial arbitrage

**Execution Filters Applied:**
- Minimum $10 USDC liquidity threshold
- Exclusion of post-game periods (order books essentially evaporate)
- Deduplication for Polymarket's mirrored liquidity design (Y+N order book unification)

**Context:** NBA markets account for ~30% of Polymarket's sports activity in 2025. Trading volume grew from $51M (2024) to $0.89B (2025) — 17.45× growth.

---

## Key Findings

### Single-Market Arbitrage (YES + NO < $1.00)
- Among 3,042 markets: only **7 executable in-game episodes** found
- Median duration: **3.6 seconds** per episode
- 81.1% of raw detection signals were post-game noise (filter eliminated)
- Spread markets showed deeper liquidity gaps than moneyline contracts
- Conclusion: exceedingly rare in well-capitalized NBA markets

### Combinatorial Arbitrage (cross-market logical dependencies)
- **290 active episodes** identified across moneyline-spread pairs
- Concentrated heavily in **final game minutes** (abrupt scoring events)
- Median return: **101 basis points** (statistically meaningful)
- Theoretical "jackpot" (both contracts pay simultaneously) never empirically realized
- **Critical bottleneck: liquidity shallowness**
  - 76.9% of episodes constrained to average executable size of just **14.8 shares**
  - Retail-scale only — institutional extraction structurally impossible

### Market Efficiency Assessment
- Polymarket NBA markets demonstrate "profound microstructural efficiency"
- Liquidity shallowness functionally prevents institutional-scale arbitrage extraction
- Classical limits-to-arbitrage theory validated in decentralized market context
- Profitable mispricings exist but remain structurally limited to retail-scale extraction

---

## Conclusions

Despite identifying executable mispricings, Polymarket NBA markets are microstructurally efficient. Liquidity constraints prevent scaling beyond retail positions. The theoretical middle-arbitrage jackpot scenario never materialized empirically. The study validates that prediction market arbitrage is real but size-constrained.
