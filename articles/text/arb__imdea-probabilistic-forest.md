---
title: "Unravelling the Probabilistic Forest: Arbitrage in Prediction Markets"
url: "https://arxiv.org/abs/2508.03474"
source: "arxiv.org"
date: "2025-08-05"
type: "academic_paper"
theme: "arb"
lang: "en"
---

# Unravelling the Probabilistic Forest: Arbitrage in Prediction Markets

**Authors:** Oriol Saguillo, Vahid Ghafouri, Lucianna Kiffer, Guillermo Suarez-Tangil (IMDEA Networks Institute)

**Published:** August 5, 2025

**Classification:** Cryptography and Security (cs.CR); Trading and Market Microstructure (q-fin.TR)

**PDF:** https://arxiv.org/pdf/2508.03474
**HTML:** https://arxiv.org/html/2508.03474v1
**Published proceedings:** https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.AFT.2025.27

---

## Abstract

The researchers investigate inefficiencies on Polymarket, a prediction market platform where users trade shares tied to future event outcomes. The platform requires condition sets to be "exhaustive -- collectively accounting for all possible outcomes -- and mutually exclusive." Despite this design requirement, pricing inconsistencies create arbitrage opportunities.

The study addresses three central questions:
1. What conditions enable arbitrage?
2. Does arbitrage actually occur on Polymarket?
3. Have participants exploited these opportunities?

---

## Methodology

- Analyzed on-chain historical order book data from April 1, 2024 to April 1, 2025
- Examined 86 million Polymarket bets across thousands of markets
- Developed heuristic-driven reduction strategy to address scalability challenges, reducing naive computational complexity from O(2^(n+m)) comparisons
- Approach incorporated: timeliness, topical similarity, combinatorial relationships, and expert validation
- Used textual embeddings (Linq-Embed-Mistral) for semantic market matching
- Used LLMs to extract combinatorial relationships and logical dependencies from market condition descriptions

---

## Key Findings

### Two Arbitrage Types Identified

**1. Market Rebalancing Arbitrage** (intra-market)
- Occurs within a single market/condition
- Exploits cases where sum of YES prices across mutually exclusive outcomes != $1.00
- Generated ~$10.58M across 7,051 conditions
- Simple binary: if YES + NO < $1, buy both; if > $1, sell both

**2. Combinatorial Arbitrage** (inter-market)
- Spans across multiple logically related markets
- Exploits logical dependencies between market conditions
- Generated only ~$95,157 (0.24% of total)
- 62% of LLM-detected dependencies never yielded actual profits

### NegRisk Rebalancing (dominant sub-type)
- In multi-outcome markets (N>=3 mutually exclusive conditions where Σ(prices) must equal 1.0)
- Generated ~$29M (73% of all profits)
- Only 8.6% of available opportunities but 29× capital efficiency vs binary
- Top performing strategies: buying NO positions ($17.31M profit), buying YES positions ($11.09M profit)
- 662 markets with NegRisk rebalancing opportunities

### Scale of Exploitation
- **Total arbitrage profits: $39,587,585 ($40M)**
- Top performer: $2,009,631.76 across 4,049 transactions averaging $496 per trade
- Top 3 wallets: 10,200+ bets combined, $4.2M profit
- 14 of top 20 most profitable wallets identified as bots

### Political Markets Dominant
- 2024 US election markets were primary driver
- Biden's VP pick and Democratic nominee change created biggest spikes
- Politics markets had larger per-opportunity profits than sports despite fewer opportunities

### Speed of Opportunities
- Average arbitrage opportunity duration: 2.7 seconds (2026 data; was 12.3 seconds in 2024)
- 73% of arbitrage profits captured by sub-100ms execution bots
- Median arbitrage spread: 0.3% in 2026

---

## Conclusions

Arbitrage opportunities exist on Polymarket and have been systematically exploited by sophisticated automated participants. The market is increasingly efficient for simple binary arbitrage but NegRisk multi-outcome markets remained exploitable longer due to complexity. Combinatorial arbitrage theory exceeds practice due to execution barriers.
