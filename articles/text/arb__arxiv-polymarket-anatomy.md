---
title: "The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election"
url: "https://arxiv.org/abs/2603.03136"
source: "arxiv.org"
date: "2026-03-03"
type: "academic_paper"
theme: "arb"
lang: "en"
---

# The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election

**Authors:** Kwok Ping Tsang, Zichao Yang

**Submitted:** March 3, 2026 (v1); Revised May 7, 2026 (v2)

**HTML:** https://arxiv.org/html/2603.03136v1
**SSRN:** https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6336679

---

## Abstract

Using complete on-chain Polygon data, this paper analyzes Polymarket's 2024 U.S. Presidential Election market. The researchers developed a transaction-level accounting framework to address measurement problems arising from Polymarket's heterogeneous trade mechanisms (peer-to-peer exchanges, share minting, burning, position conversion).

---

## Methodology

- On-chain transaction analysis from Polygon blockchain
- Volume decomposition framework with three complementary metrics:
  - **Exchange-equivalent trading volume**: secondary-market turnover
  - **Net inflow**: new capital committed to outcomes
  - **Gross market activity**: total economic engagement
- Trader-level disagreement measurement systems
- Market quality metrics: arbitrage deviation analysis, Kyle's lambda calculations

---

## Key Findings

### Volume Discrepancies (Critical for Researchers)
- Naive October Trump-market volume: **$958 million**
- After decomposition: **$391 million** (59% downward adjustment)
- Naive aggregation severely overstates real trading due to minting/burning mechanics
- Implication: published Polymarket volume figures should be treated with caution

### Market Efficiency
- **Kyle's lambda** (price impact) fell from 0.518 to 0.01 over 10 months
- **Arbitrage deviation half-lives** fell from hours to under a minute as volume grew
- Market enforced $1.00 constraint via arbitrage mechanisms throughout

### Three Critical Episodes
1. **Biden's withdrawal (July):** Sharp correlation spikes between Trump and Democrat markets
2. **Post-debate period (mid-September):** Temporary consensus formation
3. **October whale activity:** $30M in directional bets, provoking counter-flows

### Trader Behavior
- 71.8% of participants traded in Trump YES markets
- Strong intraday seasonality peaking during U.S. business hours
- Pronounced specialization: most traders concentrated on single candidates
- Large October directional bets consistent with heterogeneous-beliefs trading, not manipulation

---

## Implications for Arbitrage

- Early market (sparse volume): arbitrage deviations lasted hours, creating large windows
- Mature market (October+): arbitrage deviations closed in under a minute
- Arbitrage activity was the primary mechanism enforcing the $1.00 probability sum constraint
- Market maturation driven by volume influx, not participant sophistication changes
