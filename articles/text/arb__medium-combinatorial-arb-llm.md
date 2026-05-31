---
title: "Combinatorial Arbitrage in Prediction Markets: Why 62% of LLM-Detected Dependencies Fail to Generate Profit"
url: "https://medium.com/@navnoorbawa/combinatorial-arbitrage-in-prediction-markets-why-62-of-llm-detected-dependencies-fail-to-26f614804e8d"
source: "medium.com"
date: "2025-08-01"
type: "blog"
theme: "arb"
lang: "en"
---

# Combinatorial Arbitrage in Prediction Markets: Why 62% of LLM-Detected Dependencies Fail to Generate Profit

**Author:** Navnoor Bawa

**URLs:**
- https://medium.com/@navnoorbawa/combinatorial-arbitrage-in-prediction-markets-why-62-of-llm-detected-dependencies-fail-to-26f614804e8d
- https://navnoorbawa.substack.com/p/combinatorial-arbitrage-in-prediction

---

## Core Finding

IMDEA Networks examined 86 million Polymarket bids from the 2024 U.S. election. Using LLM-based semantic analysis to detect logically dependent market pairs:

- 46,360 market pairs processed by Linq-Embed-Mistral embeddings
- 1,576 pairs initially flagged as potentially dependent
- 374 after probabilistic constraint validation
- **13 pairs confirmed with true logical dependencies** (expert validation)
- **5 pairs generated realized arbitrage profits** totaling $95,157
- **62% failure rate** — most detected opportunities never materialized profitably

---

## Why Combinatorial Arbitrage Fails in Practice

### 1. Liquidity Asymmetry
- Primary markets: $500K+ liquidity
- Dependent markets: often only $5K
- Result: theoretical arbitrage exists but position sizes are minuscule

### 2. Execution Timing Risk
- 75% of matched Polymarket orders execute within 950 blocks (~1 hour on Polygon)
- Cross-market strategies require simultaneous fills
- Delays between legs destroy arbitrage as prices shift

### 3. Oracle Divergence Risk
- Multiple resolution mechanisms create compounded risk
- March 2025 UMA governance attack: different oracles resolved inconsistently across platforms
- What looks like a logical guarantee may resolve differently on different systems

### 4. Correlation vs. Dependency Confusion
- LLMs sometimes flagged correlated events rather than logically dependent ones
- "Trump wins presidency" and "S&P rises next day" — correlated, not logically dependent
- Requires manual verification to eliminate false positives

### 5. Computational Complexity
- General combinatorial arbitrage removal: **#P-hard** (marginal polytope with general constraints)
- **coNP-hard** for combinatorial options clearing
- Scaling to higher-order dependencies (3+ markets) remains computationally intractable

---

## Performance Reality

| Strategy | Total Profit | % of All Arbitrage |
|---|---|---|
| NegRisk rebalancing | $28,990,000 | 73.2% |
| Single-condition binary | $10,580,000 | 26.7% |
| Combinatorial (inter-market) | $95,157 | 0.24% |

**Conclusion:** Combinatorial arbitrage is theoretically appealing but practically dominated by simpler strategies. The sophistication required (LLM detection pipeline, integer programming optimization, simultaneous multi-leg execution) yields only 0.24% of total arbitrage value.

---

## Implementation Pipeline (for those still interested)

1. **Detection:** LLM semantic analysis of market descriptions
2. **Validation:** Probabilistic constraint checking (sum constraints, dominance constraints)
3. **Expert verification:** Manual review of logical dependency type
4. **Execution:** Integer programming for optimal position sizing, Frank-Wolfe algorithm for bridging continuous/discrete
5. **Monitoring:** Real-time fill tracking, cross-leg coordination

## Future Outlook
- Advances in LLM context handling may enable higher-order dependency detection
- Decentralized market creation may create more complex dependencies
- Institutional entry will further compress even these small opportunities
