---
title: "Multi-Market Dependency Arbitrage: A Practical Guide"
url: "https://medium.com/coinmonks/multi-market-dependency-arbitrage-a-practical-guide-e88ad583c259"
source: "medium.com"
date: "2026-04-01"
type: "blog"
theme: "arb"
lang: "en"
---

# Multi-Market Dependency Arbitrage: A Practical Guide

**Author:** Arbigab Trading Platform

**URL:** https://medium.com/coinmonks/multi-market-dependency-arbitrage-a-practical-guide-e88ad583c259

---

## Core Concept

Prediction markets price logically dependent events independently. When related markets violate joint probability constraints, structural arbitrage exists.

**Example (Pennsylvania election):**
- Market A: "Republicans win Pennsylvania by 5%+"
- Market B: "Trump wins Pennsylvania"
- If A is true, B must be true — but markets may price this inconsistently

---

## Two Arbitrage Types

### Local Arbitrage
Exploits simple mispricing where YES + NO prices don't sum to $1.

### Dependency Arbitrage
Identifies logically impossible outcome combinations:
- If A implies B, then P(A) ≤ P(B)
- If markets price P(A) > P(B), this is pure arbitrage
- Buy B (the underpriced outcome), sell/short A

---

## Technical Foundation

### Mathematical Framework
- **Valid outcomes** described as integer programming constraints (not enumerated)
- **Bregman divergence:** Measures information distance between current prices and arbitrage-free prices. Maximum extractable profit equals this divergence.
- **Frank-Wolfe algorithm:** Bridges continuous optimization with discrete feasibility by iteratively solving integer programs

### Scale from IMDEA Research
- Single-market arbitrage: ~$10.6M (26.8%)
- Multi-condition rebalancing: ~$29.0M (73.2%)
- Cross-market dependencies: ~$0.1M (0.24%)

---

## Production System Architecture

### Three Operational Layers

**1. Data Pipeline**
- Real-time order book data via WebSockets
- Market description ingestion for semantic processing
- Historical price archive for backtesting

**2. Detection Layer**
- Cheap local checks: milliseconds (price-sum violations)
- Expensive optimization: seconds-minutes (dependency graph analysis)
- LLM semantic analysis for market pair matching

**3. Execution Validation**
- Simulates trades against current order books before committing
- Slippage estimation
- Fee threshold check (minimum spread to cover taker fees + gas)

---

## Implementation Considerations

**Position sizing:** Conservative sizing using liquidity estimates from order book depth

**Slippage buffers:** Account for market impact when sizing multi-leg positions

**Fee thresholds:** Minimum net spread = taker fee + gas cost + minimum profit margin

**Monitoring:**
- Fill rate tracking (what % of detected opportunities are actually executable)
- Latency monitoring (detection-to-execution time)
- Kill-switch alerts for drawdown or infrastructure issues

---

## Why Dependency Arbitrage Underperforms Theory

From IMDEA data:
- Only 5 of 13 verified dependent pairs generated profits
- Total: $95,157 vs. $39.5M for simpler approaches
- 10× implementation complexity for 0.24% of profit

**Root cause:** Simultaneous fill requirement across multiple markets + liquidity asymmetry + oracle divergence risk

---

## Bottom Line

Dependency arbitrage is best understood as "extracting impossible probability mass" — an optimization problem rather than directional trading. But in practice, the execution barriers are severe. Simple NegRisk rebalancing captures the same mathematical insight with far less complexity.
