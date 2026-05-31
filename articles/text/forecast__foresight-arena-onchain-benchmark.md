---
title: "Foresight Arena: An On-Chain Benchmark for Evaluating AI Forecasting Agents"
url: https://arxiv.org/html/2605.00420
source: arxiv
date: "2026-04-23"
type: paper
theme: forecast
lang: en
---

# Foresight Arena: An On-Chain Benchmark for Evaluating AI Forecasting Agents

**Authors:** Maksym Nechepurenko, Pavel Shuvalov (Devnull, Dubai)
**Submitted:** April 23, 2026
**arXiv:** 2605.00420v2

## Abstract

Introduces an on-chain evaluation system for AI forecasting agents addressing three limitations of existing benchmarks: dataset contamination, centralized trust requirements, and improper metrics. The system employs commit-reveal smart contracts on Polygon PoS with trustless outcome resolution via Gnosis framework, measuring performance through proper scoring rules rather than profit-and-loss metrics.

## Key Contributions

1. **On-chain verifiability:** All predictions recorded on Polygon with independent audit capability
2. **Trustless resolution:** Market outcomes directly from Gnosis Conditional Token Framework
3. **Proper scoring metrics:** Brier Score and novel Alpha Score preventing gaming through position sizing
4. **Murphy decomposition analysis:** Separating calibration from discriminative power
5. **Permissionless participation:** Gasless transactions via EIP-712 signed messages
6. **Statistical framework:** ~350 predictions needed to detect 0.02 alpha edge

## Main Findings

**Brier Score:** Standard squared error metric satisfying strict propriety. Expected score minimized only when reporting true probability beliefs.

**Alpha Score:** Difference between market baseline Brier and agent Brier, measuring informational edge.
Formula: α = B_baseline - B_agent

**Murphy Decomposition:** Decomposes Brier into three components:
- Uncertainty (irreducible)
- Reliability (calibration error)
- Resolution (discriminative power)

Alpha decomposes as: resolution gain + reliability gap.

**Sample Size Requirements (Proposition 3):**
- Detecting 0.02 alpha edge: ~350 predictions (50 rounds × 7 markets)
- Detecting 0.01 alpha edge: ~1,400 predictions
- Formula: n ≳ 0.139/(α*)²

**Simulation Results (50-round study):**
- Random baseline: ᾱ = -0.139 (overwhelmingly distinguishable)
- Frontier LLM archetypes: ᾱ ≈ +0.003 to +0.005 (not individually significant at this sample size)
- Market-tracking agents: Negative alpha despite similar Brier scores to market

## Why Proper Scoring Outperforms PnL

Profit-and-loss metrics conflate forecasting accuracy with "market-timing skill, position sizing, and risk tolerance." Proper scoring rules isolate predictive quality by design.

## Conclusion

The framework cleanly separates forecasting quality from trading strategy through incentive-compatible metrics and blockchain-recorded history. However, R=50 provides marginal power for detecting sub-0.02 differences between frontier models. Multi-year evaluation (200+ rounds) required for confident LLM ranking.

Code: github.com/foresight-arena/contracts

## Relevance to Prediction-Market Pricing

Provides a rigorous framework for evaluating any AI-based forecasting system against Polymarket baselines. The Alpha Score concept directly measures edge above the market — the core metric for prediction-market trading strategies.
