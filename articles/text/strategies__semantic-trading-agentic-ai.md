---
title: "Semantic Trading: Agentic AI for Clustering and Relationship Discovery in Prediction Markets"
url: https://arxiv.org/abs/2512.02436
source: arxiv.org
date: "2025-12-02"
type: paper
theme: strategies
lang: en
---

# Semantic Trading: Agentic AI for Clustering and Relationship Discovery in Prediction Markets

**Authors:** Agostino Capponi, Alfio Gliozzo, Brian Zhu

## Abstract

Prediction markets allow users to trade on outcomes of real-world events, but are prone to fragmentation through overlapping questions, implicit equivalences, and hidden contradictions across markets.

This paper presents an agentic AI pipeline that autonomously:
1. Clusters markets into coherent topical groups using natural-language understanding over contract text and metadata
2. Identifies within-cluster market pairs whose resolved outcomes exhibit strong dependence, including same-outcome (correlated) and different-outcome (anti-correlated) relationships

## Methodology

**End-to-end pipeline:**
- LLM-based agent reads market text
- Clusters markets into coherent topical groups
- Performs targeted within-cluster relationship discovery

**Design principle:** Combine semantic structure with statistical validation:
- Clustering narrows candidate space to pairs that share meaning
- Outcome-based dependence analysis on resolved markets filters relationships to empirically reliable ones

**Dataset:** Historical resolved markets on Polymarket

## Results

- Agent-identified relationships achieved **roughly 60–70% accuracy**
- Induced trading strategies earned **about 20% average returns over week-long horizons**

## Key Insight

The approach introduces an end-to-end pipeline that discovers latent semantic structure in prediction markets — relationships that exist but are not obvious to human traders scanning individual contracts.

**Trading implication:** Correlated market relationships (where one outcome implies another) and anti-correlated relationships (where one outcome contradicts another) can be systematically exploited by:
1. Buying underpriced correlated pairs
2. Selling overpriced contradictions
3. Hedging via discovered anti-correlations

## Why This Works

Traders focus on individual markets rather than relationships between them. Systematic cross-market analysis at scale — only possible with AI — reveals consistent mispricings that persist because manual discovery is infeasible.
