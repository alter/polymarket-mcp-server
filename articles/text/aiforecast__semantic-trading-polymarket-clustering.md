---
title: "Semantic Trading: Agentic AI for Clustering and Relationship Discovery in Prediction Markets"
url: "https://arxiv.org/abs/2512.02436"
source: "arxiv"
date: "2025-12-02"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Semantic Trading: Agentic AI for Clustering and Relationship Discovery in Prediction Markets

**Authors:** Agostino Capponi, Alfio Gliozzo, Brian Zhu

**arXiv:** 2512.02436 | Submitted: December 2, 2025

## Abstract

Prediction markets allow users to trade on outcomes of real-world events, but are prone to fragmentation through overlapping questions, implicit equivalences, and hidden contradictions across markets. We present an agentic AI pipeline that autonomously (i) clusters markets into coherent topical groups using natural-language understanding over contract text and metadata, and (ii) identifies within-cluster market pairs whose resolved outcomes exhibit strong dependence, including same-outcome (correlated) and different-outcome (anti-correlated) relationships. Using a historical dataset of resolved markets on Polymarket, we evaluate the accuracy of the agent's relational predictions. We then translate discovered relationships into a simple trading strategy to quantify how these relationships map to actionable signals. Results show that agent-identified relationships achieve roughly 60-70% accuracy, and their induced trading strategies earn about 20% average returns over week-long horizons, highlighting the ability of agentic AI and large language models to uncover latent semantic structure in prediction markets.

## Key Methods

- NLP-based market clustering: LLM reads contract text + metadata, assigns to topic groups
- Within-cluster relationship discovery: correlated (same-outcome) and anti-correlated (different-outcome) pair identification
- Historical Polymarket resolved markets as evaluation dataset
- Simple signal-to-trade strategy derived from discovered relationships

## Key Results

- Relationship prediction accuracy: ~60-70%
- Trading strategy average returns: ~20% over week-long horizons
- Demonstrates latent semantic structure in Polymarket markets exploitable by AI

## Relevance to Polymarket Trading

This is directly applicable to our arena setup. The key insight: many Polymarket markets are semantically redundant or correlated — exploiting this structure generates 20% weekly returns. The clustering approach can identify "same-outcome" pairs for convergence trades and "different-outcome" pairs for spread trades.
