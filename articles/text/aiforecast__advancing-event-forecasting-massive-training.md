---
title: "Advancing Event Forecasting through Massive Training of Large Language Models: Challenges, Solutions, and Broader Impacts"
url: "https://arxiv.org/abs/2507.19477"
source: "arxiv"
date: "2025-07-25"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Advancing Event Forecasting through Massive Training of Large Language Models: Challenges, Solutions, and Broader Impacts

**Authors:** Sang-Woo Lee, Sohee Yang, Donghyun Kwak, Noah Y. Siegel

**arXiv:** 2507.19477 | Submitted: July 25, 2025

## Abstract

Many recent papers have studied the development of superforecaster-level event forecasting LLMs. While methodological problems with early studies cast doubt on the use of LLMs for event forecasting, recent studies with improved evaluation methods have shown that state-of-the-art LLMs are gradually reaching superforecaster-level performance, and reinforcement learning has also been reported to improve future forecasting. Additionally, the unprecedented success of recent reasoning models and Deep Research-style models suggests that technology capable of greatly improving forecasting performance has been developed. Therefore, based on these positive recent trends, we argue that the time is ripe for research on large-scale training of superforecaster-level event forecasting LLMs.

## Key Methods (Proposed)

Three core training challenges identified:
1. **Noisiness-sparsity:** Sparse and noisy outcome data
2. **Knowledge cut-off:** Temporal gaps in training data
3. **Simple reward structure:** Inadequate signal design

Proposed mitigations:
- Hypothetical event Bayesian networks
- Incorporating poorly-recalled and counterfactual events
- Auxiliary reward signals
- Aggressive use of market, public, and crawling datasets

## Key Results

Position paper; no new empirical results. Provides roadmap for large-scale superforecaster-level LLM training.

## Relevance to Polymarket Trading

Roadmap paper: outlines what's needed to build a superforecaster-level model trained on prediction market data. The three challenges (noisiness-sparsity, knowledge cutoff, reward structure) must be addressed in any serious Polymarket training pipeline.
