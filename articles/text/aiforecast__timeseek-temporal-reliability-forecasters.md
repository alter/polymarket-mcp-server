---
title: "TimeSeek: Temporal Reliability of Agentic Forecasters"
url: "https://arxiv.org/abs/2604.04220"
source: "arxiv"
date: "2026-04-05"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# TimeSeek: Temporal Reliability of Agentic Forecasters

**Authors:** Hamza Mostafa (University of Waterloo), Om Shastri (Wharton School, UPenn), Dennis Lee (Automorphic Labs)

**arXiv:** 2604.04220 | Submitted: April 5, 2026

## Abstract

We introduce TimeSeek, a benchmark for studying how the reliability of agentic LLM forecasters changes over a prediction market's lifecycle. We evaluate 10 frontier models on 150 CFTC-regulated Kalshi binary markets at five temporal checkpoints, with and without web search, for 15,000 forecasts total. Models are most competitive early in a market's life and on high-uncertainty markets, but much less competitive near resolution and on strong-consensus markets. Web search improves pooled Brier Skill Score (BSS) for every model overall, yet hurts in 12% of model-checkpoint pairs, indicating that retrieval is helpful on average but not uniformly so. Simple two-model ensembles reduce error without surpassing the market overall.

## Key Methods

- 10 frontier LLMs evaluated on 150 CFTC-regulated Kalshi binary markets
- 5 temporal checkpoints per market lifecycle
- Conditions: with and without web search
- 15,000 total forecasts
- Dataset: Kalshi markets Oct 2025–Jan 2026; 5 categories; trading volumes $5.6K–$41.3M; durations 2–337 days; 73% NO / 27% YES outcomes
- Metric: Brier Skill Score (BSS)

## Key Results

- Models most competitive **early** in market life and on high-uncertainty markets
- Models least competitive **near resolution** and on strong-consensus markets
- Web search improves overall BSS for every model, but hurts in 12% of model-checkpoint pairs
- Simple 2-model ensembles reduce error but don't beat the market overall
- Motivates time-aware evaluation and selective-deference policies

## Relevance to Polymarket Trading

Critical insight for trading strategy: LLM edge is concentrated in **early-market, high-uncertainty** conditions. As market approaches resolution with strong consensus, the market price itself is more informative than LLM forecasts. Deploy LLM agents early; reduce position size as consensus builds.
