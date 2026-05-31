---
title: "Prediction Arena: Benchmarking AI Models on Real-World Prediction Markets"
url: "https://arxiv.org/abs/2604.07355"
source: "arxiv"
date: "2026-03-28"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Prediction Arena: Benchmarking AI Models on Real-World Prediction Markets

**Authors:** Jaden Zhang, Gardenia Liu, Oliver Johansson, Hileamlak Yitayew, Kamryn Ohly, Grace Li (Arcada Labs / Harvard University)

**arXiv:** 2604.07355 | Submitted: March 28, 2026

## Summary

57-day longitudinal study (January 12 – March 9, 2026) of frontier AI models trading autonomously on live prediction markets (Kalshi and Polymarket) with real capital ($10,000 each), making decisions every 15–45 minutes. First study to evaluate frontier models as fully autonomous agents with real financial consequences.

## Key Methods

- Cohort 1: 6 frontier models, full 57-day period, live trading on Kalshi + Polymarket
- Cohort 2: 4 next-gen models, 3-day paper trading (preliminary)
- 7 market categories: Financial, Crypto, Weather, Politics, Entertainment, Sports, Meta/AI
- Each model starts with $10,000, autonomous decisions every 15–45 minutes
- Metrics: return, settlement accuracy, exit patterns, market preferences, token usage

## Key Results

- Cohort 1 Kalshi returns: -16.0% to -30.8% (all models lost on Kalshi)
- Cohort 1 Polymarket average: -1.1% (much better platform)
- Best result: Cohort 2 model +6.02% on Polymarket in 3 days
- Grok-4-20-checkpoint: highest settlement win rate at 71.4%
- Platform design is critical: same models perform very differently on Kalshi vs. Polymarket
- Multi-agent AI-vs-AI setting explored: information aggregation and strategic interaction

## Relevance to Polymarket Trading

Key finding: Polymarket is much more favorable than Kalshi for LLM agents (-1.1% vs. -22.6%). Models with high settlement win rates (Grok 71.4%) are viable. This is the first real-capital benchmark showing what frontier models can actually do.
