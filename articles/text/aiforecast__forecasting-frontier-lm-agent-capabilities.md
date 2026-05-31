---
title: "Forecasting Frontier Language Model Agent Capabilities"
url: "https://arxiv.org/abs/2502.15850"
source: "arxiv"
date: "2025-02-01"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Forecasting Frontier Language Model Agent Capabilities

**Authors:** Govind Pimpale, Axel Højmark, Jérémy Scheurer, Marius Hobbhahn

**arXiv:** 2502.15850 | Submitted: February 2025

## Summary

Uses a two-step forecasting approach to predict future LM agent performance: Release Date → Elo → Benchmark. Validates on SWE-Bench Verified, Cybench, and RE-Bench. Forecasts LM agent performance trajectories through 2026.

## Key Methods

- Two-step approach: Release Date → Elo rating → Benchmark performance
- Backtested on 38 LMs from OpenLLM 2 leaderboard
- Principal component analysis (PC-1) for cross-benchmark assessment
- 6 forecasting methods compared

## Key Results

- By early 2026:
  - Non-specialized agents (low capability elicitation): 54% success on SWE-Bench Verified
  - SOTA agents: 87% success on SWE-Bench Verified
- Two-step method outperforms direct one-step prediction
- Note: approach may be "too conservative" — does not account for inference-compute scaling

## Relevance to Polymarket Trading

Provides calibrated expectations for LLM agent capabilities through 2026. Models will continue improving rapidly — LLM forecasting agents built today will have significantly better base models available within 12 months.
