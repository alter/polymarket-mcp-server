---
title: "AIA Forecaster: Technical Report"
url: "https://arxiv.org/abs/2511.07678"
source: "arxiv"
date: "2025-11-10"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# AIA Forecaster: Technical Report

**Authors:** Rohan Alur, Bradly C. Stadie, Daniel Kang, Ryan Chen, Matt McManus, Michael Rickert, Tyler Lee, Michael Federici, Richard Zhu, Dennis Fogerty, Hayley Williamson, Nina Lozinski, Aaron Linsky, Jasjeet S. Sekhon (Bridgewater AIA Labs / related institutions)

**arXiv:** 2511.07678 | Submitted: November 10, 2025

## Abstract

The AIA Forecaster is a Large Language Model (LLM)-based system for judgmental forecasting using unstructured data. The system combines three core elements: agentic search over high-quality news sources, a supervisor agent that reconciles disparate forecasts for the same event, and a set of statistical calibration techniques to counter behavioral biases in large language models. On the ForecastBench benchmark, the AIA Forecaster achieves performance equal to human superforecasters, surpassing prior LLM baselines. While the AIA Forecaster underperforms market consensus on a prediction market benchmark, an ensemble combining it with market consensus outperforms consensus alone, demonstrating that the forecaster provides additive information.

## Key Methods

1. **Agentic search:** Multi-agent search over high-quality curated news sources
2. **Supervisor agent:** Reconciles disparate forecasts from multiple agents
3. **Statistical calibration:** Platt scaling to counter LLM hedging bias (push probabilities toward extremes)

Key finding on calibration: LLMs systematically avoid extreme probabilities; calibration correction is essential.

AIA Brier on ForecastBench: 0.0753 vs. human superforecasters: 0.0740
On liquid markets (MarketLiquid 1,610 markets): AIA 0.1258 vs. market 0.1106
Ensemble (67% market + 33% AIA): 0.106 — beats both components alone.

## Key Results

- AIA matches human superforecasters on ForecastBench (Brier 0.0753 vs. 0.0740)
- Underperforms market consensus on liquid prediction markets alone
- Ensemble combining AIA + market consensus beats consensus alone
- Learned ensemble weights: ~67% market / 33% AIA
- AI forecaster contains independent information even when individually weaker

## Relevance to Polymarket Trading

Key architecture lesson: use LLM forecaster as a 33% correction on top of Polymarket market prices. Don't replace market price — enhance it. Multi-agent + supervisor pattern is the recommended production architecture.
