---
title: "Using AI Agents to Forecast Prediction Markets"
url: "https://saulius.io/blog/using-ai-agents-to-forecast-prediction-markets"
source: "saulius.io"
date: "2025-01-01"
type: "blog"
theme: "aiforecast"
lang: "en"
---

# Using AI Agents to Forecast Prediction Markets

**Source:** Saulius blog (practitioner)
**URL:** https://saulius.io/blog/using-ai-agents-to-forecast-prediction-markets

## Key Content

Practitioner report on deploying AI agents (PrediBench/FutureBench-style) for prediction market forecasting.

### Architecture Pattern

1. Read market question
2. Conduct iterative multi-step web research (smolagents framework: `web_search` + `visit_webpage` tools, or Tavily + web scraping)
3. Generate probability estimate
4. Size position

### AIA Forecaster Architecture (practical breakdown)

- Multiple independent forecasting agents
- Supervisor agent reconciles disagreements + conducts additional research
- Produces aggregated probability

### Calibration Fix

- LLMs systematically hedge: avoid extreme probabilities
- Fix: Platt scaling to push probabilities toward extremes where evidence is strong

### Performance (AIA on ForecastBench)

- AIA Brier: 0.0753
- Human superforecasters: 0.0740
- Market baseline: 0.0965

### Ensemble (on MarketLiquid 1,610 liquid markets)

- AIA alone: 0.1258
- Market alone: 0.1106
- Ensemble (convex regression): 0.106 — beats both
- Learned weights: ~67% market / 33% AIA

### Key Takeaway

Treat AI forecasters as calibrated layer feeding into wisdom of crowds. Not a replacement — a complement. The 67/33 weighting is empirically validated.

## Relevance to Polymarket Trading

Most actionable blog post in this corpus. The 67/33 blending formula (market price + LLM correction) with Platt scaling calibration is the recommended baseline architecture for any Polymarket LLM agent.
