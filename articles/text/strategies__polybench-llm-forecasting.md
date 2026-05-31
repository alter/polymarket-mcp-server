---
title: "PolyBench: Benchmarking LLM Forecasting and Trading Capabilities on Live Prediction Market Data"
url: https://arxiv.org/abs/2604.14199
source: arxiv.org
date: "2026-04-18"
type: paper
theme: strategies
lang: en
---

# PolyBench: Benchmarking LLM Forecasting and Trading Capabilities on Live Prediction Market Data

## Overview

PolyBench introduces a multimodal benchmark from Polymarket data containing **38,666 binary prediction markets spanning 4,997 events**, paired with order book data and news streams.

Researchers evaluated **seven state-of-the-art LLMs** to assess their forecasting abilities under real market conditions.

## Methodology

Three data sources synchronized:
- Central Limit Order Book (CLOB) snapshots
- Real-time news streams
- Market prediction data

**Data collection period:** February 6–12, 2026
**Total predictions generated:** 36,165 (under identical, timestamp-locked market states)

## Evaluation Metrics

- **Directional accuracy**: Was the model's prediction direction correct?
- **Confidence-Weighted Return (CWR)**: Return weighted by model confidence
- **Annualized Percentage Yield (APY)**: Annualized financial return
- **Sharpe ratio**: Via realistic order-book execution simulation

## Results

| Model | CWR | Result |
|-------|-----|--------|
| MiMo-V2-Flash | +17.6% | Profitable |
| Gemini-3-Flash | +6.2% | Profitable |
| 5 other models | negative | Loss-making |

**Critical finding:** Only 2 of 7 models achieved positive financial returns despite all models exhibiting "uniformly high stated confidence."

## Key Insight

Results reveal "a pronounced performance divergence" highlighting "the gap between surface-level language fluency and genuine probabilistic reasoning" in live market conditions.

High stated confidence does not correlate with actual forecasting accuracy on financial prediction tasks. Models that "sound confident" frequently lose money when trading.

## Implications for Strategy

- LLM-based trading bots must be evaluated on financial return metrics, not just accuracy
- Ensemble and calibration approaches matter more than model size
- Market-implied probabilities contain information that most LLMs underutilize
