---
title: "QuantAgent: Price-Driven Multi-Agent LLMs for High-Frequency Trading"
url: "https://arxiv.org/abs/2509.09995"
source: "arxiv"
date: "2025-09-01"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# QuantAgent: Price-Driven Multi-Agent LLMs for High-Frequency Trading

**Authors:** Fei Xiong, Xiang Zhang, Aosong Feng, Siqi Sun, Chenyu You

**arXiv:** 2509.09995

## Summary

Multi-agent LLM framework for high-frequency trading using only price data (no news/text). Decomposes trading into 4 specialized agents: Indicator, Pattern, Trend, Risk. Tests across 9 financial instruments showing outperformance at 1-hour and 4-hour intervals.

## Key Methods

- 4 specialized agents: Indicator (technical indicators), Pattern (chart patterns), Trend (trend features), Risk (risk management)
- Domain-specific tools per agent
- Short-horizon reasoning optimized for high-frequency decisions
- Structured trading signals as input (no text/news)
- Tested on 9 financial instruments

## Key Results

- Consistent outperformance vs. baseline methods across 9 instruments
- Superior predictive accuracy at 1-hour and 4-hour intervals
- Demonstrates viability of pure price-signal LLM trading without text input

## Relevance to Polymarket Trading

Alternative approach: skip news entirely, use only price signals. For Polymarket, this means orderbook + mid-price history as sole inputs. The 4-agent specialization (Indicator/Pattern/Trend/Risk) maps to Polymarket microstructure analysis.
