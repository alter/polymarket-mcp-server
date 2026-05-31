---
title: "LiveTradeBench: Seeking Real-World Alpha with Large Language Models"
url: "https://arxiv.org/abs/2511.03628"
source: "arxiv"
date: "2025-11-01"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# LiveTradeBench: Seeking Real-World Alpha with Large Language Models

**Authors:** Haofei Yu, Fenghai Li, Jiaxuan You

**arXiv:** 2511.03628 | Submitted: November 2025

## Summary

Live trading environment for evaluating LLM decision-making across structurally different markets including US stocks and Polymarket prediction markets. 50-day live evaluations of 21 LLMs. Key finding: static benchmark performance does not correlate with live trading success.

## Key Methods

- 50-day live evaluations of 21 LLMs across model families
- Agents observe: prices, news, portfolio holdings → output percentage allocations
- Multi-market testing: US stocks + Polymarket prediction markets
- Real-time data streaming (no offline backtesting)
- Metrics: portfolio performance, risk preferences, signal responsiveness

## Key Results

- Static benchmark performance (LMArena scores) does NOT correlate with trading success
- Models exhibit distinct portfolio styles reflecting different risk preferences
- Certain LLMs effectively adapt decisions based on live market signals
- Exposes fundamental gap between static evaluation and real-world competence
- Polymarket included as a test market alongside US equities

## Relevance to Polymarket Trading

Critical warning: lab benchmark scores (MMLU, LMArena etc.) are useless for predicting trading performance. Must evaluate LLMs live on actual prediction market data. The 21-model comparison provides a practical ranking of which LLMs actually adapt to live signals.
