---
title: "Nexus: An Agentic Framework for Time Series Forecasting"
url: "https://arxiv.org/abs/2605.14389"
source: "arxiv"
date: "2026-05-14"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Nexus: An Agentic Framework for Time Series Forecasting

**Authors:** Sarkar Snigdha Sarathi Das, Palash Goyal, Mihir Parmar, Nanyun Peng, Vishy Tirumalashetty, Chun-Liang Li, Rui Zhang, Jinsung Yoon, Tomas Pfister (Google / Pennsylvania State University)

**arXiv:** 2605.14389 | Submitted: May 14, 2026

## Summary

Multi-agent framework combining Time Series Foundation Models (TSFMs) with LLMs for multimodal time series forecasting. Addresses the gap: TSFMs ignore text; LLMs ignore numerical patterns. Nexus decomposes prediction into specialized agents for macro/micro temporal fluctuations and integrates contextual (news/events) data.

## Key Methods

- Multi-agent decomposition: macro-level + micro-level temporal fluctuation agents
- Context integration agent: processes unstructured data (news, events)
- Synthesizer agent merges predictions into final forecast
- Domain-level calibration loop: learns from past prediction errors across historical splits
- Generates reasoning traces explaining forecast drivers
- Evaluated on data after LLM knowledge cutoffs: Zillow real estate metrics + volatile stock datasets

## Key Results

- Consistently matches or outperforms SOTA TSFMs (including TimesFM-2.5) and strong LLM baselines
- Works strictly outside LLM knowledge cutoffs (no data leakage)
- Produces interpretable reasoning traces
- Current-gen LLMs possess stronger intrinsic forecasting ability than recognized when properly structured

## Relevance to Polymarket Trading

The calibration loop (evaluating past prediction errors to generate domain-specific review guidelines) is directly applicable to Polymarket market categories. Synthesizer agent pattern for combining numerical price data with news context is the right architecture for event markets.
