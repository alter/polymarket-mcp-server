---
title: "LLM-as-a-Prophet: Understanding Predictive Intelligence with Prophet Arena"
url: "https://arxiv.org/abs/2510.17638"
source: "arxiv"
date: "2025-10-20"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# LLM-as-a-Prophet: Understanding Predictive Intelligence with Prophet Arena

**Authors:** Qingchuan Yang, Simon Mahns, Sida Li, Anri Gu, Jibang Wu, Haifeng Xu (University of Chicago and affiliates)

**arXiv:** 2510.17638 | Submitted: October 20, 2025 | Revised: December 21, 2025

## Abstract

Forecasting is not only a fundamental intellectual pursuit but also is of significant importance to societal systems such as finance and economics. With the rapid advances of large language models (LLMs) trained on Internet-scale data, it raises the promise of employing LLMs to forecast real-world future events, an emerging paradigm we call "LLM-as-a-Prophet". This paper systematically investigates such predictive intelligence of LLMs. To this end, we build Prophet Arena, a general evaluation benchmark that continuously collects live forecasting tasks and decomposes each task into distinct pipeline stages, in order to support our controlled and large-scale experimentation. Our comprehensive evaluation reveals that many LLMs already exhibit impressive forecasting capabilities, reflected in, e.g., their small calibration errors, consistent prediction confidence and promising market returns. However, we also uncover key bottlenecks towards achieving superior predictive intelligence via LLM-as-a-Prophet, such as LLMs' inaccurate event recalls, misunderstanding of data sources and slower information aggregation compared to markets when resolution nears.

## Key Methods

- Prophet Arena: continuous live benchmark decomposing forecasting into pipeline stages
- Controlled, large-scale experimentation framework
- Evaluation across multiple LLM families
- Analysis of pipeline-stage contributions to accuracy

## Key Results

- Many LLMs exhibit: small calibration errors, consistent prediction confidence, promising market returns
- Key bottlenecks identified:
  1. Inaccurate event recall
  2. Misunderstanding of data sources
  3. Slower information aggregation vs. markets near resolution
- Contamination-free evaluation setting due to forward-looking questions

## Relevance to Polymarket Trading

The three identified bottlenecks map directly to Polymarket trading failure modes: recall errors → wrong base rates; data source misunderstanding → stale news; slow aggregation → don't fade consensus near resolution.
