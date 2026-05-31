---
title: "PolyBench: Benchmarking LLM Forecasting and Trading Capabilities on Live Prediction Market Data"
url: "https://arxiv.org/abs/2604.14199"
source: "arxiv"
date: "2026-04-01"
type: "paper"
theme: "academic"
lang: "en"
authors: ["Pu Cheng", "Juncheng Liu", "Yunshen Long"]
---

# PolyBench: Benchmarking LLM Forecasting and Trading Capabilities on Live Prediction Market Data

## Abstract

The researchers created a multimodal benchmark using Polymarket data containing 38,666 binary prediction markets. They evaluated seven state-of-the-art LLMs by generating 36,165 predictions using synchronized market snapshots, order-book data, and news streams from February 2026.

## Key Findings

- Only 2 of 7 models achieved positive financial returns, revealing a gap between surface-level language fluency and genuine probabilistic reasoning
- MiMo-V2-Flash delivered 17.6% Confidence-Weighted Return; Gemini-3-Flash achieved 6.2%
- The remaining five models generated losses despite expressing high confidence
- PolyBench serves as a contamination-proof, financially-grounded evaluation standard for future LLM research
- Code and data made publicly available
