---
title: "PolyBench: Benchmarking LLM Forecasting and Trading Capabilities on Live Prediction Market Data"
url: "https://arxiv.org/abs/2604.14199"
source: "arxiv"
date: "2026-04-03"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# PolyBench: Benchmarking LLM Forecasting and Trading Capabilities on Live Prediction Market Data

**Authors:** Pu Cheng, Juncheng Liu, Yunshen Long (Sichuan University)

**arXiv:** 2604.14199 | Submitted: April 3, 2026

## Abstract

Predicting real-world events from live market signals demands systems that fuse qualitative news with quantitative order-book dynamics under strict temporal discipline -- a challenge existing benchmarks fail to capture. We present PolyBench, a multimodal benchmark derived from Polymarket that records point-in-time cross-sections of 38,666 binary prediction markets spanning 4,997 events, synchronously coupling each snapshot with a Central Limit Order Book (CLOB) state and a real-time news stream. Using PolyBench, we evaluate seven state-of-the-art Large Language Models -- spanning open- and closed-source families -- generating 36,165 predictions under identical, timestamp-locked market states collected between February 6 and 12, 2026. Our multidimensional framework assesses directional accuracy, our proposed Confidence-Weighted Return (CWR), Annualized Percentage Yield (APY), and Sharpe ratio via realistic order-book execution simulation. The results reveal a pronounced performance divergence: only two of seven models achieve positive financial returns -- MiMo-V2-Flash at 17.6% CWR and Gemini-3-Flash at 6.2% CWR -- while the remaining five incur losses despite uniformly high stated confidence. These findings highlight the gap between surface-level language fluency and genuine probabilistic reasoning under live market uncertainty, and establish PolyBench as a contamination-proof, financially-grounded evaluation standard for future LLM research.

## Key Methods

- Multimodal benchmark: CLOB state + real-time news stream + market snapshot
- 38,666 binary markets across 4,997 events from Polymarket
- 7 LLMs evaluated: open- and closed-source families
- 36,165 predictions under timestamp-locked identical market states
- Metrics: directional accuracy, Confidence-Weighted Return (CWR), APY, Sharpe ratio
- Realistic order-book execution simulation

## Key Results

- Only 2/7 models achieved positive returns: MiMo-V2-Flash (+17.6% CWR), Gemini-3-Flash (+6.2% CWR)
- 5/7 models incurred losses despite high stated confidence
- Reveals gap between language fluency and genuine probabilistic reasoning under live market uncertainty
- GitHub: https://github.com/PolyBench/PolyBench

## Relevance to Polymarket Trading

Critical benchmark: tests on actual Polymarket data with CLOB. Finding that only 2/7 frontier LLMs profit is sobering. CWR metric is novel and directly applicable to sizing decisions.
