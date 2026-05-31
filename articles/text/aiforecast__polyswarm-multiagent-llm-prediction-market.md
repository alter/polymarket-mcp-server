---
title: "PolySwarm: A Multi-Agent Large Language Model Framework for Prediction Market Trading and Latency Arbitrage"
url: "https://arxiv.org/abs/2604.03888"
source: "arxiv"
date: "2026-04-04"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# PolySwarm: A Multi-Agent Large Language Model Framework for Prediction Market Trading and Latency Arbitrage

**Authors:** Rajat M. Barot (SUNY Binghamton), Arjun S. Borkhatariya (Arizona State University)

**arXiv:** 2604.03888 | Submitted: April 4, 2026

## Abstract

This paper presents PolySwarm, a novel multi-agent large language model (LLM) framework designed for real-time prediction market trading and latency arbitrage on decentralized platforms such as Polymarket. PolySwarm deploys a swarm of 50 diverse LLM personas that concurrently evaluate binary outcome markets, aggregating individual probability estimates through confidence-weighted Bayesian combination of swarm consensus with market-implied probabilities, and applying quarter-Kelly position sizing for risk-controlled execution. The system incorporates an information-theoretic market analysis engine using Kullback-Leibler (KL) divergence and Jensen-Shannon (JS) divergence to detect cross-market inefficiencies and negation pair mispricings. A latency arbitrage module exploits stale Polymarket prices by deriving CEX-implied probabilities from a log-normal pricing model and executing trades within the human reaction-time window. We provide a full architectural description, implementation details, and evaluation methodology using Brier scores, calibration analysis, and log-loss metrics benchmarked against human superforecaster performance. We further discuss open challenges including hallucination in agent pools, computational cost at scale, regulatory exposure, and feedback-loop risk, and outline five priority directions for future research. Experimental results demonstrate that swarm aggregation consistently outperforms single-model baselines in probability calibration on Polymarket prediction tasks.

## Key Methods

- 50 diverse LLM personas for concurrent binary market evaluation
- Confidence-weighted Bayesian aggregation: swarm consensus + market-implied priors
- KL divergence and Jensen-Shannon divergence for cross-market inefficiency detection
- Negation pair mispricing detection (YES + NO price deviating from $1)
- Latency arbitrage module: CEX-implied probabilities via log-normal pricing model
- Quarter-Kelly position sizing for risk management

## Key Results

- Swarm aggregation consistently outperforms single-model baselines in probability calibration on Polymarket
- Production-ready multi-agent LLM trading terminal architecture described in detail
- Asynchronous execution pipeline for real-time trading

## Relevance to Polymarket Trading

Directly applicable: designed specifically for Polymarket. Core insights: (1) ensemble of diverse LLM personas beats single model, (2) KL/JS divergence detects exploitable mispricings, (3) CEX-lag arbitrage via log-normal model, (4) quarter-Kelly sizing.
