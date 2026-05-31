---
title: "PolySwarm: A Multi-Agent Large Language Model Framework for Prediction Market Trading and Latency Arbitrage"
url: https://arxiv.org/abs/2604.03888
source: arxiv.org
date: "2026-04-04"
type: paper
theme: strategies
lang: en
---

# PolySwarm: Multi-Agent LLM Framework for Prediction Market Trading

**Authors:** Rajat M. Barot (SUNY Binghamton), Arjun S. Borkhatariya (Arizona State University)
**arXiv ID:** 2604.03888

## Abstract

PolySwarm is a multi-agent LLM framework deploying 50 diverse personas to evaluate binary outcome markets on decentralized platforms like Polymarket. The system combines individual probability estimates through confidence-weighted Bayesian combination of swarm consensus with market-implied probabilities, and applies quarter-Kelly position sizing for risk-controlled execution. It incorporates information-theoretic analysis using KL and Jensen-Shannon divergence to detect market inefficiencies, plus a latency arbitrage module exploiting stale prices within human reaction windows.

## Swarm Aggregation Methodology

**Two-Stage Bayesian Combination:**
- Stage 1: p_swarm = Σ(w_i × p_i) / Σ(w_i) (confidence-weighted averaging)
- Stage 2: 0.70×p_swarm + 0.30×p_market (linear Bayesian mixture)

**Agent Pool Architecture:**
- 50 diverse personas: macro economists, technical analysts, contrarians, domain specialists
- Drawn without replacement per evaluation
- 5-second scan cycle with asyncio concurrent LLM inference
- Rate-limiting semaphores across multiple providers (Claude, GPT, Ollama)

## Latency Arbitrage Module

For cryptocurrency contracts, derives CEX-implied probabilities using log-normal pricing:
pcex = Φ(ln(S/K) / (σ√T))
where S=spot price, K=strike, σ=volatility, T=time-to-expiry.

5-second polling cycle targets exploitable human processing delays before prices equilibrate.

## Position Sizing: Quarter-Kelly

Trades execute only when Expected Value exceeds 5% threshold:
EV = p_combined × b - (1-p_combined)

Quarter-Kelly: f = 0.25 × f*
where f* = [p·b-(1-p)]/b

Hard caps on maximum position and daily losses for risk management.

## Information-Theoretic Analysis

- KL divergence: DKL(P‖Q) = Σx P(x) log(P(x)/Q(x))
- Jensen-Shannon divergence (symmetric bounded alternative)
- Detects negation pair mispricings where P(E)+P(¬E) ≠ 1
- Identifies mutually exclusive outcome partition violations

## Evaluation Metrics

- **Brier Score:** BS = 1/N Σ(ft−ot)² — expert humans achieve 0.10–0.18 on political/economic events
- **Log-Loss:** LL = −1/N Σ[ot·log ft + (1−ot)·log(1−ft)]
- **Calibration analysis** benchmarked against human superforecasters

## Key Results

Swarm aggregation consistently outperforms single-model baselines in probability calibration on Polymarket prediction tasks.

## Technical Infrastructure

- Market selection filters by minimum trading volume and recent activity
- Asynchronous SQLite caching with configurable TTL
- Real-time Vue 3 dashboard with WebSocket updates
- Persistent tracking of predictions, executions, PnL

## Challenges

Authors acknowledge hallucination in agent pools, computational scaling costs, regulatory exposure, and feedback-loop risks as open problems.
