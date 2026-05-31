---
title: "Fill Probabilities in a Limit Order Book with State-Dependent Stochastic Order Flows"
url: https://arxiv.org/abs/2403.02572
source: arxiv
date: "2024-03-05"
type: paper
theme: micro
lang: en
---

# Fill Probabilities in a Limit Order Book with State-Dependent Stochastic Order Flows

**Authors:** Felix Lokin, Fenghui Yu

**Submitted:** March 5, 2024; Revised February 6, 2026

**arXiv:** 2403.02572

## Abstract (verbatim)

"This paper studies the fill probabilities of limit orders placed at different price levels in a limit order book. These probabilities play a central role in execution optimization, as limit orders are not guaranteed to be executed and inherently involve a trade-off between execution cost and execution risk. We model the limit order book within a general state-dependent stochastic framework, representing its dynamics as a collection of interacting queuing systems while incorporating key stylized market features. Within this framework, we derive semi-analytical expressions for several quantities of interest under state-dependent order flows, including the probability of a mid-price change, the fill probabilities of orders placed at the best quotes, and those of orders placed deeper in the book before the opposite best quote moves."

## Key Findings

1. **Semi-analytical fill probability expressions** via Laplace transform techniques — computationally tractable
2. **State-dependent arrival rates** — rates change based on current LOB state (imbalance, depth)
3. Fill probabilities derived for:
   - Orders at best bid/ask
   - Orders deeper in the book
   - Probability of mid-price change
4. **Validated on real foreign exchange spot market data** with good accuracy
5. Foundation: Cont-Stoikov-Talreja (2010) queueing framework extended to state-dependent case

## LOB as Queueing System

- Limit orders = "births" in the queue at each price level
- Market orders + cancellations = "deaths"
- Birth-death process with state-dependent rates
- Laplace transforms yield semi-analytical solutions

## Relevance to Polymarket CLOB Trading

- Fill probability estimation is critical for maker strategies on Polymarket
- State-dependent model accounts for spread and imbalance affecting fill likelihood
- Mid-price change probability = probability of binary price moving to a new tick — direct trading signal
- Deeper book fill probabilities: relevant for placing non-best-quote limit orders on illiquid markets
- Foundation for optimal limit order placement on Polymarket CLOB

**Classification:** Quantitative Finance (Trading and Market Microstructure)
