---
title: "Self-Resolving Prediction Markets for Unverifiable Outcomes"
url: https://arxiv.org/abs/2306.04305
source: arxiv
date: "2023-06-07"
type: paper
theme: forecast
lang: en
---

# Self-Resolving Prediction Markets for Unverifiable Outcomes

**Authors:** Siddarth Srinivasan, Ezra Karger, Yiling Chen
**Submitted:** June 7, 2023; Last revised February 18, 2025
**arXiv:** 2306.04305

## Abstract

Proposes a novel prediction market mechanism that elicits and efficiently aggregates information without observing the outcome, by paying agents the negative cross-entropy between their prediction and that of a carefully chosen reference agent.

## Key Findings

1. **Core Innovation:** A reference agent with superior information access serves as a reliable proxy for ground truth, enabling market resolution without direct outcome verification.

2. **Market Design:** The self-resolving mechanism terminates probabilistically after each report, with agents paid based on the final prediction. "The final agent is chosen as the reference agent since they observe the full history of market forecasts, and thus have more information by design."

3. **Game-Theoretic Result:** The mechanism achieves a perfect Bayesian equilibrium where truthful reporting becomes rational for all participants.

4. **Broader Applicability:** Extends to verifiable scenarios as well.

## Connection to Scoring Rule Markets

The mechanism is a sequential and shared proper scoring rule (market scoring rule), paying agents the difference in score between consecutive market states — identical in structure to Hanson's LMSR but designed for unverifiable outcomes.

## Relevance to Prediction-Market Pricing

Relevant for understanding the information-aggregation properties of prediction markets when outcomes are subjective or hard to verify. The market scoring rule formulation connects directly to Polymarket's AMM pricing mechanism.
