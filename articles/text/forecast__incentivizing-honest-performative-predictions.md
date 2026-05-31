---
title: "Incentivizing Honest Performative Predictions with Proper Scoring Rules"
url: https://arxiv.org/abs/2305.17601
source: arxiv
date: "2023-05-28"
type: paper
theme: forecast
lang: en
---

# Incentivizing Honest Performative Predictions with Proper Scoring Rules

**Authors:** Caspar Oesterheld, Johannes Treutlein, Emery Cooper, Rubi Hudson (CMU, UC Berkeley)
**Submitted:** May 28, 2023 (accepted for UAI 2023)
**arXiv:** 2305.17601

## Abstract

Investigates how traditional proper scoring rules function when predictions can actually influence outcomes (performative predictions). Demonstrates that standard proper scoring rules no longer incentivize honest reporting in this setting.

## Key Findings

1. **Score Maximization vs. Accuracy:** "Reports maximizing expected score generally do not reflect an expert's beliefs" in performative settings.

2. **Binary vs. Multi-outcome:** For binary predictions with bounded influence, scoring rules can be designed where optimal reports approach fixed points arbitrarily closely. However, this becomes impossible for predictions involving more than two outcomes.

3. **Empirical Results:** Prediction errors often exceed 5–10% in numerical simulations.

4. **Fixed points:** Predictions that accurately represent expert beliefs *after* the prediction's impact has occurred — the relevant equilibrium concept for performative settings.

## Application Context

Relevant for:
- Public forecasts that move markets (performative in financial sense)
- Election predictions that influence turnout
- Prediction markets where forecasts are publicly visible and influence trader behavior

## Relevance to Prediction-Market Pricing

In prediction markets, published prices are themselves performative — they influence trader behavior, which in turn influences future prices. This means standard calibration incentives break down. Understanding this failure mode is essential for building robust probability estimation systems for Polymarket.
