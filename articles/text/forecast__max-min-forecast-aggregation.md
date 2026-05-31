---
title: "From Proper Scoring Rules to Max-Min Optimal Forecast Aggregation"
url: https://arxiv.org/abs/2102.07081
source: arxiv
date: "2021-02-14"
type: paper
theme: forecast
lang: en
---

# From Proper Scoring Rules to Max-Min Optimal Forecast Aggregation

**Authors:** Eric Neyman, Tim Roughgarden
**Submitted:** February 14, 2021; revised August 19, 2023
**Published:** Operations Research (DOI: 10.1287/opre.2022.2414)
**arXiv:** 2102.07081 [cs.GT]

## Abstract

Establishes connections between forecast elicitation and aggregation problems. Introduces "quasi-arithmetic (QA) pooling" associated with proper scoring rules.

## Key Findings

1. **QA Pooling Framework:** Maps expert forecasts and weights to a consensus forecast via quasi-arithmetic means associated with specific proper scoring rules.
   - Quadratic scoring rule → linear opinion pool (simple average)
   - Logarithmic scoring rule → logarithmic opinion pool (geometric mean of odds)

2. **Max-Min Optimality:** When a forecaster agent pays experts proportionally to their weights, using QA pooling maximizes worst-case profit across possible outcomes.

3. **Computational Efficiency:** The aggregator's score is concave relative to expert weights, enabling online gradient descent for learning weights with low regret.

4. **Axiomatic Characterization:** QA pooling satisfies natural axioms generalizing Kolmogorov's classical work on quasi-arithmetic means.

5. **Extremization justification:** The logarithmic pool (QA pooling with log score) naturally extremizes relative to the linear pool, providing theoretical grounding.

## Relevance to Prediction-Market Pricing

Provides the theoretical underpinning for why the geometric mean of odds (logarithmic pooling) is theoretically superior to simple averaging for combining probability forecasts. The max-min optimality guarantee makes this approach robust even in adversarial settings — relevant for combining predictions about Polymarket markets.
