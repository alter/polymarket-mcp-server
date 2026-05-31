---
title: "Leveraging Log Probabilities in Language Models to Forecast Future Events"
url: "https://arxiv.org/abs/2501.04880"
source: "arxiv"
date: "2025-01-08"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Leveraging Log Probabilities in Language Models to Forecast Future Events

**Authors:** Tommaso Soru, Jim Marshall

**arXiv:** 2501.04880 | Submitted: January 8, 2025

## Summary

Novel forecasting approach using LLM log probabilities (instead of sampled text) for multi-step probability estimation. Achieves Brier score 0.186 — 26% improvement over random, 19% improvement over existing AI systems.

## Key Methods

- Input: current trend data and trajectory information
- Log probability-based multi-step estimation for probability calculation
- 15 topic categories
- Uses LLM token-level probability outputs rather than generated text

## Key Results

- Brier score: 0.186
- 26% improvement over random prediction
- 19% improvement over other available AI systems (at time of publication)

## Relevance to Polymarket Trading

Log-probability approach is computationally efficient — no need for long chain-of-thought. Can generate fast probability estimates for many markets simultaneously. Useful for initial screening before deeper analysis on promising positions.
