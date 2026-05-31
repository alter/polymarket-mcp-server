---
title: "Future Is Unevenly Distributed: Forecasting Ability of LLMs Depends on What We're Asking"
url: "https://arxiv.org/abs/2511.18394"
source: "arxiv"
date: "2025-11-23"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Future Is Unevenly Distributed: Forecasting Ability of LLMs Depends on What We're Asking

**Authors:** Chinmay Karkar, Paras Chopra

**arXiv:** 2511.18394 | Submitted: November 23, 2025

## Summary

Investigates how LLM forecasting performance varies by question type and domain using ~10,000 forecasting questions from Polymarket, Metaculus, and Manifold Markets (January–July 2025). Key finding: LLM forecasting ability is highly variable — domain structure and question framing matter enormously.

## Key Methods

- ~10,000 forecasting questions from Polymarket, Metaculus, and Manifold Markets
- Period: January–July 2025
- Multiple LLM families tested
- Analysis by domain, question phrasing, and contextual information
- Investigation of how factual news context affects predictions

## Key Results

- Forecasting ability is highly variable — depends heavily on what and how you ask
- Some domains: LLMs perform well; others: systematic failure
- News context affects predictions non-uniformly
- Key pitfalls identified: logical leakage, unreliable news retrieval, data contamination from training cutoffs
- LLMs perform better on question types where they have strong base-rate knowledge

## Relevance to Polymarket Trading

Critical practical insight: not all market categories are equally tractable for LLM agents. Likely better on: Politics, Sports, Finance (structured). Likely worse on: Crypto micro-events, obscure niche events. Need category-specific calibration.
