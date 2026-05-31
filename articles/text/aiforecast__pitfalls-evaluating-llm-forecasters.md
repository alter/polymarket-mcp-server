---
title: "Pitfalls in Evaluating Language Model Forecasters"
url: "https://arxiv.org/abs/2506.00723"
source: "arxiv"
date: "2025-05-31"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Pitfalls in Evaluating Language Model Forecasters

**Authors:** Daniel Paleka, Shashwat Goel, Jonas Geiping, Florian Tramèr (ETH Zurich and affiliates)

**arXiv:** 2506.00723 | Submitted: May 31, 2025

## Summary

Critical analysis of LLM forecasting evaluation methodology. Identifies two primary categories of evaluation failures that render many published performance claims untrustworthy.

## Key Methods

- Systematic analysis of existing LLM forecasting papers
- Concrete examples of evaluation failures from prior work
- Identification of temporal leakage patterns

## Key Findings

Two primary problem categories:

1. **Temporal leakage issues (trust problem):**
   - Training data contamination (events in pre-cutoff data)
   - Retrieval leakage (retrieved documents contain post-question-date information)
   - Evaluation design leakage (question resolution info leaking into context)

2. **Generalization failures (extrapolation problem):**
   - Benchmark performance ≠ real-world forecasting ability
   - Static question sets become contaminated over time
   - Distribution shift between evaluation questions and real market questions

## Relevance to Polymarket Trading

Essential checklist before trusting any LLM forecasting result:
- [ ] Is training data cutoff before question creation date?
- [ ] Are retrieved news articles date-filtered to before question date?
- [ ] Are evaluation questions from after training cutoff?
- [ ] Are there explicit decontamination checks?
Any paper lacking these controls should be discounted. Applies to internal backtesting: never use LLM knowledge of events that occurred after the "as-of" date of a market snapshot.
