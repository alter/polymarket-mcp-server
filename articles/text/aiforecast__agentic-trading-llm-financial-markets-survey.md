---
title: "Agentic Trading: When LLM Agents Meet Financial Markets"
url: "https://arxiv.org/abs/2605.19337"
source: "arxiv"
date: "2026-05-27"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Agentic Trading: When LLM Agents Meet Financial Markets

**Authors:** Yihan Xia, Panpan You, Taotao Wang, Fang Liu, Han Qi, Xiaoxiao Wu, Shengli Zhang

**arXiv:** 2605.19337 | Submitted: May 2026

## Summary

Systematic audit of 77 LLM trading agent studies through March 2026. Uses Architecture-Capability-Adaptation framework. Critical finding: the field has severe reproducibility and protocol problems — only 2/19 core studies employ time-consistent split protocols; none achieve R3 reproducibility.

## Key Methods

- Evidence mapping of 77 studies
- 19 core studies meeting minimum empirical criteria
- Reproducibility classification: R0–R3 framework
- Protocol coding: time-consistent splits, transaction-cost modeling, survivorship bias
- Architecture-Capability-Adaptation analytical lens

## Key Results

- Only 2/19 studies use extractable time-consistent split protocols
- Only 1/19 includes explicit transaction-cost modeling
- None achieve R3 reproducibility standards
- LLM memory risk: "outdated priors" — parametric knowledge becomes stale as market regimes change
- Field has architectural expansion but severe evaluation protocol problems

## Relevance to Polymarket Trading

Critical methodological warning: most published LLM trading performance results are not reproducible. When evaluating strategies, require: (1) time-consistent splits, (2) transaction cost modeling (Polymarket has ~2% spread), (3) explicit regime tagging. Parametric memory staleness is a real risk for news-based LLM agents.
