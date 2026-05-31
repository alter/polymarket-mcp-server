---
title: "Agentic Forecasting using Sequential Bayesian Updating of Linguistic Beliefs"
url: "https://arxiv.org/abs/2604.18576"
source: "arxiv"
date: "2026-04-20"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Agentic Forecasting using Sequential Bayesian Updating of Linguistic Beliefs

**Author:** Kevin Murphy (University of British Columbia)

**arXiv:** 2604.18576 | Submitted: April 20, 2026

## Abstract

We present the Bayesian Linguistic Forecaster (BLF), an agentic system for binary forecasting that achieves state-of-the-art performance on the ForecastBench benchmark. The system is built on three ideas. (1) Linguistic belief state: a semi-structured representation combining numerical probability estimates with natural-language evidence summaries, updated by the LLM at each step of an iterative tool-use loop. This contrasts with the common approach of appending all retrieved evidence to an ever-growing, unstructured context. (2) Hierarchical multi-trial aggregation: running K independent trials and combining them using logit-space averaging shrinkage with a data-dependent prior. (3) Hierarchical calibration: Platt scaling with a hierarchical prior, which avoids over-shrinking extreme predictions for sources with skewed base rates. On 400 questions from the ForecastBench leaderboard, BLF outperforms all the top public methods, including Cassi, GPT-5, Grok 4.20, and Foresight-32B. Careful ablation studies, using mixed effects analysis to control for question variability (which accounts for 62% of the variance in performance), reveals that all 3 components contribute to the overall gains, but some components matter more than others, depending on the base LLM, and the setting (e.g. with or without a crowd prior). All our experiments are based on a robust back-testing framework which we develop, which has a leakage rate below 1.5%, and may be of independent interest.

## Key Methods

1. **Linguistic Belief State:** Semi-structured representation = probability + natural-language evidence summary; updated each iteration (approximate sequential Bayesian inference)
2. **Hierarchical Multi-Trial Aggregation:** K independent trials, logit-space shrinkage with data-dependent prior
3. **Hierarchical Calibration:** Platt scaling with hierarchical prior; avoids over-shrinking extreme predictions

- Leverages web search and tool use
- Backtesting framework with <1.5% leakage rate
- Mixed effects analysis to control for question variability (62% of variance)

## Key Results

- SOTA on ForecastBench (400 questions): outperforms Cassi, GPT-5, Grok 4.20, Foresight-32B
- All 3 components contribute per ablation study
- Backtesting framework with <1.5% temporal leakage

## Relevance to Polymarket Trading

The linguistic belief state approach (maintain compressed evidence + probability, update sequentially) is the right architecture for any LLM-based Polymarket trading agent. Avoids context overflow. Hierarchical calibration is critical for skewed base rates (many Polymarket markets resolve near 0 or 1).
