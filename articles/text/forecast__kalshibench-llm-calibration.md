---
title: "KalshiBench: A New Benchmark for Evaluating Epistemic Calibration via Prediction Markets"
url: https://arxiv.org/abs/2512.16030
source: arxiv
date: "2025-12-17"
type: paper
theme: forecast
lang: en
---

# Do Large Language Models Know What They Don't Know? KalshiBench: A New Benchmark for Evaluating Epistemic Calibration via Prediction Markets

**Author:** Lukas Nel
**Submitted:** December 17, 2025
**arXiv:** 2512.16030

## Abstract

Introduces KalshiBench, comprising 300 prediction market questions from Kalshi (CFTC-regulated exchange) with real-world outcomes occurring after model training cutoffs. Assesses whether LLMs can appropriately quantify uncertainty regarding genuinely unknown future events, rather than merely measuring static knowledge accuracy.

## Key Findings

1. **Systematic Overconfidence:** All five evaluated models demonstrated "systematic overconfidence across all models" — Claude Opus 4.5, GPT-5.2, DeepSeek-V3.2, Qwen3-235B, and Kimi-K2.

2. **Calibration Performance:**
   - Claude Opus 4.5: best calibration (ECE = 0.120)
   - GPT-5.2-XHigh (reasoning-enhanced): worse calibration (ECE = 0.395) despite comparable accuracy
   - Superforecasters: ECE ≈ 0.03–0.05 (dramatically better)

3. **Baseline Comparison:** Only one model achieved a positive Brier Skill Score, meaning most models "perform worse than simply predicting base rates."

4. **Scaling Limitations:** "Scaling and enhanced reasoning do not automatically confer calibration benefits." Epistemic calibration requires targeted development.

## Dataset

- 300 questions from 1,531-question KalshiBench dataset
- Real outcomes post-training cutoff (no data leakage)
- Questions span politics, economics, finance, sports

## Relevance to Prediction-Market Pricing

Confirms that even frontier LLMs are systematically overconfident and perform below base rates on average. Any AI-assisted Polymarket system must include explicit probability calibration post-processing. The ECE gap between LLMs (0.12) and superforecasters (0.03–0.05) quantifies the calibration improvement target.
