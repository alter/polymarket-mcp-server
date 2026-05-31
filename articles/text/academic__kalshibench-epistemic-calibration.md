---
title: "Do Large Language Models Know What They Don't Know? KalshiBench: A New Benchmark for Evaluating Epistemic Calibration via Prediction Markets"
url: "https://arxiv.org/abs/2512.16030"
source: "arxiv"
date: "2025-12-01"
type: "paper"
theme: "academic"
lang: "en"
authors: ["Lukas Nel"]
---

# KalshiBench: A New Benchmark for Evaluating Epistemic Calibration via Prediction Markets

## Abstract

The research examines whether large language models can accurately express confidence levels matching their actual performance. The author introduces KalshiBench, a benchmark using 300 prediction market questions from Kalshi with real-world outcomes occurring after model training cutoffs. This approach evaluates models on genuinely uncertain future events rather than static knowledge.

## Key Findings

1. **Systematic Overconfidence:** Testing five frontier models revealed systematic overconfidence across all models, with Claude Opus 4.5 showing the best calibration (ECE=0.120).

2. **Reasoning Doesn't Help:** Notably, reasoning-enhanced models like GPT-5.2-XHigh demonstrated worse calibration scores despite comparable accuracy levels.

3. **Poor Performance:** Most models performed worse than simply predicting base rates, with only one achieving a positive Brier Skill Score.

4. **Distinct Capability:** The findings suggest epistemic calibration requires targeted development, as scaling and enhanced reasoning do not automatically confer calibration benefits.
