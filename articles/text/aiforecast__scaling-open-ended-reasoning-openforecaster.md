---
title: "Scaling Open-Ended Reasoning to Predict the Future"
url: "https://arxiv.org/abs/2512.25070"
source: "arxiv"
date: "2025-12-31"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Scaling Open-Ended Reasoning to Predict the Future

**Authors:** Nikhil Chandak, Shashwat Goel, Ameya Prabhu, Moritz Hardt, Jonas Geiping (Max Planck Institute for Intelligent Systems, ELLIS Institute Tübingen, Tübingen AI Center, University of Tübingen)

**arXiv:** 2512.25070 | Submitted: December 2025

## Summary

Trains language models to make predictions on open-ended forecasting questions by synthesizing novel forecasting questions from daily news. Releases OpenForesight (52,000 training questions) and OpenForecaster 8B (fine-tuned Qwen3-8B via GRPO reinforcement learning).

## Key Methods

- Automated question synthesis from global news events using offline news corpus (no future leakage)
- Training: Qwen3 thinking models on OpenForesight dataset
- RL: Group Relative Policy Optimization (GRPO) with improved reward function
- Retrieval-augmented inference at test time using offline news snapshots
- Held-out test: May–August 2025 events

## Key Results

- OpenForecaster 8B outperforms GPT OSS 120B on Brier score
- Competitive with 100B+ models on OpenForesight Test Set (302 questions, May–Aug 2025) and FutureX (86 questions, Jul–Aug 2025)
- Calibration improvements generalize across popular benchmarks
- All models, code, and data open-sourced
- GitHub: https://github.com/OpenForecaster/scaling-forecasting-training
- Project: https://openforecaster.github.io/

## Relevance to Polymarket Trading

52K training questions + open GRPO training code = practical path to building a Polymarket-specific forecasting model. OpenForecaster 8B beats 120B models — viable for production use. RAG with news corpus is key to avoiding leakage.
