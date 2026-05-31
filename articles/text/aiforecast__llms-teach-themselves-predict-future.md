---
title: "LLMs Can Teach Themselves to Better Predict the Future"
url: "https://arxiv.org/abs/2502.05253"
source: "arxiv"
date: "2025-02-07"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# LLMs Can Teach Themselves to Better Predict the Future

**Authors:** Benjamin Turtel, Danny Franklin, Philipp Schoenegger

**arXiv:** 2502.05253 | Submitted: February 7, 2025

## Abstract

We present an outcome-driven fine-tuning framework that enhances the forecasting capabilities of large language models (LLMs) without relying on human-curated reasoning samples. The approach uses model self-play to generate diverse reasoning trajectories and probabilistic forecasts on questions resolved after the models' training cutoff, ranks reasoning traces by proximity to actual outcomes, and fine-tunes via Direct Preference Optimization (DPO).

## Key Methods

- Self-play: model generates multiple reasoning traces + probability estimates per question
- Ranking: traces ranked by proximity to resolved outcome (e.g., predicting 5% beats 10% when answer is NO)
- Fine-tuning: Direct Preference Optimization (DPO) on ranked traces
- Dataset: 12,100 binary outcome questions from Polymarket
  - Training: 9,800 questions resolved July 1 – December 15, 2024
  - Test: 2,300 questions resolved December 25, 2024 – January 23, 2025

## Key Results

- Accuracy improvement: +7-10% for Phi-4 14B and DeepSeek-R1 14B
- Fine-tuned 14B models match forecasting capability of much larger frontier models (GPT-4o)
- No human-curated reasoning samples required
- Polymarket data sufficient for self-supervised calibration training

## Relevance to Polymarket Trading

This is the blueprint for fine-tuning a small open-source model (14B) on Polymarket historical data to approach GPT-4o forecasting quality at much lower inference cost. The DPO approach on self-generated traces is practical without human labeling.
