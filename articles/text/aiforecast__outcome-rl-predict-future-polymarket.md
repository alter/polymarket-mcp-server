---
title: "Outcome-based Reinforcement Learning to Predict the Future"
url: "https://arxiv.org/abs/2505.17989"
source: "arxiv"
date: "2025-05-23"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Outcome-based Reinforcement Learning to Predict the Future

**Authors:** Benjamin Turtel, Danny Franklin, Kris Skotheim, Luke Hewitt, Philipp Schoenegger

**arXiv:** 2505.17989 | Published in: Transactions on Machine Learning Research (11/2025)

## Abstract

Reinforcement Learning with Verifiable Rewards (RLVR) has been an effective approach for improving Large Language Models' reasoning in domains such as coding and mathematics. Here, we apply RLVR methods towards forecasting future real-world events — a challenging task for RL due to the very noisy (and delayed) outcomes involved. Using a novel dataset of recent questions from a prediction market and accompanying relevant news headlines, we show that a compact (14B) reasoning model can be trained to match or surpass the predictive accuracy of frontier models like o1, while greatly improving probabilistic calibration.

## Key Methods

- RLVR (GRPO and ReMax) applied to forecasting
- Adaptations: remove per-question variance scaling in GRPO; baseline-subtracted advantages in ReMax
- Training dataset: ~110K questions (10K from Polymarket + 100K synthetically generated)
- Input: TRUE/FALSE question + set of news articles published before prediction date
- Guardrails: penalize gibberish, non-English, missing rationales
- Median prediction sampling during inference (7-run ensemble, best performer)

## Key Results

- 14B model matches or surpasses frontier model o1 accuracy
- Substantially improved probabilistic calibration vs. frontier models
- Trading simulation ROI: >10% across test questions
- Best model: 7-run ReMax ensemble
- Lower market confidence questions: ~20% ROI in simulation

## Relevance to Polymarket Trading

Proof that a 14B RLVR-trained model beats o1 on calibration AND generates 10-20% ROI in Polymarket simulations. The 100K synthetic question augmentation is critical for training stability. ReMax > GRPO for this domain.
