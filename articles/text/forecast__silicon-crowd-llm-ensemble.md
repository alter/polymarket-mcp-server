---
title: "Wisdom of the Silicon Crowd: LLM Ensemble Prediction Capabilities Rival Human Crowd Accuracy"
url: https://arxiv.org/abs/2402.19379
source: arxiv
date: "2024-02-29"
type: paper
theme: forecast
lang: en
---

# Wisdom of the Silicon Crowd: LLM Ensemble Prediction Capabilities Rival Human Crowd Accuracy

**Authors:** Philipp Schoenegger (LSE), Indre Tuminauskaite (Independent), Peter S. Park (MIT), Philip E. Tetlock (UPenn)
**Submitted:** February 29, 2024; revised July 22, 2024 (v6)
**Published:** Science Advances, November 8, 2024
**arXiv:** 2402.19379

## Abstract

The researchers demonstrate that aggregated LLM predictions on 31 binary questions matched the accuracy of 925 human forecasters from a real tournament. They tested twelve different language models and found that when combined through ensemble methods, they achieved performance equivalent to human crowd forecasting. LLM accuracy improved 17–28% when exposed to median human predictions.

## Introduction

Individual frontier LLMs previously underperformed compared to human crowd forecasting tournaments. The hypothesis: the "wisdom of the crowd" effect — where aggregated predictions outperform individuals — may also apply to machine ensembles. This paper tests whether LLMs can generalize beyond training data by predicting real-world future events.

## Main Findings

**Study 1 Results:**
- LLM ensemble achieved mean Brier score of 0.20 (SD=0.12), significantly outperforming the 50% baseline (p=0.026)
- LLM crowd performance (M=0.20) was statistically equivalent to the human crowd (M=0.19, p=0.850)
- Equivalence testing confirmed the groups matched with Cohen's d=0.1
- Models demonstrated "acquiescence bias": predicting above 50% despite roughly even resolution rates

**Study 2 Results:**
- GPT-4 improved from Brier score 0.17 to 0.14 after human exposure (p=0.003)
- Claude 2 improved from 0.221 to 0.15 (p<0.001)
- Prediction intervals narrowed significantly for both models
- Strong correlation between initial deviation from human median and forecast adjustment magnitude (r≈0.87–0.88)

## Methodology

**Study 1:** Twelve diverse models queried (GPT-4, Claude 2, Llama-2-70B, Mistral-7B, and others). Each received three independent queries using prompts instructing them to "respond as a superforecaster." Median forecasts were aggregated across models. Questions drawn from Metaculus platform (October 2023–January 2024).

**Study 2:** GPT-4 and Claude 2 received initial forecasts, then updated predictions after exposure to human crowd medians using elaborated prompts based on superforecasting principles.

Evaluation: Brier scores (strictly proper scoring rule), calibration analysis via Murphy Decomposition, Benjamini-Hochberg correction for multiple comparisons.

## Conclusion

"LLM prediction capabilities can match the gold standard of the human crowd tournament method" through ensemble aggregation, demonstrating a "wisdom of the silicon crowd" effect. Models exhibited poor calibration and overconfidence, suggesting room for improvement through targeted calibration training.

## Relevance to Prediction-Market Pricing

LLM ensembles can generate calibrated prior probability estimates for prediction market events. The 17–28% accuracy improvement from human feedback suggests hybrid human+LLM approaches are superior to either alone.
