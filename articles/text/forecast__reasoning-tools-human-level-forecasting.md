---
title: "Reasoning and Tools for Human-Level Forecasting"
url: https://arxiv.org/abs/2408.12036
source: arxiv
date: "2024-08-01"
type: paper
theme: forecast
lang: en
---

# Reasoning and Tools for Human-Level Forecasting

**Authors:** Elvis Hsieh, Preston Fu, Jonathan Chen (UC Berkeley)
**Date:** August 2024
**arXiv:** 2408.12036v1

## Abstract

Proposes RTF (Reasoning and Tools for Forecasting), a framework of reasoning-and-acting (ReAct) agents that dynamically retrieve updated information and run numerical simulations with equipped tools. Demonstrated competitive performance against human predictions on forecasting platforms.

## Introduction

Traditional language models struggle with "timely data updates" that cause predictions to shift substantially. The paper addresses judgmental forecasting (human expertise integrating historical data and intuition) as distinct from statistical time-series forecasting.

## Main Findings

1. **Hierarchical Planning:** A high-level planner handles abstract logic while low-level agents execute tasks using "Google API calling and Python simulation," enhancing efficiency and context window conservation.

2. **Performance Results:** RTF achieved a Brier score of 0.169 versus human crowd 0.172, with "superior accuracy (73.9% vs. 73.8%)" using ensemble methods.

3. **Key insight:** "Ensembles only contribute if each ensemble member is already sufficiently calibrated."

4. **Tool Integration:** Expands observation sources beyond Wikipedia to include Google Search API and Python interpreter, enabling real-time reasoning.

## Methodology

- ReAct agent framework with hierarchical planning
- Google Search API + Python interpreter for real-time data access
- Ensemble aggregation of multiple agent runs
- Benchmarked on Metaculus questions; compared against human crowd

## Conclusion

RTF "offers a robust tool for real-world decision-making" by advancing language models' abilities to reason and dynamically interact with current data. Smaller, carefully calibrated model ensembles prove more cost-effective than larger ones.

## Relevance to Prediction-Market Pricing

The calibration-first-then-ensemble principle directly applies to building AI forecasting systems for Polymarket. A Brier score of 0.169 (better than human crowd 0.172) demonstrates practical forecasting edge achievable with tool-augmented LLMs.
