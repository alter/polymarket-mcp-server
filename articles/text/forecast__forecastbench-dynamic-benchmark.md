---
title: "ForecastBench: A Dynamic Benchmark of AI Forecasting Capabilities"
url: https://arxiv.org/abs/2409.19839
source: arxiv
date: "2024-09-30"
type: paper
theme: forecast
lang: en
---

# ForecastBench: A Dynamic Benchmark of AI Forecasting Capabilities

**Authors:** Ezra Karger, Houtan Bastani, Chen Yueh-Han, Zachary Jacobs, Danny Halawi, Fred Zhang, Philip E. Tetlock
**Institution:** Forecasting Research Institute
**Submitted:** September 30, 2024; Last revised February 28, 2025 (v5)
**arXiv:** 2409.19839

## Abstract

ForecastBench is a dynamic benchmark that evaluates the accuracy of ML systems on an automatically generated and regularly updated set of 1,000 forecasting questions. The benchmark exclusively contains questions about future events with no known answer at submission time to prevent data leakage. Expert forecasters outperform the top-performing LLM (p-value <0.001).

## Introduction

Forecasting supports critical decision-making in economics, public health, and policy. Previous static benchmarks become obsolete as model knowledge cutoffs advance and face data contamination risks. ForecastBench addresses these limitations through continuous updates with future-event questions.

## Main Findings

- Median superforecaster Brier score: 0.093
- General public median Brier score: 0.107
- Top LLM (GPT-4-Turbo, Claude-3.5-Sonnet) Brier score: ~0.113
- Models with access to human crowd forecasts performed significantly better
- Performance gap widens substantially for combination questions
- Significant correlation between Arena scores and forecasting accuracy

## Methodology

**Benchmark design:**
- 1,000 questions biweekly from a continuously updated bank of 5,948 questions
- Questions drawn from nine sources: Metaculus, Polymarket, Manifold, ACLED, FRED, DBnomics, etc.
- Human forecasters answered 200-question subsets
- LLMs tested under six conditions: zero-shot prompting, scratchpad reasoning, inclusion of crowd forecasts, news retrieval, and model aggregation
- Evaluation via Brier scores against ground truth or community aggregates

**Key question types:** Binary (yes/no), as well as numeric and conditional questions

## Conclusion

Despite frontier models' advances on other benchmarks, they underperform humans in probabilistic forecasting. The dynamic benchmark enables ongoing capability tracking. LLMs will match superforecaster performance when Arena score approaches a certain threshold (extrapolated from current trends).

**Public leaderboard:** www.forecastbench.org

## Relevance to Prediction-Market Pricing

Establishes the current gap between LLM forecasting and human superforecasters. Provides a rigorous evaluation framework applicable to calibrating AI-driven prediction market strategies.
