---
title: "Automating Forecasting Question Generation and Resolution for AI Evaluation"
url: "https://arxiv.org/abs/2601.22444"
source: "arxiv"
date: "2026-01-30"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Automating Forecasting Question Generation and Resolution for AI Evaluation

**Authors:** Nikos I. Bosse, Peter Mühlbacher, Jack Wildman, Lawrence Phillips, Dan Schwarz

**arXiv:** 2601.22444 | Submitted: January 30, 2026

## Summary

Automated system using LLM-powered web research agents to generate and resolve forecasting questions at scale. Produces 1,499 diverse questions, demonstrating higher quality than Metaculus (96% verifiable/unambiguous vs. typical human-curated platforms). Resolves questions at 95% accuracy. Demonstrates that more capable LLMs perform better on generated questions.

## Key Methods

- LLM-powered web research agents for automated question generation
- Diverse, real-world forecasting question creation across multiple domains
- Resolution months after generation to avoid leakage
- Question decomposition strategy for improved forecasting performance

## Key Results

- 96% of generated questions verifiable and unambiguous (exceeds Metaculus)
- 95% resolution accuracy
- LLM performance by model:
  - Gemini 3 Pro: Brier score 0.134
  - GPT-5: Brier score 0.149
  - Gemini 2.5 Flash: Brier score 0.179
- Question decomposition strategy: Brier 0.141 → 0.132 improvement
- Better LLMs consistently perform better (validates benchmark utility)

## Relevance to Polymarket Trading

Automated question generation at scale enables large training datasets without manual curation. The 96% quality rate exceeds human-curated platforms. Can be adapted to automatically generate Polymarket-style training data from news.
