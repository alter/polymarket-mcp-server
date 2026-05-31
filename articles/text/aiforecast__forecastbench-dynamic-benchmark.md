---
title: "ForecastBench: A Dynamic Benchmark of AI Forecasting Capabilities"
url: "https://www.forecastbench.org/docs/"
source: "forecastbench.org"
date: "2025-01-01"
type: "benchmark"
theme: "aiforecast"
lang: "en"
---

# ForecastBench: A Dynamic Benchmark of AI Forecasting Capabilities

**Organization:** Forecasting Research Institute (FRI) / Wharton School, UPenn

**URL:** https://www.forecastbench.org

**Paper:** https://faculty.wharton.upenn.edu/wp-content/uploads/2026/02/ForecastBench_A_Dynamic_.pdf (accepted ICLR 2025)

## Overview

ForecastBench is the primary dynamic benchmark for LLM forecasting ability, running since 2024. Refreshed daily; new forecasting rounds every 2 weeks (1,000 LLM questions / 200 human questions per round).

## Scoring

- **Metric (as of March 2026):** Brier Index = (1 − √Brier score) × 100%
  - 100% = perfect; 50% = always predict 50%; 0% = maximally wrong
- Previously: difficulty-adjusted Brier score
- Rankings stabilize within ~50 days of new model participation

## Current Performance (as of early 2026)

- Human superforecasters: Brier score ~0.081 (leaderboard top)
- Best LLM: GPT-4.5 — Brier score 0.101
- LLM improvement rate: ~0.016 Brier points/year
- Projected LLM–superforecaster parity (Brier Index): May 2027 (95% CI: April 2026 – May 2028)
- Market consensus: ranked #22 (was #2 a year prior) — rapidly surpassed by LLMs

## Categories

Dataset questions + Market questions (from prediction markets including Polymarket, Kalshi, Metaculus)

## Relevance to Polymarket Trading

The authoritative leaderboard for evaluating which LLM to use as a forecasting core. GPT-4.5 and Gemini 3 Pro are currently top performers. Use Brier Index for model selection decisions. Market-conditioned LLM ensembles consistently outperform either component alone.
