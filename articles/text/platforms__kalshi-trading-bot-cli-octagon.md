---
title: "OctagonAI Kalshi Trading Bot CLI — AI-Native CLI with Kelly Sizing & 5-Gate Risk Engine"
url: "https://github.com/OctagonAI/kalshi-trading-bot-cli"
source: github
date: "2026"
type: tool
theme: platforms
lang: en
---

# OctagonAI Kalshi Trading Bot CLI

## Strategy Description
AI-native trading system for Kalshi. Core approach:
1. **Research & Probability Estimation**: Deep fundamental research via Octagon Research API → independent probability estimates distinct from market pricing
2. **Edge Detection**: Edge = model probability minus live order book price
3. **Trade Execution**: Orders placed only when edge criteria are met

## Kelly Sizing Details
- **Default Multiplier**: 0.5 (half-Kelly for conservative positioning)
- **Configurable**: `risk.kelly_multiplier` setting (0–1 range)
- **Bankroll Integration**: Sizing incorporates total bankroll at command execution
- **Per-Leg Application**: Individual contract position sizes reflect Kelly calculation relative to edge size and probability estimates

Example: `kalshi basket size --bankroll 1000 --kelly 0.25`

## 5-Gate Risk Engine
Sequential filters before ANY trade executes:
1. **Kelly Gate**: Verifies position size aligns with fractional Kelly calculation
2. **Liquidity Gate**: Confirms sufficient order book volume
3. **Correlation Gate**: Prevents excessive concentration in correlated markets
4. **Concentration Gate**: Limits exposure per category/theme
5. **Drawdown Gate**: Blocks trading if max drawdown threshold exceeded (default 20%)

Additional parameters:
- Daily loss limit: $200 (configurable)
- Max concurrent positions: 10
- Max positions per category: 3

## Technology Stack
- Language: TypeScript
- Runtime: Bun (≥1.1)
- Database: SQLite (`bun:sqlite`) for caching and analytics
- External: Kalshi Exchange API, Octagon Research API
- Optional: Tavily (web research), multiple LLM providers (OpenAI, Anthropic, Google, xAI, OpenRouter, Ollama)

## Command Categories
- **Discovery**: search, similar, clusters, peers, correlate
- **Portfolio**: basket (build, backtest, size, validate), portfolio, watch
- **Analysis**: analyze, backtest, events, catalysts, series
- **Execution**: buy, sell, cancel
- **Configuration**: setup, init, config, themes

All commands support `--json` flag for agent orchestration.
