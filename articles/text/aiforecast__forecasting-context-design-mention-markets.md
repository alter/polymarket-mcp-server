---
title: "Forecasting Future Language: Context Design for Mention Markets"
url: "https://arxiv.org/abs/2602.21229"
source: "arxiv"
date: "2026-02-01"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Forecasting Future Language: Context Design for Mention Markets

**Authors:** Sumin Kim, Jihoon Kwon, Yoon Kim, Nicole Kagan, Raffi Khatchadourian, Wonbin Ahn, Alejandro Lopez-Lira, Jaewon Lee, Yoontae Hwang, Oscar Levy, Yongjae Lee, Chanyeol Choi

**arXiv:** 2602.21229

## Summary

Studies "mention markets" — prediction markets where contracts resolve based on whether a keyword is mentioned in a future public event (e.g., earnings calls). Tests how LLMs can forecast these, focusing on context design and Market-Conditioned Prompting (MCP).

## Key Methods

- Market-Conditioned Prompting (MCP): treat market-implied probability as prior; instruct LLM to update it using textual evidence
- MixMCP: combine market probability with LLM posterior
- Context variants: news only, prior transcripts only, combined
- Evaluation on earnings call keyword mention prediction

## Key Results

1. Enriched context consistently enhances forecasting performance
2. MCP produces better-calibrated forecasts than unconditioned LLM
3. MixMCP outperforms both market baseline and LLM alone by dampening LLM posterior update with market prior
- Formula: ~67% market weight + 33% LLM update

## Relevance to Polymarket Trading

MCP is immediately deployable: use Polymarket's current price as prior, feed LLM the news context, get calibrated update. The ~67/33 weighting (matching AIA findings) appears robust across different paper types. This pattern is the consensus architecture.
