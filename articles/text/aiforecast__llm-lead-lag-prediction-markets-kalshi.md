---
title: "LLM as a Risk Manager: LLM Semantic Filtering for Lead-Lag Trading in Prediction Markets"
url: "https://arxiv.org/abs/2602.07048"
source: "arxiv"
date: "2026-02-01"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# LLM as a Risk Manager: LLM Semantic Filtering for Lead-Lag Trading in Prediction Markets

**Authors:** Sumin Kim, Minjae Kim, Jihoon Kwon, Yoon Kim, Nicole Kagan, Joo Won Lee, Oscar Levy, Alejandro Lopez-Lira, Yongjae Lee, Chanyeol Choi

**arXiv:** 2602.07048 | Submitted: February 2026

## Abstract

Prediction markets provide a unique setting where event-level time series are directly tied to natural-language descriptions, yet discovering robust lead-lag relationships remains challenging due to spurious statistical correlations. We propose a hybrid two-stage causal screener to address this challenge: (i) a statistical stage that uses Granger causality to identify candidate leader-follower pairs from market-implied probability time series, and (ii) an LLM-based semantic stage that re-ranks these candidates by assessing whether the proposed direction admits a plausible economic transmission mechanism based on event descriptions. Because causal ground truth is unobserved, we evaluate the ranked pairs using a fixed, signal-triggered trading protocol that maps relationship quality into realized profit and loss (PnL). On Kalshi Economics markets, our hybrid approach consistently outperforms the statistical baseline. Across rolling evaluations, the win rate increases from 51.4% to 54.5%. Crucially, the average magnitude of losing trades decreases substantially from 649 USD to 347 USD. This reduction is driven by the LLM's ability to filter out statistically fragile links that are prone to large losses, rather than relying on rare gains. These improvements remain stable across different trading configurations, indicating that the gains are not driven by specific parameter choices. Overall, the results suggest that LLMs function as semantic risk managers on top of statistical discovery, prioritizing lead-lag relationships that generalize under changing market conditions.

## Key Methods

- Stage 1: Granger causality to identify leader-follower market pairs from probability time series
- Stage 2: LLM semantic filter — evaluates plausibility of economic transmission mechanism
- Dataset: Kalshi Economics markets, Oct 2021–Nov 2025, 554 active markets
- Rolling-window protocol: 60-day train, 30-day test, 18 non-overlapping test periods
- Signal-triggered trading protocol mapping relationship quality to PnL

## Key Results

- Win rate: 51.4% → 54.5% (hybrid vs. pure Granger)
- Average losing trade: $649 → $347 (46.5% reduction)
- Gains persist across all holding horizons (1–21 days)
- LLM filter most effective during large leader price moves (>10pt: +17.6pp win rate)
- PnL more than doubles at 7-day holding horizon

## Relevance to Polymarket Trading

Directly translatable: Polymarket markets have correlated outcomes (e.g., political events). Use LLM semantic filter on Granger-identified pairs to reduce false positives and downside risk.
