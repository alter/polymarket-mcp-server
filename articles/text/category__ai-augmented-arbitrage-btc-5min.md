---
title: "AI-Augmented Arbitrage in Short-Duration Prediction Markets: Live Trading Analysis of Polymarket's 5-Minute Bitcoin Binary Options"
url: "https://medium.com/@gwrx2005/ai-augmented-arbitrage-in-short-duration-prediction-markets-live-trading-analysis-of-polymarkets-8ce1b8c5f362"
source: "medium"
date: "2026"
type: "blog"
theme: "category"
lang: "en"
---

# AI-Augmented Arbitrage in Polymarket 5-Minute Bitcoin Binary Options

## Core System Architecture

The researchers deployed an automated trading agent combining momentum analysis with an LLM filter called "OpenClaw." The system operated as a Docker cluster with three services managing trade execution, monitoring, and data distribution across the Polygon blockchain.

## Trading Strategy

The approach centered on detecting "DISLOCATION" signals where Bitcoin's price movement outpaced Polymarket token price adjustments. The system computed weighted momentum across four timeframes (30s, 60s, 120s, 240s), with evolving weight distributions. Early configuration allocated "65% of momentum weight to the last 60 seconds," causing micro-bounces to dominate predictions in larger downtrends.

## Key Results

**Session 1 (v2 Engine):** Starting with ~$17 USDC.e, the system lost "$15.47 (-49.5% ROI)" across 15 trades, with an 80% bias toward UP predictions during a DOWN market. Critically, they discovered their outcome resolver used spot prices rather than official Polymarket resolution prices, creating false feedback.

**Session 2 (v3 Engine):** With $31.19 starting capital and a 10-minute trend filter, losses dropped to "$4.18" across 4 trades. This represented a "7× improvement in capital preservation."

## Critical Findings

The paper documents a dramatic paper-to-live performance gap: "paper trading assumed perfect fills at quoted mid-prices with zero market impact, while live execution exhibited 2–4 cent slippage." Their measured win rates of 25–27% fell well below the ~53% breakeven threshold, suggesting these ultra-short markets approximate random walks.

## LLM Integration Value

OpenClaw provided trend-aware signal filtering and structured reasoning via chain-of-thought prompting, but couldn't overcome fundamentally flawed input data. The mechanical trend filter proved more valuable than AI judgment operating on biased signals.

## Key Lesson

These 5-minute crypto binary markets on Polymarket approximate random walks. Paper-to-live gaps are severe due to slippage. The ~53% breakeven threshold is difficult to exceed consistently at these timescales.
