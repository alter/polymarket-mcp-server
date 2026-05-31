---
title: "Unlocking the Forecasting Economy: A Suite of Datasets for the Full Lifecycle of Prediction Markets"
url: https://arxiv.org/abs/2604.20421
source: arxiv
date: "2026-04-01"
type: paper
theme: sports
lang: en
---

# Unlocking the Forecasting Economy: Prediction Market Lifecycle Datasets

**arXiv:** 2604.20421v1 | April 2026

## Dataset Scale

Comprehensive Polymarket database (October 2020 – March 2026):
- **770,880** market records
- **943,548,464** OrderFilled records (fill-level trades)
- **1,988,150** oracle-resolution events
- **2,492,419** distinct trader addresses

## NBA Sports Markets Analysis

### Scope
- 4,332 candidate NBA markets → 4,103 clean markets (single-game binary winner outcomes)
- Excluded: MVP, spread, totals, player props, quarter/halftime markets

### Calibration Findings — Key Result

Pre-game Polymarket prices demonstrate **strong alignment with actual outcomes**:

| Metric | Value |
|---|---|
| Brier Score | 0.20339 |
| LogLoss | 0.59180 |
| Expected Calibration Error | 0.02745 |
| Max Calibration Error | 0.08450 |

"Isotonic calibration brings limited value" — markets already close to calibrated.

"Predicted pre-game probabilities are broadly consistent with realized win frequencies" — calibration curve near the 45-degree line.

## Interpretation

Polymarket NBA winner markets are **efficiently priced** at pre-game open. The implication: to find edge in NBA prediction markets, you need to either:
1. Find signals the market hasn't yet incorporated (injury news, lineup changes arriving late)
2. Trade in-play when market updates lag real-time events
3. Exploit the favourite-longshot bias at extreme price ranges
4. Look at less liquid sports/markets where information efficiency is lower

## Technical Architecture

- Continuous synchronization (not one-time snapshots)
- Bridge-layer entity resolution: **99.40%** oracle-event linkage
- On-chain recovery improved token resolution from 18.75% → 100%

## CPI Macro Application (non-sports)

Reconstructed continuous inflation estimates from discrete bucket markets — Polymarket estimates "often closer to realized CPI than external forecast benchmark."

## Code/Data Availability

Pipeline designed with resumable execution and checkpoint-based recovery — data publicly accessible.
