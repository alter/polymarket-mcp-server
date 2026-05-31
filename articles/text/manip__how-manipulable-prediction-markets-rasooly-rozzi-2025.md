---
title: "How manipulable are prediction markets?"
url: "https://arxiv.org/abs/2503.03312"
source: arXiv (econ.GN)
date: "2025-03-05"
type: academic_paper
theme: manip
lang: en
---

# How Manipulable Are Prediction Markets?

**Authors:** Itzhak Rasooly (Sciences Po Paris), Roberto Rozzi (University of Siena)  
**arXiv:** 2503.03312  
**Submitted:** March 5, 2025  
**PDF:** https://arxiv.org/pdf/2503.03312

## Abstract

Large-scale field experiment testing whether prediction markets can be successfully manipulated. Researchers randomly shocked prices across **817 separate markets** on the Manifold platform, tracked hourly price data, and measured whether effects persisted up to 60 days. Builds the first model tracing the price path in a prediction market following a manipulation attempt.

## Methodology

- **Platform:** Manifold (Maniswap / constant-product AMM), ~10,000 active users at time of experiment
- **Trades placed:** December 2024 – February 2025
- **Design:** Randomly assign markets to "yes bet" or "no bet" shocks of ~5 percentage points
- **Measurement:** ~600,000 hourly price observations over 30 days; 60-day snapshots
- **Validation:** Sweepcash (real-money) sub-experiment for robustness

## Key Findings

1. **Manipulation persists:** Effects of random trades remain visible even **60 days** after execution
2. **Partial reversion:** Prices revert approximately 40% toward baseline under baseline conditions—never fully
3. **Market heterogeneity:** Manipulation harder in markets with:
   - More active traders
   - Greater trading volume
   - External probability estimate (e.g., duplicated on Metaculus)
4. **Theoretical model:** Active markets run on a faster "clock speed" and revert more quickly; external signals reduce reliance on internal price as a belief anchor

## Theoretical Framework

Based on constant product rule (Maniswap). Three theoretical predictions confirmed by experiment:
- Manipulation can affect prices both short- and long-term
- Behavioral responses generate some price reversion
- Effects vary systematically with market structure

## Context

Paper was motivated by prominent 2024 US election concerns about prediction market manipulation (Vox, FT, WSJ, Bloomberg, NYT coverage). Platform Manifold announced discontinuation of Sweepcash on Feb 13, 2025, cutting the experiment short on some markets.

## Key Implication

Even small prediction markets can be moved by modest cash outlays. Larger, deeper, externally-anchored markets are more resilient—but not immune. This has direct regulatory implications for election-adjacent markets.

## Links

- arXiv abstract: https://arxiv.org/abs/2503.03312
- SciencesPo HAL: https://sciencespo.hal.science/hal-05022889v1/file/2025_itzhak_rasooly_and_roberto_rozzi_how_manipulable_are_prediction_markets.pdf
