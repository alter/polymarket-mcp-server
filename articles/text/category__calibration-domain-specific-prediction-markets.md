---
title: "Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets"
url: "https://arxiv.org/html/2602.19520v1"
source: "arxiv"
date: "2026-02"
type: "academic_paper"
theme: "category"
lang: "en"
---

# Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets

Le, N.A. (2026). arXiv:2602.19520

## Core Findings

This research examines whether prediction market prices accurately reflect true probabilities across different domains. Using 292 million trades from Kalshi and Polymarket, the author identifies four calibration components explaining 87.3% of variance:

**Universal horizon effect**: "All domains share a tendency toward underconfidence, with prices compressed toward 50%, at long time horizons." Slopes rise from 0.99 (within one hour) to 1.32 (beyond one month).

**Domain-specific biases**: Politics exhibits persistent underconfidence (+0.15), while Weather and Entertainment show overconfidence (−0.09 each). Sports and Crypto cluster near perfect calibration.

**Domain-by-horizon interactions**: Each domain follows a distinct calibration trajectory. Political markets remain underconfident across nearly all horizons. Sports markets calibrate well short-term but become underconfident beyond one month. Weather markets over-react to signals at short horizons before converging to underconfidence.

**Trade-size scale effect**: On Kalshi, large political trades (100+ contracts) produce slopes of 1.74 versus 1.19 for single trades—a gap of 0.53. This effect vanishes on Polymarket, indicating platform-specific microstructure rather than universal behavior.

## Weather Markets

Weather markets are uniquely overconfident at short horizons — the only domain where prices are too extreme — likely reflecting over-reaction to meteorological signals. When a forecast predicts a storm tomorrow, traders push prices too far, overshooting what climatological base rates would justify. Short-horizon weather prices (slopes 0.69–0.97) are overconfident; beyond one month, they become underconfident.

## Sports Markets

Sports markets are well calibrated at short-to-medium horizons (slopes 0.90–1.10) but become sharply underconfident beyond one month (slope 1.74).

## Macro/Economic Markets

Macro/economic markets show calibration dynamics tied to information release schedules. CPI and Fed-rate contracts shift rapidly on announcement days.

## Practical Implications

For political markets one week pre-resolution, a 70-cent price actually implies approximately 83% probability, not 70%. Sports prices under one week are generally trustworthy. Short-horizon weather prices tend toward overconfidence, overstating predicted outcomes.

The research challenges the assumption that prediction markets aggregate information uniformly across domains, demonstrating that calibration quality depends systematically on what is being predicted, timing, and trader composition.
