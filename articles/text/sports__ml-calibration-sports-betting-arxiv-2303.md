---
title: "Machine Learning for Sports Betting: Should Model Selection Be Based on Accuracy or Calibration?"
url: https://arxiv.org/abs/2303.06021
source: arxiv
date: "2023-03-10"
type: paper
theme: sports
lang: en
---

# Machine Learning for Sports Betting: Should Model Selection Be Based on Accuracy or Calibration?

**Authors:** Conor Walsh, Alok Joshi  
**arXiv:** 2303.06021 | Submitted March 10, 2023 (v4: February 1, 2024)  
**Category:** Machine Learning (cs.LG)

## Abstract

The paper examines whether model calibration or accuracy better serves sports betting predictions. The authors state: "If bettors can leverage data to reliably predict the probability of an outcome, they can recognise when the bookmaker's odds are in their favour."

They hypothesize that calibration outperforms accuracy for this domain. Testing on NBA data with published odds, the results showed calibration-based selection achieved **+34.69% return on investment versus -35.17%** compared to accuracy-based selection.

## Key Findings

**Performance Metrics:**
- Calibration-based model selection: **+34.69% ROI** (average), +36.93% (best case)
- Accuracy-based model selection: **-35.17% ROI** (average), +5.56% (best case)

## Primary Claims

The authors conclude that "for sports betting (or any probabilistic decision-making problem), calibration is a more important metric than accuracy." They recommend bettors prioritize calibration when selecting predictive models.

## Core Insight for Prediction Markets

A well-calibrated model with moderate accuracy can dramatically outperform a high-accuracy model that is poorly calibrated in a betting context. The reason: what matters is not how often you pick the right winner, but whether your probability estimates match reality when you place bets with positive expected value.

**Transferable edge:** Use calibration metrics (Brier score, reliability diagrams, ECE) rather than accuracy to select models for value-bet detection on Polymarket/Kalshi.
