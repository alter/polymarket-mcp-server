---
title: "What is Calibration?"
url: https://www.lesswrong.com/posts/goppxmpbFoTfdaTsE/what-is-calibration
source: lesswrong
date: "2023-03-13"
type: blog
theme: forecast
lang: en
---

# What is Calibration?

**Author:** AlexMennen
**Date:** March 13, 2023
**Platform:** LessWrong

## Summary

Explores the nuanced concept of calibration in probabilistic forecasting. Challenges the seemingly straightforward definition that "a forecaster is well-calibrated if, for every p∈[0,1], the fraction of propositions assigned probability p that are true approximately equals p."

## Key Points

**The Core Problem:** Calibration depends critically on how predictions are organized and sequenced. The same underlying information can produce excellent or terrible calibration scores depending on the chosen framework.

**Mathematical Examples:** For any probability distribution, you can construct sequences where a forecaster appears perfectly calibrated or catastrophically miscalibrated — regardless of actual forecasting skill.

**Practical Implication:** Correlation among predictions undermines naive calibration evaluation. When multiple forecasts share common causal factors (like economic conditions affecting multiple companies), outcomes cluster toward extremes rather than matching assigned probabilities.

**True Purpose:** Calibration training aims to detect whether someone's probability estimates form a coherent distribution, not merely whether they're accurate on specific sequences.

## Recommendation

Either factor correlated propositions separately or remove redundant correlated predictions before evaluation to maintain statistical validity.

## Relevance to Prediction-Market Pricing

Critical warning against naive calibration evaluation on correlated Polymarket questions. Political event markets are highly correlated (election outcome affects many downstream markets) — calibration should be evaluated on decorrelated subsets.
