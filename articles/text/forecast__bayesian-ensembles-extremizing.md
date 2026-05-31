---
title: "Bayesian Ensembles of Binary-Event Forecasts: When Is It Appropriate to Extremize or Anti-Extremize?"
url: https://arxiv.org/abs/1705.02391
source: arxiv
date: "2017-05-05"
type: paper
theme: forecast
lang: en
---

# Bayesian Ensembles of Binary-Event Forecasts: When Is It Appropriate to Extremize or Anti-Extremize?

**Authors:** Kenneth C. Lichtendahl Jr., Yael Grushka-Cockayne, Victor Richmond R. Jose, Robert L. Winkler
**Submitted:** May 5, 2017; Last revised April 11, 2018
**Published:** Operations Research, 2021 (DOI: 10.1287/opre.2021.2176)
**arXiv:** 1705.02391

## Abstract

Organizations routinely combine multiple expert or model forecasts for binary events. While averaging forecasts is common, it tends to be underconfident. The authors introduce "Bayesian ensembles" — optimal aggregators that transform experts' probabilities into information states, combine these linearly, then transform back to probability space. Key finding: these optimal aggregators do not always extremize the average forecast, and when they do, they can run counter to existing methods.

## Introduction

Extremizing — shifting the average probability farther from 0.5 (or from the base rate) — is commonly used to improve naive forecast averages. The theoretical justification is that forecasters share some information, so the raw average double-counts shared knowledge and is underconfident. However, the direction and magnitude of optimal adjustment depends on the information structure.

## Main Findings

1. **Bayesian ensemble construction:** Transform expert probabilities into information states (log-odds space), take a weighted linear combination, then transform back. This is non-linear in the original probability space.
2. **When to extremize vs. anti-extremize:** Extremizing (pushing away from base rate) is appropriate when experts have largely independent information. Anti-extremizing is appropriate when experts share substantial common information.
3. **Empirical performance:** Bayesian ensembles outperform the linear opinion pool and individual algorithms on real datasets.
4. **Novel concept of anti-extremizing:** Introduced as a formal counterpart to extremizing; necessary when shared information dominates.

## Methodology

- Bayesian framework modeling information as conditionally independent signals given the outcome
- Tested on geopolitical forecasting data from Good Judgment Project
- Compared against: simple average, extremized average, prediction markets, other ensemble methods

## Conclusion

The direction of optimal aggregation adjustment (extremizing vs. anti-extremizing) depends on the information structure of the forecasting group. Practitioners should not blindly apply extremization — the correct direction depends on how much information forecasters share.

## Relevance to Prediction-Market Pricing

Provides the theoretical framework for understanding when to push Polymarket prices further toward 0 or 1 (extremize), and when to pull them back toward 0.5 (anti-extremize). Directly applicable to building aggregation layers on top of raw market prices.
