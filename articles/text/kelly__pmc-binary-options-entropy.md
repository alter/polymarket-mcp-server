---
title: "Portfolio Optimization for Binary Options Based on Relative Entropy"
url: "https://pmc.ncbi.nlm.nih.gov/articles/PMC7517297/"
source: "PMC / MDPI"
date: "2020"
type: "academic_paper"
theme: "kelly"
lang: "en"
---

# Portfolio Optimization for Binary Options Based on Relative Entropy

**Published in:** PMC (PubMed Central) / MDPI

## Overview

This research paper introduces Discrete Entropic Portfolio Optimization (DEPO), a novel approach to managing portfolios of binary options and other discrete-return assets using information theory principles.

## Key Concepts

**Core Innovation**: Extends entropy-based portfolio optimization to assets with binary returns (+100% or -100%), such as binary options and sports bets, moving beyond traditional continuous-return models.

**Risk Measurement**: Rather than variance, the method uses "relative entropy" (Kullback-Leibler divergence) to measure portfolio risk. The target distribution is uniform, meaning maximum entropy indicates lower risk — opposite to continuous return portfolios.

**Mathematical Framework**: "Relative entropy satisfies monotonicity, translation invariance, and convexity" requirements for a valid risk measure.

## Main Results

**DEPO Advantages**:
- Non-parametric and robust to non-normality
- Handles small sample sizes effectively
- Balances expected growth rate with risk mitigation

**Empirical Performance**:
- FOREX binary options (Feb-Mar 2020): DEPO generated 9.1% profit vs. 36.4% loss for Kelly criterion
- NFL betting (2019-20 season): DEPO produced 10% gain vs. 37% loss for Kelly approach

## Practical Applications

Applies to any discrete-outcome investments including digital options, sports betting, and fixed-return contracts — directly applicable to prediction markets (YES/NO binary resolution).

## Relevance

Demonstrates that Kelly criterion can be **outperformed** by entropy-based alternatives for binary outcomes when probability estimation is imperfect. Important caveat for prediction market traders relying solely on Kelly.
