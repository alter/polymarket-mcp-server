---
title: "Distributional Robust Kelly Gambling: Optimal Strategy under Uncertainty in the Long-Run"
url: "https://arxiv.org/abs/1812.10371"
source: "arXiv"
date: "2018-12-20"
type: "academic_paper"
theme: "kelly"
lang: "en"
---

# Distributional Robust Kelly Gambling: Optimal Strategy under Uncertainty in the Long-Run

**Authors:** Qingyun Sun and Stephen Boyd  
**arXiv ID:** 1812.10371 (math.OC)  
**Submitted:** December 20, 2018 | **Last Revised:** June 10, 2021

## Abstract

The paper addresses a practical limitation of classical Kelly gambling strategy: uncertainty about probability distributions. While traditional Kelly betting maximizes expected logarithmic wealth growth under known probabilities, real-world applications face distribution uncertainty.

The authors present a distributional robust version where "the bet is chosen to maximize the worst-case (smallest) expected log growth among the distributions in the given set." Key contributions include:

- **Computational**: The robust Kelly problem is convex and tractable for finite outcomes using standard disciplined convex programming tools (e.g., CVXPY).
- **Theoretical**: The authors extend Breiman's foundational work by proving that distributional robust Kelly strategy "asymptotically maximizes the worst-case rate of asset growth" and outperforms substantially different strategies.

## Classical vs. Distributional Robust Kelly

In classic Kelly gambling, bets are chosen to maximize the expected log growth of wealth, under a **known** probability distribution.

The **distributional robust version** considers the case where the probability distribution is not known but lies in a given set of possible distributions — and the bet is chosen to maximize the **worst-case (smallest) expected log growth** among the distributions in the given set.

## Key Results

1. The distributional robust Kelly gambling problem is convex, and for a large class of uncertainty sets, can be transformed into tractable form following disciplined convex programming rules.
2. In sequential decision making with varying distributions within a given uncertainty set, the distributionally robust Kelly strategy has been proven to **asymptotically maximize the worst-case rate of asset growth**, dominating any other essentially different strategy by magnitude.
3. Bridges classical Kelly theory and practical portfolio allocation by explicitly addressing model misspecification.

## Motivation

The classic Kelly strategy is rarely used in practical portfolio allocation directly due to practically unavoidable uncertainty about the true probability distribution. This paper provides a rigorous theoretical framework for making Kelly robust to this uncertainty via minimax optimization over ambiguity sets.
