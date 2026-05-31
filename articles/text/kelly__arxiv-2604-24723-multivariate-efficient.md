---
title: "Efficient Multivariate Kelly Optimization Reveals Sigmoidal Scaling Laws"
url: "https://arxiv.org/abs/2604.24723"
source: "arXiv"
date: "2026-04-27"
type: "academic_paper"
theme: "kelly"
lang: "en"
---

# Efficient Multivariate Kelly Optimization Reveals Sigmoidal Scaling Laws

**Authors:** Ruslan Tepelyan and Daniel Lam  
**arXiv ID:** 2604.24723 (q-fin.MF)  
**Submitted:** April 27, 2026; Revised April 29, 2026

## Abstract Overview

The research addresses a computational challenge in Kelly criterion optimization when managing multiple simultaneous bets. The authors note that "the optimal Kelly strategy generally requires numerical optimization over a joint outcome space" and that naive approaches demand exponential computational resources — O(2^N) time and memory for N simultaneous wagers.

## Key Methodological Contributions

**Method 1 — Integral Transform Approach:** For independent bets, the team developed a formulation that reduces computational complexity from O(2^N) to O(N), enabling solutions for problems with hundreds of bets through numerically stable quadrature methods.

**Method 2 — Decomposition Strategy:** The researchers created a decomposition-based approach that "constructs and solves carefully chosen subproblems, yielding feasible lower bounds and infeasible upper bounds" on optimal growth rates.

## Primary Findings

Using synthetic prediction market data, the researchers demonstrated that "the shortfall ratio between the lower and upper bounds is well-approximated by a sigmoid function of the relative subproblem size." This reveals predictable scaling patterns based on low-dimensional problem statistics, making it practical to quantify solution quality in large-scale scenarios.

## Significance for Prediction Markets

This paper is directly applicable to portfolio-level Kelly sizing on prediction markets like Polymarket, where a trader may have dozens of simultaneous open positions. Classical Kelly can only handle one bet at a time; this work scales the multivariate problem to hundreds of concurrent bets efficiently.
