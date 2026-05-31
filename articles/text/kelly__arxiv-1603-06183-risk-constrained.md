---
title: "Risk-Constrained Kelly Gambling"
url: "https://arxiv.org/abs/1603.06183"
source: "arXiv"
date: "2016-03-20"
type: "academic_paper"
theme: "kelly"
lang: "en"
---

# Risk-Constrained Kelly Gambling

**Authors:** Enzo Busseti, Ernest K. Ryu, Stephen Boyd  
**arXiv ID:** 1603.06183 [q-fin.PM]  
**Submitted:** March 20, 2016  
**Published in:** Journal of Investing, 25(3):118–134, 2016  
**Full PDF also at:** https://web.stanford.edu/~boyd/papers/pdf/kelly.pdf

## Abstract

The paper considers the classic Kelly gambling problem with a general distribution of outcomes, and an additional risk constraint that limits the probability of a drawdown of wealth to a given undesirable level. The authors develop a bound on the drawdown probability; using this bound instead of the original risk constraint yields a convex optimization problem that guarantees the drawdown risk constraint holds.

Numerical experiments show that the bound on drawdown probability is reasonably close to the actual drawdown risk, as computed by Monte Carlo simulation.

## Key Findings

1. **Drawdown bound**: A mathematical bound on drawdown probability is derived and used to transform the constrained Kelly problem into a tractable convex optimization.
2. **Outperforms fractional Kelly**: Simulations show that this method yields bets that outperform fractional-Kelly bets for the same drawdown risk level or growth rate.
3. **Markowitz connection**: A natural quadratic approximation of the convex problem is closely connected to the classical mean-variance Markowitz portfolio selection problem.
4. **Single risk-aversion parameter**: The method is parametrized by a single parameter interpreted as a risk-aversion parameter, allowing a systematic tradeoff between asymptotic growth rate and drawdown risk.

## Problem Formulation

In Kelly gambling, a fixed fraction of total wealth is placed on n bets. The fractions are denoted as **b** ∈ ℝⁿ, so **b** ≥ 0 and **1**ᵀ**b** = 1.

A bound on the drawdown risk is derived, which is then used to form the risk-constrained Kelly gambling problem — a tractable convex optimization problem.

## Background

In 1956, John Kelly proposed a systematic way to allocate total wealth across a number of bets so as to maximize the long-term growth rate when the gamble is repeated.

It is well known that with Kelly-optimal bets there is a risk of wealth dropping substantially from its original value before increasing — i.e., a drawdown. Several ad hoc methods can be used to limit this drawdown risk, at the cost of decreased growth rate. The best known method is fractional Kelly betting, in which only a fraction of the Kelly optimal bets are made.

This paper provides a principled, convex-optimization approach to replacing the ad hoc fractional Kelly with a constraint-satisfying method that provably beats fractional Kelly at the same risk level.
