---
title: "Optimal Betting: Beyond the Long-Term Growth"
url: "https://arxiv.org/abs/2503.17927"
source: "arXiv"
date: "2025-03-23"
type: "academic_paper"
theme: "kelly"
lang: "en"
---

# Optimal Betting: Beyond the Long-Term Growth

**Authors:** Levon Hakobyan and Sergey Lototsky  
**arXiv ID:** 2503.17927  
**Submitted:** March 23, 2025  
**Classification:** Quantitative Finance > Risk Management

## Abstract Summary

The paper addresses limitations of the Kelly portfolio strategy. While this approach offers "optimal long-term growth rate," it tends to produce overly aggressive investment strategies. The authors propose a unified framework for evaluating risk in both discrete and continuous-time scenarios by introducing "asymptotic variance that describes fluctuations of the portfolio growth." Building on this analysis, they develop two novel risk measurement approaches.

## Key Mathematical Framework

The standard wealth model is defined as:

W_n^f = W_0 * product_{k=1}^n (1 + f * r_k)

where f is the betting fraction and r_k are iid returns. Two critical functions characterize performance:

- **Growth rate**: g_r(f) = E[ln(1+f*r)]
- **Asymptotic variance**: v_r(f) = Var[ln(1+f*r)]

The paper proves that g_r(f) is concave and v_r(f) is increasing in the betting fraction — establishing that higher growth comes with greater variance.

## Two New Risk Measures

**1. Asymptotic Sharpe Ratio**:
SR_r(f) = g_r(f) / sqrt(v_r(f))

This captures the growth-to-volatility tradeoff and is "monotonically decreasing near the Kelly optimum f*," meaning conservative bets improve risk-adjusted returns.

**2. Ridge Coefficient**:
Ri_r(f, gamma) = g_r(f) - gamma * v_r(f)

where gamma represents risk aversion. For any gamma > 0, maximizing this coefficient yields a fractional Kelly strategy f_Ri < f*.

## Central Result

Every fractional Kelly strategy can be realized as a solution to the constrained optimization problem: maximize growth subject to a variance bound. This provides a computationally efficient alternative to prior approaches while grounding strategy selection in the central limit theorem.

## Illustrative Examples

**Bernoulli Case (p=0.75)**: Choosing f = 0.2 instead of Kelly's f* = 0.5 sacrifices roughly 30% growth while reducing variance by ~90%.

**Heavy-Tailed Returns**: With Cauchy-distributed returns, maximizing Sharpe ratio suggests f ≈ 0.25, substantially reducing the Kelly fraction while maintaining ~90% of optimal growth.

## Extensions

The framework extends beyond iid returns to ergodic sequences with weak dependence, incorporating correlation effects through an adjusted asymptotic variance.
