---
title: "Numerically Solving Kelly Criterion for Multiple Simultaneous Bets"
url: "https://vegapit.com/article/numerically_solve_kelly_criterion_multiple_simultaneous_bets/"
source: "VegaPit"
date: "2020"
type: "blog"
theme: "kelly"
lang: "en"
---

# Numerically Solving Kelly Criterion for Multiple Simultaneous Bets

## Overview

This article explores extending the Kelly criterion from single bets to multiple simultaneous independent bets using numerical optimization methods.

## The Kelly Criterion Foundation

The Kelly criterion formula:

**f* = [p(b+1) - 1] / b**

where p represents winning probability and b represents the profit-to-loss ratio. This maximizes the expected logarithm of final wealth.

## The Multiple Bets Challenge

When dealing with simultaneous independent bets, algebraic solutions become impractical. "Solving for f₁ and f₂ algebraically is clearly more time consuming to achieve than for the single bet case."

## Numerical Solution Approach (Rust implementation)

The author implements a Rust-based solution using gradient ascent optimization:

- `multiple_simultanous_expectation_log_wealth()` calculates expected log-wealth and its gradient across all possible bet outcomes using Cartesian products
- `multiple_simultaneous_kelly()` applies iterative gradient ascent with constraints: clips individual wagers between 0 and 1; scales proportionally if total exceeds 100%

## Key Finding: Probabilistic Edge

Research by C. Whitrow (2007) revealed that optimal wagers correlate with "probabilistic edge":

**Probabilistic edge = p - 1/(b + 1)**

"Optimal wagers tend to be proportional to the 'probabilistic edge' of each bet."

## Sequential vs. Simultaneous Comparison

A critical distinction: optimal wagers in simultaneous settings are substantially smaller than individual Kelly criteria suggest. The distribution differs dramatically from sequential betting, with allocations rebalancing based on collective portfolio risk.

## Risk Considerations

"Optimising long term returns is only one aspect of the risk allocation process" — risk-of-ruin mitigation is equally critical.

## Conclusion

Kelly's framework extends to multiple simultaneous bets through numerical methods. Practitioners should treat results as reference points, balancing growth maximization against risk tolerance and ruin probability.
