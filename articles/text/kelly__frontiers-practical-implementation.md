---
title: "Practical Implementation of the Kelly Criterion: Optimal Growth Rate, Number of Trades, and Rebalancing Frequency for Equity Portfolios"
url: "https://www.frontiersin.org/journals/applied-mathematics-and-statistics/articles/10.3389/fams.2020.577050/full"
source: "Frontiers in Applied Mathematics and Statistics"
date: "2020"
type: "academic_paper"
theme: "kelly"
lang: "en"
---

# Practical Implementation of the Kelly Criterion: Optimal Growth Rate, Number of Trades, and Rebalancing Frequency for Equity Portfolios

**Authors:** Carta and Conversano  
**Published:** Frontiers in Applied Mathematics and Statistics (2020)

## Overview

This peer-reviewed research article examines how the Kelly criterion performs as a portfolio optimization method compared to traditional mean-variance approaches.

## Key Findings

**Main Contribution**: "The Kelly criterion beats any other approach in many aspects. In particular, it maximizes the expected growth rate and the median of the terminal wealth."

**Performance Characteristics**:
- Kelly portfolios achieve higher expected returns but with greater volatility
- They are less diversified than traditional Markowitz portfolios
- Short-term performance is riskier, but long-term wealth accumulation exceeds competitors

## Methodology

The research combines:
1. Monte Carlo simulations using geometric Brownian motion to test Kelly criterion properties across 100 to 40,000 simulated trades
2. Real-world testing using 42 equities from the EuroStoxx50 index (2000-2018)
3. Rolling window optimization with 24-month rebalancing periods

## Critical Insights

Kelly's optimality requires genuinely long investment horizons:
- With only 100-1,000 trades: minimal advantage visible
- At 10,000+ trades: superior performance emerges clearly

**Practical Recommendation**: Optimal results with "a short length of the window width" (2 years) with daily rebalancing, though this increases transaction costs.

## Limitations

Over-leveraging significantly beyond Kelly's optimal fraction leads to portfolio deterioration. Accurate mean and variance estimation is essential — estimation errors can eliminate the criterion's advantages.

## Comparison with Mean-Variance

Kelly portfolios are less diversified than Markowitz portfolios but generate higher terminal wealth over long horizons, providing empirical confirmation of Kelly's theoretical superiority for long-run growth.
