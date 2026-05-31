---
title: "Mathematical Analysis of Multi-Outcome Sports Betting Arbitrage: A System of Equations Approach"
url: https://www.researchgate.net/publication/387671984_Mathematical_Analysis_of_Multi-Outcome_Sports_Betting_Arbitrage_A_System_of_Equations_Approach
source: researchgate
date: "2025-01-01"
type: paper
theme: sports
lang: en
---

# Mathematical Analysis of Multi-Outcome Sports Betting Arbitrage

**Source:** ResearchGate, 2025

## Overview

Mathematical framework for identifying and exploiting arbitrage opportunities in **three-outcome sports betting markets** (Home-Draw-Away). Uses systems of simultaneous equations to determine optimal stake allocation.

## Core Condition for Arbitrage

Arbitrage occurs when the sum of reciprocals of all offered odds is less than 1:

```
1/odds_H + 1/odds_D + 1/odds_A < 1
```

This means the total implied probabilities across all outcomes < 100% — guaranteed profit exists regardless of outcome.

## Stake Optimization System

To maximize profit across all outcomes simultaneously, solve:

```
Stake_H × odds_H = Target_profit
Stake_D × odds_D = Target_profit  
Stake_A × odds_A = Target_profit
```

Optimal stake for each outcome: `Stake_i = Total_investment / (odds_i × Σ(1/odds_j))`

## Why Opportunities Exist

1. Different bookmakers with different opinion on probability of outcomes
2. Timing: one book updates faster than another after news
3. Promotional odds: books sometimes offer enhanced odds creating arb vs. other books
4. Round-number pricing errors in less liquid markets

## Practical Detection Algorithm

```python
def detect_arbitrage(h_odds, d_odds, a_odds):
    implied_sum = 1/h_odds + 1/d_odds + 1/a_odds
    if implied_sum < 1.0:
        profit_pct = (1 - implied_sum) / implied_sum * 100
        return True, profit_pct
    return False, 0
```

## Limitations

- Bookmakers identify and limit arbitrageurs systematically
- Requires simultaneous bet placement (execution risk)
- Opportunities typically last seconds in liquid markets
- Account restrictions reduce long-term viability
- In prediction markets (Polymarket): YES + NO for same contract should sum to exactly $1.00 by design — but can deviate in combinatorial NegRisk markets

## Application to Prediction Markets

For binary prediction markets (Polymarket/Kalshi):
- `1/YES_price + 1/NO_price < 1.0` when sum of prices < $1.00 (direct arb)
- In combinatorial markets: more complex, 3+ outcome structure applies directly
- The Saguillo et al. (2025) study found $40M extracted from 7,051 Polymarket opportunities using this exact framework
