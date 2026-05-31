---
title: "Expected Goals in Football: Improving Model Performance and Demonstrating Value"
url: https://journals.plos.org/plosone/article?id=10.1371%2Fjournal.pone.0282295
source: plosone
date: "2023-01-01"
type: paper
theme: sports
lang: en
---

# Expected Goals in Football: Improving Model Performance and Demonstrating Value

**Journal:** PLOS ONE, 2023  
**Also available:** PubMed (PMC10075453)

## What is xG?

Expected Goals (xG): probability score (0–1) for each shot representing likelihood it results in a goal based on historical data.

Examples:
- Penalty spot shot: ~0.76 xG
- Header from 8 yards after cross: ~0.15 xG
- 25-yard driven shot with defenders: ~0.03 xG

## Research Gap

xG models had not previously considered:
1. **Player/team ability** effects on conversion rates
2. **Psychological effects** (pressure, match situation)

This paper addresses both through machine learning.

## Methodology

- Extended xG model with player-specific and team-specific ability features
- Compared traditional statistics vs. newly developed xG metric for predictive ability
- Machine learning models for xG value estimation

## Key Results

- Error values "competitive with optimal values from other papers"
- Added features (player/team ability, psychological context) have "significant impact on xG model outputs"
- Model improves on basic xG by incorporating previously untested features

## xG in Betting Markets

For sportsbooks/bettors:
- Sum of match xG = estimate of how many goals team "should have scored"
- Discrepancy between xG and actual goals signals regression to mean opportunity
- High xG with no goals = positive regression signal (team due to score)
- Low xG with many goals = negative regression signal (team lucky, will regress)

**Sportsbooks now display live xG** within betting apps to help bettors gauge underlying performance vs. actual score.

## Bundesliga Betting Study (SAGE Journals 2026)

Related paper: "Can simple models predict football — and beat the odds? Lessons from the German Bundesliga"
- xG-based model vs. 11 Bundesliga seasons (2014/15 – 2024/25)
- **~10% ROI** using average market odds
- **~15% ROI** using best available prices
- "Bookmaker odds exhibit superior statistical calibration but xG model captures signals not fully reflected in market prices"

## Transferable Edge

1. xG provides signal **orthogonal to current market prices** in specific situations
2. Teams with high xG / low goals → market underestimates true threat (buy)
3. Teams with low xG / high goals → market overestimates quality (sell)
4. xG combined with Bayesian Poisson model → competitive with Betfair accuracy
5. For Polymarket football contracts: real-time xG is actionable signal during live trading windows
