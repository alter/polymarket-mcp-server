---
title: "Beating the House: Identifying Inefficiencies in Sports Betting Markets"
url: https://arxiv.org/abs/1910.08858
source: arxiv
date: "2019-10-19"
type: paper
theme: sports
lang: en
---

# Beating the House: Identifying Inefficiencies in Sports Betting Markets

**Authors:** Sathya Ramesh, Ragib Mostofa, Marco Bornstein, John Dobelman  
**arXiv:** 1910.08858 | October 19, 2019  
**Category:** Economics (General Economics, General Finance, Applications)

## Abstract

Developed a betting algorithm generating **above-market returns for the NFL, NBA, NCAAF, NCAAB, and WNBA** betting markets by analyzing a novel dataset and employing a non-parametric win probability model to locate positive expected value opportunities.

## Methodology

- **Model type:** Non-parametric win probability model
- **Engine:** Bayesian formula for a team's likelihood to win a game
- **Approach:** Allow pricing of moneyline bets for any point spread
- **Data:** Novel proprietary dataset of historical bets across 5 sports leagues
- **Strategy:** Identify and bet only on positive expected value (EV) situations

## Context

Research conducted following the U.S. Supreme Court's repeal of the federal ban on sports betting (2018), establishing relevance during the industry's growth phase.

## Key Insight

A non-parametric approach allows the model to remain flexible across different sports without imposing distributional assumptions. The Bayesian formula creates a team win probability estimate that can be compared directly against market-implied probabilities derived from sportsbook odds, flagging when the model's probability exceeds the market's by a sufficient margin.

## Transferable Edges

1. Non-parametric win probability estimation avoids overfitting structural assumptions
2. Moneyline pricing from point spread creates cross-market arbitrage signals
3. Multi-sport applicability suggests the edge is structural (market inefficiency) not sport-specific
4. Positive EV filtering as core bet selection criterion — do not bet on every game
