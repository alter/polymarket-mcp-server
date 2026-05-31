---
title: "Position Sizing in Prediction Markets: The Kelly Criterion Guide"
url: "https://www.predictionhunt.com/blog/prediction-market-position-sizing-kelly-criterion"
source: "PredictionHunt"
date: "2024"
type: "blog"
theme: "kelly"
lang: "en"
---

# Position Sizing in Prediction Markets: The Kelly Criterion Guide

## Core Thesis

87% of prediction market traders lose money not due to poor forecasting, but because they size positions incorrectly. The Kelly Criterion provides a mathematical framework for optimal bet sizing, though most professionals use fractional versions rather than full Kelly.

## Key Concepts

**The Kelly Formula**
f* = (bp − q) / b, where f* represents the bankroll fraction to wager, b equals net odds, p is winning probability, and q is losing probability. For prediction markets, b = (1 − price) / price.

**Full Kelly's Problem**
Research shows full Kelly creates "a 33% probability of halving your bankroll before doubling it." While theoretically optimal, this extreme volatility causes traders to abandon their strategies during drawdowns.

**Fractional Kelly Solution**
Half-Kelly produces "approximately 75% of full Kelly's growth rate while cutting volatility in half." This tradeoff makes it the standard among professional traders and quantitative firms.

## Prediction Market-Specific Challenges

- Takers lose money (-1.12% per trade on average) while makers gain (+1.12%), emphasizing execution discipline
- Capital remains locked until contract resolution, creating opportunity costs
- Liquidity varies dramatically across contracts
- Resolution ambiguity introduces unexpected risks

## Correlation Risk

The article emphasizes that seemingly diversified portfolios often contain hidden correlations. Using Student-t copulas reveals joint blowup probabilities are "two to five times higher" than standard models suggest.

## Practical Framework

- Sizing single positions at 2–5% of total bankroll
- Maintaining 30% cash reserves for opportunities
- Tracking correlated exposure across positions
- Scaling down during drawdowns (cut sizes in half after 20% loss)

## The Data

Analysis of 72.1 million trades across prediction markets reveals that top 13% of profitable traders share common traits: mathematical discipline, patience on entries, and conservative sizing — not superior forecasting ability.
