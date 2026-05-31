---
title: "Sports Betting: An Application of Neural Networks and Multiple Simultaneous Kelly Criterion"
url: https://arxiv.org/abs/2307.13807
source: arxiv
date: "2023-07-01"
type: paper
theme: sports
lang: en
---

# Sports Betting: An Application of Neural Networks

**arXiv:** 2307.13807

## Abstract

Combines Von Neumann-Morgenstern Expected Utility Theory, deep learning techniques, and advanced formulations of the Kelly Criterion to optimize sports betting strategies. Applied to English Premier League.

## Key Results

- **135.8% profit** relative to initial wealth during latter half of the 20/21 EPL season
- Model accuracy: **54%** (vs crowd 51.5%, "always home" 38%, "always draw" 20%)
- Multiple Simultaneous Kelly Criterion (SQP algorithm): ~20.9 seconds to converge per fixture

## Methodology

1. **Deep neural network** forecasts match outcome probabilities (3-way: H/D/A)
2. **Multiple Simultaneous Kelly Criterion** optimizes bet allocation across simultaneous fixtures
- For portfolios with logarithmic utilities
- SQP (Sequential Quadratic Programming) algorithm used
3. Integration of **modern portfolio theory** principles into bet selection

## Core Insight

Rather than betting on individual games sequentially, the model treats a slate of fixtures as a portfolio optimization problem — minimizing covariance between bets while maximizing expected log-wealth growth. This diversification within a betting session is a key innovation beyond single-game Kelly.

## Transferable Edges

- Portfolio-theoretic bet sizing across correlated events (applies directly to Polymarket)
- Logarithmic utility (Kelly growth) as the objective function
- Neural network probability estimates as inputs to Kelly formula
- Even modest accuracy improvements over the market (54% vs 51.5%) compound to large returns when sized correctly
