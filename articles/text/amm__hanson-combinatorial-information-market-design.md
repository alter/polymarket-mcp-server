---
title: "Combinatorial Information Market Design"
url: "https://hanson.gmu.edu/combobet.pdf"
source: "hanson.gmu.edu"
date: "2003-01-01"
type: "paper"
theme: "amm"
lang: "en"
---

# Combinatorial Information Market Design

**Author:** Robin D. Hanson

**Published:** Information Systems Frontiers, Springer, vol. 5(1), pages 107-119, January 2003.
**DOI:** 10.1023/A:1022058209073

**Semantic Scholar:** https://www.semanticscholar.org/paper/Combinatorial-Information-Market-Design-Hanson/0785bb16786e5d57da2d0ba4dc440cd3f9b710f0

## Overview

Combinatorial information markets aggregate information on the entire joint probability distribution over many variables, by allowing bets on all variable value combinations. This paper introduces market scoring rules and considers several design issues:
- How to represent variables to support both conditional and unconditional estimates
- How to avoid becoming a money pump via errors in calculating probabilities
- How to ensure that users can cover their bets without needlessly preventing them from using previous bets as collateral for future bets

## Key Insight: Market Scoring Rules

Standard information markets face thin markets and irrational participation. Scoring rules suffer from opinion pooling problems. Market scoring rules avoid all these problems:
- In thin markets: act like simple scoring rules (elicit individual beliefs)
- In thick markets: act like automated market makers (aggregate group consensus)
- Logarithmic versions have cost and modularity advantages

## LMSR Properties

- Bounded loss that grows logarithmically in the number of outcomes
- Infinite liquidity
- Modularity that respects independence relationships
- Prices always live between 0 and 1 (behave like probabilities)
- In mutually exclusive outcome markets: prices are jointly coherent (if one goes up, others adjust)

## Significance

This paper and Hanson's 2007 follow-up ("Logarithmic Market Scoring Rules for Modular Combinatorial Information Aggregation") are the foundational works for the entire field of automated market making for prediction markets. The LMSR is the de facto standard for subsidized prediction markets and the conceptual ancestor of all DeFi AMMs.
