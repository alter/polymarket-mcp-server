---
title: "Kelly Criterion for Prediction Markets — Optimal Position Sizing for Trading Bots"
url: "https://rekko.ai/docs/guides/kelly-criterion-position-sizing"
source: "Rekko AI"
date: "2024"
type: "documentation"
theme: "kelly"
lang: "en"
---

# Kelly Criterion for Prediction Markets — Optimal Position Sizing for Trading Bots

## Overview

The Kelly criterion provides a mathematical framework for determining optimal position sizes in prediction market trading. This methodology balances growth maximization with risk management by calculating the fraction of a trader's bankroll to allocate based on estimated probability and market pricing.

## Core Formula

For binary prediction markets:

`kelly_fraction = (p - c) / (1 - c)`

where p represents the trader's estimated probability, and c denotes the market price.

## Key Principles

**Why Position Sizing Matters**: Even with consistent edge, poor position sizing destroys returns. The Kelly criterion solves this by calculating sizing that maximizes long-term growth rate given estimated probabilities and market prices.

**Full Kelly Versus Fractional Approaches**:
- Half Kelly (50% of full Kelly) reduces variance substantially while maintaining ~75% of growth potential
- Quarter Kelly suits conservative approaches with uncertain estimates
- Traders should adjust fractions based on confidence in probability estimates

## Practical Implementation

Manual calculations involve determining edge (estimated probability minus market price) and applying the Kelly formula. The Rekko API automates this process through signals endpoints that estimate true probabilities, calculate edges, and return Kelly-derived position sizing recommendations.

Portfolio considerations become important when holding correlated positions. Specialized portfolio endpoints account for correlation effects to prevent oversizing across related markets.

## Critical Warnings

Common pitfalls include:
- Overestimating edge
- Ignoring trading fees
- Sizing positions independently without portfolio awareness
- Deploying full Kelly despite its theoretical assumptions rarely holding in practice
