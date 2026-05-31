---
title: "Adaptive Liquidity in Prediction Markets via Online Learning"
url: "https://arxiv.org/abs/2605.09599"
source: "arxiv"
date: "2026-05-10"
type: "paper"
theme: "academic"
lang: "en"
authors: ["Enrique Nueve", "Bao Nguyen", "Rafael Frongillo", "Bo Waggoner"]
---

# Adaptive Liquidity in Prediction Markets via Online Learning

## Abstract

This paper introduces an adaptive prediction market that dynamically adjusts liquidity by combining multiple cost-function market makers through learnable weights. Traditional markets fix liquidity ex ante, creating inflexible trade-offs between price responsiveness and risk management. The approach reframes liquidity selection as an online learning problem.

## Key Findings

- The mechanism preserves essential properties including no-arbitrage conditions, bounded worst-case losses, expressiveness, and positive returns
- Introduces a hybrid structural risk signal measuring trade-offs between price impact and inventory risk on a per-round basis
- Standard online learning algorithms achieve switching-regret guarantees relative to the best sequence of liquidity regimes in hindsight
- Simulations show the mechanism dynamically adjusts liquidity in response to both order flow patterns and inventory changes
- Establishes theoretical connections between prediction market design and online learning frameworks

Submitted May 10, 2026; Classification: Computer Science and Game Theory (cs.GT).
