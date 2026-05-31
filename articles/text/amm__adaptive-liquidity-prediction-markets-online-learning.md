---
title: "Adaptive Liquidity in Prediction Markets via Online Learning"
url: "https://arxiv.org/abs/2605.09599"
source: "arxiv"
date: "2026-05-10"
type: "paper"
theme: "amm"
lang: "en"
---

# Adaptive Liquidity in Prediction Markets via Online Learning

**Authors:** Enrique Nueve, Bao Nguyen, Rafael Frongillo, Bo Waggoner

**arXiv ID:** 2605.09599 (cs.GT)

## Abstract

Prediction markets rely on liquidity to convert trades into informative prices, yet existing mechanisms fix liquidity ex ante. This restriction enforces a static trade-off between price responsiveness and worst-case loss despite inherently nonstationary trading conditions. This paper proposes treating liquidity selection as an online learning challenge. The mechanism combines multiple cost-function markets through learnable weights, creating a single adaptive market that preserves no-arbitrage, bounded worst-case loss, expressiveness, and positive upside.

## Key Contributions

- Novel framework introducing a hybrid structural risk signal that quantifies the balance between price impact and inventory risk on a per-round basis
- Theoretical guarantees: standard online learning algorithms achieve switching-regret bounds relative to optimal liquidity sequences determined retrospectively
- Empirical validation: simulations show the mechanism dynamically adjusts liquidity parameters in response to order flow patterns and inventory changes
- Interdisciplinary connection: establishes linkages between prediction market design methodology and online learning theory
- Preserves all key market properties: no-arbitrage, bounded worst-case loss, expressiveness, positive upside

## Significance

First principled framework for adaptive liquidity in prediction markets, connecting market design with online learning theory and providing switching-regret guarantees.
