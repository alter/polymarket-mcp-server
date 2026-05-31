---
title: "ParlayMarket: Automated Market Making for Parlay-style Joint Contracts"
url: "https://arxiv.org/abs/2603.22596"
source: "arxiv"
date: "2026-03-23"
type: "paper"
theme: "amm"
lang: "en"
---

# ParlayMarket: Automated Market Making for Parlay-style Joint Contracts

**Authors:** Ranvir Rana, Viraj Nadkarni, Niusha Moshrefi, Pramod Viswanath

**arXiv ID:** 2603.22596 | v2: May 19, 2026

## Abstract

Prediction markets are powerful mechanisms for aggregating information about uncertain events, but existing designs fundamentally operate at the level of individual contracts. In automated settings, liquidity is typically provided by cost-function market makers, most notably the logarithmic market scoring rule (LMSR). The paper shows that parlay markets can be supported within an AMM without incurring combinatorial loss or requiring full joint enumeration, providing a practical and scalable design that bridges combinatorial expressiveness and deployable market infrastructure.

## Key Contributions

- **Convergence Analysis:** Establishes that AMM dynamics reach a unique fixed point representing the best approximation to the true joint distribution
- **Error Bounds:** Demonstrates that parameter error remains bounded at equilibrium and pricing errors scale quadratically with the number of base markets
- **Structural Role of Parlays:** Shows that parlay trades directly improve identifiability of dependence structures, reducing steady-state error compared to marginal-only trading
- **Empirical Validation:** Provides evidence through simulations and historical Kalshi parlay data replay

## Significance

First AMM for parlay-style joint contracts that avoids combinatorial explosion while maintaining coherent pricing. Bridges combinatorial expressiveness with practical deployability.
