---
title: "Optimal Market Making (Guéant-Lehalle-Fernandez-Tapia Model)"
url: https://ar5iv.labs.arxiv.org/html/1605.01862
source: arxiv
date: "2016-05-06"
type: paper
theme: micro
lang: en
---

# Optimal Market Making (Guéant-Lehalle-Fernandez-Tapia)

**Author:** Olivier Guéant

**Submitted:** May 6, 2016

**arXiv:** 1605.01862

**Published:** Applied Mathematical Finance, 2017, 24(2), pp. 112-154

**Original GLFT paper:** "Dealing with the Inventory Risk: A Solution to the Market Making Problem" (Guéant, Lehalle, Fernandez-Tapia, 2013)

## Overview

The GLFT model extends Avellaneda-Stoikov with closed-form solutions and explicit inventory bounds. It derives tractable bid/ask quote formulas through the Hamilton-Jacobi-Bellman equation, yielding linear ordinary differential equations.

## Closed-Form Approximations

The GLFT formulas provide closed-form approximations for:
- **Optimal bid quote:** b* = r - δ_b*(t, q)
- **Optimal ask quote:** a* = r + δ_a*(t, q)

where r = reservation (indifference) price, and δ_b*, δ_a* solve a system of linear ODEs.

**Key difference from A-S:** GLFT enforces inventory bounds [q_min, q_max]:
- At q = q_max: no bid quotes posted (stop buying)
- At q = q_min: no ask quotes posted (stop selling)

This prevents inventory from diverging — critical for practical implementation.

## Generalizations

The 2016 Guéant paper extends to:
- **General intensity functions** (not just exponential order arrival)
- **Different optimization criteria** (not just CARA utility)
- **Multi-asset market making** — first closed-form approximations for multi-asset case

## Practical Adoption

GLFT formulas are used by major European and Asian banks for market making in:
- Quote-driven (illiquid) markets
- Order-driven markets with small tick size

## Relevance to Polymarket CLOB Trading

- **Closed-form quotes for Polymarket makers:** δ_b*, δ_a* can be computed in real-time from ODE solution
- **Inventory bounds critical:** Polymarket binary markets have natural bounds: 0% to 100% of portfolio in YES
- **Calibration:** Estimate λ(δ) (fill intensity vs. spread) from Polymarket historical fills; fit exponential or other intensity function
- **Multi-asset extension:** Relevant for correlated Polymarket markets (election candidate markets, related event markets)
- **Available implementation:** hftbacktest has GLFT notebook at https://hftbacktest.readthedocs.io/en/py-v2.0.0/tutorials/GLFT%20Market%20Making%20Model%20and%20Grid%20Trading.html

**Classification:** Quantitative Finance (Trading and Market Microstructure)
