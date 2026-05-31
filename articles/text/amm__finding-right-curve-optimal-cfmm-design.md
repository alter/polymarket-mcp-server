---
title: "Finding the Right Curve: Optimal Design of Constant Function Market Makers"
url: "https://arxiv.org/abs/2212.03340"
source: "arxiv"
date: "2022-12-06"
type: "paper"
theme: "amm"
lang: "en"
---

# Finding the Right Curve: Optimal Design of Constant Function Market Makers

**Authors:** Mohak Goyal, Geoffrey Ramseyer, Ashish Goel, David Mazières

**arXiv ID:** 2212.03340 | Published at EC'23 (ACM Conference on Economics and Computation)

## Abstract

Constant Function Market Makers (CFMMs) are a tool for creating exchange markets, have been deployed effectively in prediction markets, and are now especially prominent in the Decentralized Finance ecosystem. We show that for any set of beliefs about future asset prices, an optimal CFMM trading function exists that maximizes the fraction of trades that a CFMM can settle. We formulate a convex program to compute this optimal trading function. This program gives a tractable framework for market-makers to compile their belief function on the future prices of the underlying assets into the trading function of a maximally capital-efficient CFMM. Our convex optimization framework further extends to capture the tradeoffs between fee revenue, arbitrage loss, and opportunity costs of liquidity providers.

## Key Contributions

- Demonstrates optimal CFMM trading functions exist and provides convex optimization framework to compute them
- Extends analysis to model tradeoffs among fee revenue, arbitrage loss, and liquidity provider costs
- Explains diversity of real-world CFMM designs through mathematical analysis
- Enables inference of market-maker price beliefs from CFMM structure
- Introduces novel liquidity concept for CFMMs with complex KKT condition analysis over infinite-dimensional Banach space
