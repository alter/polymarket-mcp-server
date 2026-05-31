---
title: "Log-time Prediction Markets for Interval Securities"
url: "https://arxiv.org/abs/2102.07308"
source: "arxiv"
date: "2021-02-15"
type: "paper"
theme: "amm"
lang: "en"
---

# Log-time Prediction Markets for Interval Securities

**Authors:** Miroslav Dudik, Xintong Wang, David M. Pennock, David M. Rothschild

**arXiv ID:** 2102.07308

## Abstract

We design a prediction market to recover a complete and fully general probability distribution over a random variable. Traders buy and sell interval securities that pay $1 if the outcome falls into an interval and $0 otherwise. Our market takes the form of a central automated market maker and allows traders to express interval endpoints of arbitrary precision. We present two designs in both of which market operations take time logarithmic in the number of intervals (that traders distinguish), providing the first computationally efficient market for a continuous variable. Our first design replicates the popular logarithmic market scoring rule (LMSR), but operates exponentially faster than a standard LMSR by exploiting its modularity properties to construct a balanced binary tree and decompose computations along the tree nodes. The second design consists of two or more parallel LMSR market makers that mediate submarkets of increasingly fine-grained outcome partitions.

## Key Contributions

- First computationally efficient prediction market for continuous variables with O(log n) time operations
- Novel binary tree optimization enabling exponential speedup over standard LMSR
- Parallel market design with differential liquidity allocation
- Ability to express utility for information at various resolutions by assigning different liquidity values
- Guaranteed true constant bounded loss by decreasing liquidity in each submarket
- Computationally efficient arbitrage removal across submarkets
