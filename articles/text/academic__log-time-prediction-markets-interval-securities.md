---
title: "Log-time Prediction Markets for Interval Securities"
url: "https://arxiv.org/abs/2102.07308"
source: "arxiv"
date: "2021-02-01"
type: "paper"
theme: "academic"
lang: "en"
authors: ["Miroslav Dudík", "Xintong Wang", "David M. Pennock", "David M. Rothschild"]
---

# Log-time Prediction Markets for Interval Securities

## Abstract

The paper presents a prediction market designed to recover probability distributions over random variables using interval securities — financial instruments paying $1 if outcomes fall within specified intervals, $0 otherwise. The researchers developed two market designs featuring logarithmic time operations in the number of intervals, achieving the first computationally efficient market for a continuous variable.

## Key Findings

1. **Design Innovation:** The first design replicates the logarithmic market scoring rule (LMSR) while operating exponentially faster by using a balanced binary tree structure to decompose computations.

2. **Parallel Architecture:** The second design employs multiple parallel LMSR market makers handling increasingly fine-grained outcome partitions, maintaining computational efficiency across all operations.

3. **Practical Benefits:** The approach enables market designers to express preferences for information at various resolutions through differentiated liquidity values and guarantees bounded losses by adjusting liquidity across submarkets.

Published at AAMAS 2021.
