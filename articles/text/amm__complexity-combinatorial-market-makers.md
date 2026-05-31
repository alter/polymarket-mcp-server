---
title: "Complexity of Combinatorial Market Makers"
url: "https://arxiv.org/abs/0802.1362"
source: "arxiv"
date: "2008-02-11"
type: "paper"
theme: "amm"
lang: "en"
---

# Complexity of Combinatorial Market Makers

**Authors:** Yiling Chen, Lance Fortnow, Nicolas Lambert, David M. Pennock, Jennifer Wortman

**arXiv ID:** 0802.1362 | Published at ACM EC 2008

## Abstract

We analyze the computational complexity of market maker pricing algorithms for combinatorial prediction markets. We focus on Hanson's popular logarithmic market scoring rule market maker (LMSR). Our goal is to implicitly maintain correct LMSR prices across an exponentially large outcome space. We examine both permutation combinatorics, where outcomes are permutations of objects, and Boolean combinatorics, where outcomes are combinations of binary events. We look at three restrictive languages that limit what traders can bet on. Even with severely limited languages, we find that LMSR pricing is NP-hard, even when the same language admits polynomial-time matching without the market maker. We then propose an approximation technique for pricing permutation markets based on a recent algorithm for online permutation learning.

## Key Contributions

- Demonstrates NP-hardness of LMSR pricing across restricted betting languages
- Identifies computational complexity barriers in combinatorial prediction market design
- Proposes approximation algorithms for permutation-based markets using online permutation learning
- Establishes theoretical connections between market pricing and online learning frameworks
- Examines both permutation and Boolean combinatorics settings

## Significance

Foundational complexity result for combinatorial prediction markets. Even severely restricted betting languages remain NP-hard for LMSR pricing, motivating later work on approximations and alternative AMM designs (arXiv:2411.08972).
