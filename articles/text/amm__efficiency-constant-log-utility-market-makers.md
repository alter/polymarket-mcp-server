---
title: "Efficiency of Constant Log Utility Market Makers"
url: "https://arxiv.org/abs/2510.12952"
source: "arxiv"
date: "2025-10-14"
type: "paper"
theme: "amm"
lang: "en"
---

# Efficiency of Constant Log Utility Market Makers

**Authors:** Maneesha Papireddygari, Xintong Wang, Bo Waggoner, David M. Pennock

**arXiv ID:** 2510.12952

## Abstract

Automated Market Makers (AMMs) are used to provide liquidity for combinatorial prediction markets that would otherwise be too thinly traded. They offer both buy and sell prices for any of the doubly exponential many possible securities that the market can offer. The problem of setting those prices is known to be #P-hard for the original and most well-known AMM, the logarithmic market scoring rule (LMSR) market maker. We focus on another natural AMM, the Constant Log Utility Market Maker (CLUM). Unlike LMSR, whose worst-case loss bound grows with the number of outcomes, CLUM has constant worst-case loss, allowing the market to add outcomes on the fly and even operate over countably infinite many outcomes. Simpler versions of CLUM underpin several Decentralized Finance (DeFi) mechanisms including the Uniswap protocol. We first establish that pricing securities is #P-hard for CLUM, via a reduction from the model counting 2-SAT problem. We propose an approximation algorithm for pricing securities that works with high probability, and show this oracle can be implemented in polynomial time when restricted to interval securities.

## Key Contributions

1. Proves pricing securities in CLUM is computationally #P-hard
2. Key advantage: CLUM has constant worst-case loss (unlike LMSR which grows with outcomes)
3. CLUM can operate over countably infinite outcomes — allows adding outcomes dynamically
4. Proposes practical approximation algorithm for security pricing
5. Demonstrates polynomial-time oracle implementation for interval securities
6. Connects CLUM to Uniswap (simpler CLUM versions underpin Uniswap)
