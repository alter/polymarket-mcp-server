---
title: "Mechanism Design for Automated Market Makers"
url: "https://arxiv.org/abs/2402.09357"
source: "arxiv"
date: "2024-02-14"
type: "paper"
theme: "amm"
lang: "en"
---

# Mechanism Design for Automated Market Makers

**Authors:** T-H. Hubert Chan, Ke Wu, Elaine Shi

**arXiv ID:** 2402.09357 | Last revised September 13, 2025

## Abstract

Blockchains have popularized automated market makers (AMMs). An AMM exchange is an application running on a blockchain which maintains a pool of crypto-assets and automatically trades assets with users governed by some pricing function that prices the assets based on their relative demand/supply. AMMs have created an important challenge commonly known as the Miner Extractable Value (MEV). In particular, the miners who control the contents and ordering of transactions in a block can extract value by front-running and back-running users' transactions, leading to arbitrage opportunities that guarantee them risk-free returns. This paper considers how to design AMM mechanisms that eliminate MEV opportunities by processing all transactions within a block in a batch.

## Key Contributions

- A batch-processing AMM mechanism designed to eliminate MEV opportunities
- Arbitrage resilience guarantees: a miner cannot gain risk-free profit for legacy blockchains
- Fair treatment among all transactions within the same block, preventing miners from selling off favorable positions
- Incentive compatibility proofs for decentralized sequencing-fair blockchains — any individual user's best response is to follow honest strategy

## Two Tiers of Guarantees

1. For legacy blockchains (single miner per block): arbitrage resilience + fair treatment
2. For blockchains with decentralized sequencing-fairness: stronger incentive compatibility
