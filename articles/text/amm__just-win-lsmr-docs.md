---
title: "LSMR & LS-LSMR - AMMs For Prediction Markets"
url: "https://docs.just.win/docs/overview/lsmr/"
source: "docs.just.win"
date: "2023-01-01"
type: "blog"
theme: "amm"
lang: "en"
---

# LSMR & LS-LSMR - AMMs For Prediction Markets

**Source:** just.win documentation

## LS-LMSR Algorithm Properties

The LS-LMSR algorithm has several desirable properties for prediction market AMMs:

- Can be used as a price oracle for tokens using the conditional token framework (CTF)
- Bounded loss can be set as a parameter — downside to liquidity providers is limited
- Overrounding can be set as a parameter — the main function that helps LPs achieve profits
- Closed-form expressions for buying and selling allow calculating net cost for a batch of buys and sells simultaneously

## LS-LMSR vs CPMM for Prediction Markets

The CPMM (constant product AMM like Uniswap) does not admit a closed-form expression for the prediction market use case, so buying and selling is limited on-contract to one outcome token at a time. LS-LMSR can handle batch operations.

## Key Practical Advantages

For prediction market bootstrapping:
- Spreads below 2% even at bootstrap stage
- Bonding curves handle cold-start scenarios without external market makers
- Dynamic liquidity depth adjustment based on trading volume

## Technical Stack

A full prediction market AMM stack can cover:
- Solidity smart contracts
- Oracle integrations (UMA/Chainlink)
- AMM liquidity mechanisms (LS-LMSR, bonding curves)
- ERC-4337 account abstraction
- EVM-compatible chains
