---
title: "Gnosis Conditional Token Framework (CTF): Tokenizing Potential Outcomes in Prediction Markets"
url: "https://medium.com/thecapital/exploring-the-gnosis-conditional-token-framework-27744fca2558"
source: medium.com
date: "2021-01-01"
type: article
theme: depm
lang: en
---

# Gnosis Conditional Token Framework (CTF): Tokenizing Potential Outcomes in Prediction Markets

**Source:** The Capital / Medium  
**URL:** https://medium.com/thecapital/exploring-the-gnosis-conditional-token-framework-27744fca2558  
**Also see:** https://hackernoon.com/gnosis-conditional-token-framework-ctf-tokenizing-potential-outcomes-in-prediction-markets

## Overview

The Gnosis Conditional Token Framework (CTF) implements a codebase for tokenizing potential outcomes in prediction markets. It allows anyone to create crypto assets that represent information about future events with conditional outcomes. Gnosis developed the CTF as a groundbreaking standard that allowed for complex, conditional outcomes in prediction markets.

## Core Problem Solved

Traditional prediction market platforms (including Gnosis's first contracts and Augur) lacked sufficient infrastructure to support deeper combinatorial markets. They created conditional tokens in sequential layers, and tokens representing identical conditions would become different assets depending on construction order. The CTF solves this by consolidating all conditions within a single contract, decoupling them from specific collateral tokens. This architecture preserves fungibility across layers.

## Technical Foundation

Conditional tokens are built on the **ERC-1155 token standard**, which affords numerous advantages:
- ERC-1155 batch sends substantially decrease gas costs
- Multi-token standard: manages thousands of unique outcome tokens within a single contract
- Enables minting many tokens under each tokenId

To create a market, users first lock collateral (e.g., DAI or USDC) into a contract to mint conditional tokens. A market question is defined with a collection of possible outcomes. For each unit of collateral committed, participants receive conditional tokens representing all potential outcomes.

## Key Operations

- **Prepare**: Register new events with oracles, define condition IDs
- **Split**: Convert base assets into outcome-specific tokens
- **Merge**: Reverse the process, exit positions by combining complementary tokens back to collateral
- **Redeem**: Claim winnings after oracle resolution

## Combinatorial and Conditional Markets

The CTF allows users to make simple markets on the likelihood of a given event, as well as complex markets about how the likelihood of one event is affected by another:
- "What is the probability of a global recession if a trade war breaks out between the United States and China?"
- "What will Bitcoin's price be if Trump wins the election?"

Each combination of outcomes is called a **position**, and each position can represent a user's prediction for several events at once.

## Market Makers

Two approaches exist within the CTF ecosystem:
1. **CPMM (Constant Product Market Maker)** — uses the familiar "x * y = k" formula from Uniswap
2. **LMSR (Logarithmic Market Scoring Rule)** — employs logarithmic functions specifically designed for prediction markets, offering superior risk management

## Platforms Built on CTF

- **Polymarket** — uses CTF for all outcome tokenization, plus the NegRiskAdapter for multi-outcome markets
- **Omen** — originally built by Gnosis, later managed by DXdao; uses fixed product market maker
- **Presagio** — "Omen 2.0" with AI agent integration

## Polymarket CTF Integration (2026)

Polymarket announced CTF Exchange V2 and a new collateral token, Polymarket USD, on April 6, 2026. The upgrade replaces bridged USDC.e with a wrapped 1:1 USDC-backed collateral token (Polymarket USD) and adds EIP-1271 support for smart contract wallets.

## Negative Risk (Multi-Outcome) Markets

For elections with candidates A, B, and C: each candidate has a binary YES/NO market, and exactly one candidate will win. The NegRiskAdapter allows a position of one or more NO tokens to be converted to the equivalent position of YES tokens plus some amount of USDC. USDC is wrapped into WrappedCollateral to enable this mechanism.

## Documentation References

- https://conditional-tokens.readthedocs.io/en/latest/motivation.html
- https://conditionaltokens-docs.dev.gnosisdev.com/conditionaltokens/docs/introduction1/
- https://github.com/gnosis/conditional-tokens-market-makers
