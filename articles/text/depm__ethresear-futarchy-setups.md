---
title: "Possible Futarchy Setups on Ethereum"
url: "https://ethresear.ch/t/possible-futarchy-setups/1820"
source: ethresear.ch
date: "2018-04-01"
type: forum
theme: depm
lang: en
---

# Possible Futarchy Setups on Ethereum

**Author:** Chris Whinfrey  
**Forum:** Ethereum Research (ethresear.ch)  
**URL:** https://ethresear.ch/t/possible-futarchy-setups/1820  
**Date:** April 2018  
**Related threads:**  
- Futarchy with Bonding Curve Tokens: https://ethresear.ch/t/futarchy-with-bonding-curve-tokens/3449  
- Governance Mixing Auctions and Futarchy: https://ethresear.ch/t/governance-mixing-auctions-and-futarchy/10772

## Overview

This post introduces two prediction market approaches for implementing futarchy in decentralized governance, specifically for a DAO deciding whether to accept proposals based on predicted token price impacts.

## Three Prediction Market Types Reviewed

**Categorical markets:** Feature binary or multiple outcomes. "Will the groundhog see his shadow?" with YES/NO tokens. Each market has: a question, a collateral token, and at least two outcome tokens.

**Scalar markets:** Predict numerical values within specified ranges using long and short tokens. Predicting gold prices with upper and lower bounds allows proportional token redemption based on actual prices.

**Conditional markets:** Layer predictions on existing categorical markets. Use outcome tokens from base markets as collateral, enabling "if-then" predictions about dependent variables.

## Two Futarchy Models Proposed

### Model 1: Categorical + Conditional Scalar

- One categorical market about proposal acceptance
- Two conditional scalar markets predicting token prices under each scenario
- Enables measuring diverse success metrics
- Requires defining precise price bounds in advance
- More expressive but more complex to implement

### Model 2: Two Categorical Markets with Different Collateral

- Two categorical markets using different collateral tokens (ETH and governance token)
- Price ratios represent conditional valuations without requiring predefined bounds
- Simpler implementation
- Restricts optimization to token value only

## Discussion Highlights from Comments

- Futarchy works best in "smaller ecosystems where the value of a token depends on a narrow range of factors," like token-curated registries
- Concerns about confounding variables: other events can affect the price metric during the decision window
- Market manipulation vulnerabilities: large actors could move conditional prices

## Related ethresear.ch Posts

**Futarchy with Bonding Curve Tokens (September 2018):** Proposes a self-contained futarchy mechanism for bonding curve tokens. Notes that decentralized exchanges are likely the best price-finding solution for futarchy markets.

**Governance Mixing Auctions and Futarchy (September 2021):** Analyzes 3 group decision-making protocols—voting, auctions, and futarchy—and describes a hybrid auction-prediction mechanism. Asset futarchy is argued to solve trustless joint ownership by making treasury raids economically irrational.

## Significance

These ethresear.ch discussions represent the practical Ethereum ecosystem engagement with Robin Hanson's futarchy theory, bridging academic concept to on-chain implementation. They directly influenced Zeitgeist's governance design and Vitalik's 2024/2026 governance proposals.
