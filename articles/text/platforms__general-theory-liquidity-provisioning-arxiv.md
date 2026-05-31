---
title: "A General Theory of Liquidity Provisioning for Prediction Markets"
url: "https://arxiv.org/pdf/2311.08725"
source: arxiv
date: "2023-11"
type: paper
theme: platforms
lang: en
---

# A General Theory of Liquidity Provisioning for Prediction Markets

Authors: Bhaskara, Frongillo, Papireddygari
ArXiv: 2311.08725v2

## Core Contribution
Unified theoretical framework for liquidity provisioning in prediction markets, connecting disparate market-making approaches under common mathematical foundations. Draws inspiration from how AMMs implement liquidity provisioning in DeFi, allowing third-party liquidity providers (LPs) to provide assets to facilitate trade in exchange for fees.

## Key Innovation
Decouples the roles of:
- **Market mechanism** (facilitates trade)
- **Liquidity providers** (take on risk to stabilize prices)

LPs deposit assets in exchange for a cut of the fees charged on trades using those assets.

## AMM Designs Analyzed
- **Discrete bucket systems**: Traditional approach with predetermined outcome buckets
- **Soft bucket mechanisms**: Continuous functions enabling richer liquidity distributions
- **Liquidity curves**: Mathematical functions determining price discovery and execution costs

## Fee Structures & Practical Implications
- Fee structures directly impact LP profitability and market efficiency
- Market-making involves tradeoffs: capital efficiency vs. risk exposure vs. fee collection
- Budget balance: sustainable fees that compensate LPs without making prediction markets prohibitively expensive

## Relevance to DeFi
Applies to decentralized prediction platforms (Manifold Markets, Minswap) and broader AMM design where gas efficiency and automated execution create distinct constraints versus traditional prediction markets.

## Practical Takeaway for Traders
LP positions in prediction market AMMs behave like underwriting risk, not pure spread capture. LPs who provide liquidity on one-sided retail flow earn consistent returns, but face directional exposure on informed flow — mirroring the dynamics described in the Kalshi maker/taker literature.
