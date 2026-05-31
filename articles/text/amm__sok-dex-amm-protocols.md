---
title: "SoK: Decentralized Exchanges (DEX) with Automated Market Maker (AMM) Protocols"
url: "https://arxiv.org/abs/2103.12732"
source: "arxiv"
date: "2021-03-23"
type: "paper"
theme: "amm"
lang: "en"
---

# SoK: Decentralized Exchanges (DEX) with Automated Market Maker (AMM) Protocols

**Authors:** Jiahua Xu, Krzysztof Paruch, Simon Cousaert, Yebo Feng

**arXiv ID:** 2103.12732 | Last revised March 14, 2023 | Published in ACM Computing Surveys

## Abstract

As an integral part of the decentralized finance (DeFi) ecosystem, decentralized exchanges (DEXs) with automated market maker (AMM) protocols have gained massive traction with the recently revived interest in blockchain and distributed ledger technology (DLT) in general. Instead of matching the buy and sell sides, automated market makers (AMMs) employ a peer-to-pool method and determine asset price algorithmically through a so-called conservation function. We create the first systematization of knowledge in this area, establishing a general AMM framework describing the economics and formalizing the system's state-space representation, then systematically compare the top AMM protocols' mechanics, illustrating their conservation functions, slippage and divergence loss functions.

## Key Contributions

- First systematization of knowledge on AMM-based DEXs
- General AMM framework with economic analysis and state-space formalization
- Comparative analysis of leading AMM protocols: Uniswap V2/V3, Balancer, Curve, DODO
- Slippage and divergence (impermanent) loss function analysis
- Security and privacy risk assessment with mitigation strategies
- Notes: prediction markets use LMSR while DEXs use CFMMs (constant sum, constant product)

## Protocol Comparisons

- Uniswap V2: rudimentary bonding curve, low gas fees
- Uniswap V3: concentrated liquidity, improved capital efficiency
- Balancer: multi-asset pools (>2 assets)
- Curve: optimized for same-peg assets (stablecoins)
- DODO: proactively reduces divergence loss via external prices
