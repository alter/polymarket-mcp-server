---
title: "A General Theory of Liquidity Provisioning for Prediction Markets"
url: "https://arxiv.org/abs/2311.08725"
source: "arxiv"
date: "2023-11-15"
type: "paper"
theme: "amm"
lang: "en"
---

# A General Theory of Liquidity Provisioning for Prediction Markets

**Authors:** Adithya Bhaskara, Rafael Frongillo, Maneesha Papireddygari

**arXiv ID:** 2311.08725 | Last revised July 28, 2025

## Abstract

In Decentralized Finance (DeFi), automated market makers typically implement liquidity provisioning protocols. These protocols allow third-party liquidity providers (LPs) to provide assets to facilitate trade in exchange for fees. This paper introduces a general framework for liquidity provisioning for cost-function prediction markets with any finite set of securities. The framework is based on the idea of running several market makers "in parallel"; it is shown formally that several notions of parallel market making are equivalent. The most general protocol allows LPs to submit an arbitrary cost function, which specifies their liquidity over the entire price space, and determines the deposit required. The need for this flexibility is justified by demonstrating the inherent high dimensionality of liquidity. The paper recovers several existing DeFi protocols in the 2-asset case and provides a fully expressive protocol for any number of assets.

## Key Contributions

- First general framework for LP provisioning in cost-function prediction markets with any number of securities
- Demonstrates equivalence between several notions of parallel market-making
- Establishes protocols allowing flexible cost function submissions by LPs
- Proves inherent high-dimensionality of liquidity space
- Addresses computational feasibility through restricted protocol variants
- Recovers existing DeFi protocols as special cases
