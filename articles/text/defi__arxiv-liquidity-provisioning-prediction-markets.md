---
title: "A General Theory of Liquidity Provisioning for Prediction Markets"
url: https://arxiv.org/pdf/2311.08725
source: arxiv.org
date: "2026-05-30"
type: paper
theme: defi
lang: en
---

# A General Theory of Liquidity Provisioning for Prediction Markets

## Authors

Adithya Bhaskara, Rafael Frongillo, and Maneesha Papireddygari

## Overview

This paper presents a theoretical framework for understanding liquidity provision mechanisms in prediction markets. The work develops a unified mathematical theory analyzing how different liquidity provisioning mechanisms work, establishing connections between market maker designs and underlying mathematical structures — particularly convex analysis and optimization theory.

## Core Focus Areas

- Formal definitions of liquidity in automated market makers (AMMs)
- Characterization of fee structures and budget balancedness
- Analysis of various protocol designs used in decentralized finance

## Main Results

The framework reveals that many existing prediction market mechanisms can be understood through a common theoretical lens. The authors provide conditions under which different designs achieve desirable properties like:

- Proper incentive structures for traders
- Sustainable fee mechanisms
- Efficient price discovery

## Key Mechanisms Analyzed

### LMSR (Logarithmic Market Scoring Rule)

Originally designed for prediction markets, employing logarithmic functions. Allows for better risk and volatility control. Adjusts prices based on the net quantity of shares purchased for each outcome, providing automated liquidity while strictly bounding the maximum potential loss for the market maker.

### CPMM (Constant Product Market Maker)

Adapts the Constant Product Market Maker model from DeFi (as in Uniswap) for conditional tokens. In a CPMM framework, the product of the inventory balances of the different outcome tokens remains constant: `x * y = k`.

## Methodology

The paper employs rigorous mathematical analysis, utilizing convex geometry and optimization principles to characterize market maker behavior. The authors examine both discrete and continuous market models.

## Significance for DeFi

This work provides practitioners and researchers with theoretical justification for specific design choices in prediction markets and suggests principles for developing new mechanisms. The generalized framework helps explain why certain protocols function effectively while others face efficiency challenges.

## Application to Polymarket

Without lower-spread order books, permissionless prediction markets will continue to fail to capture expert knowledge on future events. The CTF enables the creation of prediction market assets that could be traded on any trading protocol, not solely on a prediction market platform.

For liquidity in the "long tail" of prediction market assets, it is necessary to have market mechanisms built for handling large numbers of unique and often illiquid tokens — the key challenge Polymarket's CLOB architecture addresses.
