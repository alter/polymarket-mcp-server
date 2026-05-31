---
title: "Bonding curve implementation for prediction markets: market making without liquidity providers and impermanent losses"
url: "https://ethresear.ch/t/bonding-curve-implementation-for-prediction-markets-market-making-without-liquidity-providers-and-impermanent-losses/8046"
source: "ethresear.ch"
date: "2020-09-29"
type: "blog"
theme: "amm"
lang: "en"
---

# Bonding curve implementation for prediction markets: market making without liquidity providers and impermanent losses

**Author:** kohshiba

**Source:** Ethereum Research Forum

## Core Mechanism

Combines two approaches:
1. **Parimutuel betting** — ensures constant liquidity without requiring liquidity providers (the pool is always the counter-party)
2. **Bonding curve dynamic pricing** — price movement incentivizes early, accurate predictions

This hybrid creates prediction markets that automatically adjust prices based on total token supply, eliminating the need for external liquidity providers.

## Key Innovation

Rather than fixed odds, the system uses bonding curves to implement price movement where:
- Early, accurate predictions are incentivized more than late ones
- The mechanism eliminates impermanent losses that plague CPMMs like Uniswap
- In prediction markets, impermanent losses become *permanent* (not temporary) because prices converge to 0 or 1 at resolution

## Market Dynamics

Participants buy shares when they believe market probabilities underestimate actual outcomes. If a market shows 50% odds but someone assesses 80% likelihood, they see profit opportunity. The market aggregates these information signals similarly to traditional AMMs.

## Comparative Advantages

- Greater liquidity than order book systems (no matching required)
- No impermanent losses (especially significant for prediction markets where losses are permanent)
- Works without external LPs
- Cold-start liquidity — market functions from day 0

## Implementation

The author implemented this mechanism in Forecastory. Key distinction from traditional AMMs: the parimutuel structure means all participants share the pool at resolution, with the bonding curve determining price dynamics during the lifetime of the market.
