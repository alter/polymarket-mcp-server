---
title: "Augur's Automated Market Maker: The LS-LMSR"
url: "https://augur.mystrikingly.com/blog/augur-s-automated-market-maker-the-ls-lmsr"
source: "augur.mystrikingly.com"
date: "2020-01-01"
type: "blog"
theme: "amm"
lang: "en"
---

# Augur's Automated Market Maker: The LS-LMSR

**Source:** Augur Blog

## What is the LS-LMSR?

The Liquidity-Sensitive LMSR (LS-LMSR) is a modification of LMSR designed by Othman et al. (2013). It makes the liquidity parameter b a function of market depth rather than a constant:

- In LMSR: b is constant — prices move the same amount regardless of market size
- In LS-LMSR: b scales with wagered amounts — thin markets are price-sensitive, thick markets are stable

## Why Augur Used an AMM

1. **Solves buyer/seller matching problem** — the AMM is always available to trade, even at market initiation, even at 2 AM
2. **Enables large multi-outcome markets** — presidential elections with dozens of candidates; AMM can price all simultaneously
3. **Creates participation incentives** — traders can always obtain liquidity

## The Problem with Plain LMSR

Setting the constant b requires knowing trader interest before trading begins — impossible in a decentralized context. The Gates Hillman Prediction Market (CMU) set b=32, which was far too low, causing price spikes exploited by sophisticated traders.

## LS-LMSR vs. CPMM

| Property | LS-LMSR | Constant Product (Uniswap) |
|---|---|---|
| AMM design basis | Prediction markets | Token exchange |
| Bounded loss | Yes | No (runs at profit in theory) |
| Computation | Complex (log + exp) | Simple (multiplication) |
| Multi-outcome | Natural | Requires separate pools |
| Gas cost | High | Low |

## Why Augur Moved Away

Augur v2 shifted to off-chain order matching via 0x Mesh because the LS-LMSR gas costs were prohibitive on Ethereum mainnet.
