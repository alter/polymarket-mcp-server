---
title: "Negative Risk Markets - Polymarket Documentation"
url: https://docs.polymarket.com/advanced/neg-risk
source: docs.polymarket.com
date: "2026-05-30"
type: documentation
theme: defi
lang: en
---

# Polymarket Negative Risk Mechanism

## Core Concept

Negative risk represents a capital-efficient trading mechanism for multi-outcome events where only one outcome can ultimately win. The innovation allows positions across different outcomes to be related through a conversion operation.

## Fundamental Mechanism

In standard markets, each outcome operates independently. However, negative risk introduces a crucial relationship: "A **No share** in any market can be converted into **1 Yes share in every other market**" through the Neg Risk Adapter contract.

### Practical Example

Using a presidential election scenario with three candidates, holding one No token on "Other" converts directly into one Yes token for both remaining candidates. This structure makes betting against a single outcome economically equivalent to betting for all alternatives.

## Implementation Details

### API Recognition

Events and markets include a `negRisk` boolean flag in the Gamma API response, allowing developers to identify which markets require special handling.

### Order Placement

When creating orders on these markets, the SDK requires explicit specification: `negRisk: true` must be included in order options alongside standard parameters like token ID, price, and size.

### Technical Architecture

The conversion operates atomically through the Neg Risk Adapter contract. Users holding No tokens for one outcome can trigger this conversion to receive Yes tokens across all other outcomes within that event.

## Augmented Variant

An advanced implementation addresses scenarios where outcomes emerge after trading begins:

- **Named outcomes**: Pre-established positions
- **Placeholder outcomes**: Reserved slots clarifiable later
- **Explicit Other**: Captures unnamed outcomes at resolution

The critical warning emphasizes trading only on named outcomes, as placeholder and Other categories carry resolution uncertainty.

## Capital Efficiency

In standard markets, to buy all outcomes, you need collateral equal to the sum of all prices. In negRisk markets, to buy all outcomes, you only need 1.0 unit of collateral.

### Convert Arbitrage

Convert arbitrage occurs when the sum of all YES prices in a negative risk set is below $1. You can buy all YES tokens for less than $1, and exactly one must resolve at $1, guaranteeing a profit if held to resolution.

Market rebalancing within multi-condition negRisk markets has been found to generate 73% of total arbitrage profits despite representing only 8.6% of opportunities — revealing a 29× capital efficiency advantage over binary arbitrage.

## Technical Contract Architecture

In order to allow USDC to be released as part of a conversion from a NO position to a YES position, USDC is wrapped into WrappedCollateral, which is then used to collateralize all underlying markets. This enables the NegRiskAdapter to manage USDC separately from the ConditionalTokens contract.

The Vault holds USDC and Yes tokens which are collected as fees from users who choose to convert NO positions, given a positive fee rate.

All trading activity on Polymarket is settled through two main contracts: the CTF Exchange for simple binary markets (YES/NO), and the NegRisk_CTFExchange for more complex, multi-outcome markets.

## Contract Addresses on Polygon

- **NegRisk_CTFExchange**: `0xC5d563A36AE78145C45a50134d48A1215220f80a`
- **NegRiskAdapter**: `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296`
