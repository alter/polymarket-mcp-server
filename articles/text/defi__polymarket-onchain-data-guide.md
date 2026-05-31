---
title: "Decoding the Digital Tea Leaves: A Guide to Analyzing Polymarket's On-Chain Order Data"
url: https://yzc.me/x01Crypto/decoding-polymarket
source: yzc.me
date: "2026-05-30"
type: article
theme: defi
lang: en
---

# Guide to Analyzing Polymarket's On-Chain Order Data

## Overview

This guide teaches analysts how to interpret Polymarket's blockchain data on the Polygon network, covering smart contract mechanics, data access methods, and practical examples for understanding prediction market transactions.

## Key Smart Contract Addresses on Polygon

- **Conditional Tokens Framework (CTF)**: `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` — Core contract for creating outcome tokens using ERC-1155 standards
- **CTF Exchange**: `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` — Handles binary market settlements
- **NegRisk_CTFExchange**: `0xC5d563A36AE78145C45a50134d48A1215220f80a` — Manages complex multi-outcome markets
- **NegRiskAdapter**: `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296` — Enables portfolio rebalancing through conversions
- **UMA Oracle**: `0x6A9D222616C90FcA5754cd1333cFD9b7fb6a4F74` — Resolves market outcomes

## Token Lifecycle Operations

### Minting (Position Split)

When two bettors with opposing views place orders, the platform combines their collateral. For example, if one bettor offers 0.70 USDC for a "YES" token and another offers 0.30 USDC for the complementary "NO" token, the exchange locks 1 USDC total and distributes outcome tokens accordingly.

### Trading (Order Matching)

Existing token holders can sell positions at any time. The matching engine pairs sellers with buyers, recording transactions through `OrderFilled` and `OrdersMatched` events.

### Burning (Position Merge)

Opposite tokens can be matched against each other and destroyed, releasing collateral. "A pair of outcome tokens is burned," returning the underlying 1 USDC of backing to participants.

### Position Conversion

Multi-outcome markets support conversions that maintain price consistency without changing expected portfolio value. A trader holding "NO A" and "NO B" tokens can convert to "YES C" plus USDC collateral, achieving identical payoff profiles.

## Data Access Methods

### Direct RPC Approach

Analysts connect to Polygon RPC endpoints (public services like polygon-rpc.com or private providers like Alchemy) using Python's web3 library to query transaction logs directly.

### Event Log Analysis

Two primary event types provide transaction data:

**OrderFilled Event Fields:**
- `orderHash`: Unique order identifier
- `maker` and `taker`: Participant addresses
- `makerAssetId`/`takerAssetId`: Asset identifiers (0 for USDC, otherwise position IDs)
- `makerAmountFilled`/`takerAmountFilled`: Settlement quantities

**OrdersMatched Event:**
Links complementary buy and sell orders, providing summary data across matched positions.

## Position Management Events

**PositionSplit**: Records token minting when two bettors with opposing views place orders simultaneously.

**PositionsMerge**: Logs token burning and collateral release when opposite positions are matched.

**PositionsConverted**: Captures portfolio rebalancing in multi-outcome markets through conversion operations, using bitmask indexSet values to indicate which NO tokens convert to YES tokens.

## Practical Analysis Scenarios

### Simple Trading

Two parties exchange outcome tokens for USDC, generating one `OrdersMatched` event and two `OrderFilled` events showing complementary buy/sell orders.

### Token Minting

Both `OrderFilled` events show `makerAssetId` of 0, indicating USDC contributions from both parties to mint new outcome token pairs.

### Token Burning

Both events display non-zero `makerAssetId` values as parties sell opposing tokens, with the exchange burning them and distributing released collateral.

### Complex Multi-Party Transactions

The matching engine can simultaneously execute direct trades, partial fills, and token minting within single transactions, visible through multiple `OrderFilled` entries tied to one `OrdersMatched` event.

## ABI Decoding Process

Smart contract ABIs enable conversion of hex-encoded log data into human-readable formats:

1. Create event signature mappings from contract ABI
2. Hash event signatures to identify log types
3. Process log topics and data fields to extract structured transaction details

## Key Implementation Details

For multi-outcome markets, the NegRiskAdapter's role in token conversions is critical: "The NegRiskAdapter smart contract exists precisely to allow a user to atomically transform one portfolio composition into another with identical economic value."
