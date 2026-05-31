---
title: "How Does Polymarket Work | The Tech Behind Prediction Markets"
url: https://rocknblock.io/blog/how-polymarket-works-the-tech-behind-prediction-markets
source: rocknblock.io
date: "2026-05-30"
type: article
theme: defi
lang: en
---

# How Polymarket Works Technically

## Core Architecture

Polymarket operates as "a protocol built around prediction markets or event markets" where users predict outcomes of future events. The system combines off-chain and on-chain components in a hybrid-decentralized structure.

## Event Markets & Share Mechanics

The platform uses binary outcomes—YES and NO shares for each event. "Share prices range from $0 to $1 and always sum to $1 between the two outcomes." For example, if YES trades at $0.36, NO trades at $0.64, representing the market's probability assessment.

## Order Book System

Polymarket employs a Central Limit Order Book (CLOB). "All orders are collected in a single place. On Polymarket, each event has its own dedicated order book." The system features **mirrored orders**—when a user places a buy order for YES shares, the system automatically displays an equivalent sell order for NO shares at the inverse price.

## Price Discovery

"Initial prices are formed when buy/sell orders match and shares are minted." The market price is calculated as "the midpoint of buy/sell spreads (if tight) or last trade (if wide)." Prices emerge entirely from supply and demand dynamics.

## Order Execution Types

Three primary mechanisms exist:

1. **Direct Match**: User-to-user trades without creating or destroying shares
2. **Minting**: "New shares are created when orders for opposite outcomes match in price"
3. **Merging**: Shares are "burned when opposite sell orders match"

## Smart Contract Architecture

### Key Contracts

**CTFExchange.sol** serves as the execution layer, handling "order execution after they are matched off-chain." It validates signatures using EIP-712 standard and manages fees.

**Trading.sol** contains "core exchange logic: receives assets, delivers assets, performs validation checks, and emits events."

**Assets.sol** defines tradable assets including USDC and market shares.

## Token Implementation

Shares use the **ERC-1155 standard** through Gnosis's Conditional Tokens Framework (CTF). Each market requires three parameters:

- **questionId**: "IPFS hash of the market question"
- **outcomeSlotCount**: Always 2 for binary markets on Polymarket
- **oracle address**: UMA's optimistic oracle for resolution

These parameters generate a **conditionId** through keccak256 hashing.

## Collateral & Settlement

"USDC is deposited to mint shares. Shares are burned to redeem USDC after market resolution." Only winning outcome holders receive payouts; losing shares become worthless.

## Oracle Resolution (UMA)

The system uses "a decentralized optimistic oracle." The process works as follows:

1. Users propose outcomes by posting a $750 bond
2. If unchallenged within 2 hours, proposals are accepted
3. Disputes trigger UMA tokenholder voting
4. Final outcomes are posted via reportPayouts
5. Users redeem shares through redeemPositions

## Order Types

**Limit orders** specify a price and wait for execution, while **market orders** "execute immediately at the best available price." Market orders can experience partial fills across multiple price levels depending on order book depth.

## Off-Chain to On-Chain Flow

Users create signed orders off-chain using EIP-712. "A backend matches orders off-chain" before triggering on-chain execution through smart contracts. This approach enables cancellations and updates without blockchain transactions.

## Three-Layer Architecture

The complete system integrates three major protocols:

**CTF**: Manages outcome combinatorics and user position representation

**CLOB**: Polymarket's proprietary order matching and limit order system

**UMA**: Decentralized oracle providing trustless result settlement through game-theoretic dispute resolution
