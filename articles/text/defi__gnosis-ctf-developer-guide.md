---
title: "Developer Guide — Conditional Tokens 1.0.3 documentation"
url: https://conditional-tokens.readthedocs.io/en/latest/developer-guide.html
source: conditional-tokens.readthedocs.io
date: "2026-05-30"
type: documentation
theme: defi
lang: en
---

# Gnosis Conditional Tokens: Developer Guide

## Overview

This guide explains how to use the ConditionalTokens smart contract, which enables prediction market infrastructure through conditional token creation and management.

## Installation

```
npm i '@gnosis.pm/conditional-tokens-contracts'
```

## Conditions Framework

### Preparation Phase

Conditions are established by calling `prepareCondition()`, which requires:
- An oracle address (responsible for reporting outcomes)
- A question identifier (interpreted by client applications)
- An outcome slot count (maximum 256 slots)

The system generates "a condition ID via keccak256(abi.encodePacked(oracle, questionId, outcomeSlotCount))". The function initializes a payout vector and emits a `ConditionPreparation` event.

### Examples

**Categorical conditions** support multiple mutually-exclusive choices. For instance, "Who will be chosen: Alice, Bob, or Carol?" would use three outcome slots.

**Scalar conditions** represent value ranges. A "What will the score be? [0, 1000]" question uses two slots representing endpoints, where payout ratios indicate precise values.

## Outcome Collections

Collections represent "nonempty proper subsets of a condition's outcome slots." They cannot be empty or include all slots.

Representation uses 256-bit index sets (bitmasks) where each bit position indicates slot inclusion. For example, `0b011` represents slots A and B combined.

Collection identifiers derive from hashing condition ID and index set coordinates on the alt_bn128 elliptic curve, with helper functions available via `getCollectionId()`.

## Positions

Positions combine collateral tokens with outcome collections. A position's value equals "a product of the values of those outcome collections composing the position."

Position IDs hash together the collateral token address and combined collection ID. The system supports positions dependent on multiple conditions simultaneously, forming a directed acyclic graph structure.

## Splitting and Merging Operations

### Splitting

`splitPosition()` either converts collateral into shallow positions or burns deeper position tokens to create even deeper ones. Valid partitions must be nontrivial (excluding empty sets and complete sets) and disjoint.

The function accepts flexible partitions—not limited to single-slot divisions—enabling positions like splitting into "(B)" and "(A|C)" simultaneously.

### Merging

`mergePositions()` reverses splitting operations, burning deeper tokens to obtain shallower ones or recovering collateral.

## Token Management

The contract implements ERC1155 multitoken standards, using position IDs as token indices. Functions include:
- `balanceOf()` for querying holdings
- `safeTransferFrom()` and `safeBatchTransferFrom()` for transfers
- `setApprovalForAll()` for operator authorization

Recipients must implement `ERC1155TokenReceiver` interface to accept transfers.

## Resolution and Redemption

Oracles call `reportPayouts()` to submit outcome predictions, emitting a `ConditionResolution` event. Subsequently, holders redeem positions through `redeemPositions()`, receiving collateral based on the reported payout vector.

The payout vector numerators represent fractional payouts—for example, "[0.5, 0.5, 0]" uses fractions like "1/2" rather than decimals for precision within Ethereum's constraint system.

## Mathematical Foundation

The positionId derivation formula:
```
conditionId = keccak256(abi.encodePacked(oracle, questionId, outcomeSlotCount))
collectionId = keccak256(abi.encodePacked(parentCollectionId, conditionId, indexSet)) [on alt_bn128]
positionId = keccak256(abi.encodePacked(collateralToken, collectionId))
```
