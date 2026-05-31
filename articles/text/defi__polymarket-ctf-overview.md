---
title: "Conditional Token Framework - Polymarket Documentation"
url: https://docs.polymarket.com/trading/ctf/overview
source: docs.polymarket.com
date: "2026-05-30"
type: documentation
theme: defi
lang: en
---

# Conditional Token Framework on Polymarket

## Overview

The Conditional Token Framework (CTF) is "an open standard developed by Gnosis" that tokenizes all Polymarket outcomes using ERC1155 tokens. Each binary market generates two tokens representing possible event outcomes.

## Token Structure

Binary markets produce paired tokens with full collateralization:

- **Yes Token**: Redeems for $1.00 pUSD if the event occurs
- **No Token**: Redeems for $1.00 pUSD if the event does not occur

Every token pair is "always fully collateralized — every Yes/No pair is backed by exactly $1.00 pUSD locked in the CTF contract."

## Core Operations

The framework enables three fundamental functions:

1. **Split**: Converting pUSD into Yes and No token pairs
2. **Merge**: Converting token pairs back into pUSD
3. **Redeem**: Exchanging winning tokens for pUSD after market resolution

## Token Identification

Position IDs are computed through three sequential steps:

1. **Condition ID**: Derived from the oracle address, question ID hash, and outcome count (always 2 for binary markets)
2. **Collection IDs**: Generated using parent collection ID (zero for top-level), condition ID, and index set bitmask (1 or 2 for binary outcomes)
3. **Position IDs**: Created by combining the collateral token (pUSD) with collection IDs

## Market Variants

Polymarket operates two CTF configurations:

- **Standard Markets**: Use the CTF Exchange with independent outcome tokens
- **Neg Risk Markets**: Employ the Neg Risk CTF Exchange with linked multi-outcome conversion capabilities
