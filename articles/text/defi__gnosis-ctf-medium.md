---
title: "Exploring the Gnosis Conditional Token Framework"
url: https://medium.com/thecapital/exploring-the-gnosis-conditional-token-framework-27744fca2558
source: medium.com
date: "2026-05-30"
type: article
theme: defi
lang: en
---

# Gnosis Conditional Token Framework

## Overview

The article explores how Gnosis implements a system for tokenizing potential outcomes in blockchain-based prediction markets. As stated, "Such markets are often referred to as information markets, idea futures, decision markets, or virtual stock markets."

## Core Concept

Prediction markets allow users to bet on event outcomes by purchasing share tokens. The framework enables combining multiple predictions into single positions, reducing computational complexity from 8 variations to 4 consolidated positions when dealing with two interconnected events.

## Key Components

**Conditional Tokens**: Built on ERC-1155 standard, these tokens represent stakes on specific outcomes. The system uses three identifiers:
- **ConditionId**: Hash of oracle address, question ID, and outcome count
- **IndexSet**: Bit array encoding which outcomes are true (represented as binary)
- **CollectionId**: Unique identifier combining IndexSet and ConditionId

**Core Operations**:
- *Prepare*: Register new prediction with oracle
- *Report*: Oracle submits outcome results
- *Split*: User deposits collateral to purchase outcome tokens
- *Merge*: User withdraws by combining positions back to base asset
- *Redeem*: Claim rewards after resolution

## Market Makers

The framework supports two pricing mechanisms:

**CPMM (Constant Product)**: Uses formula "x * y = k," identical to Uniswap pools, prioritizing mathematical simplicity.

**LMSR (Logarithmic Market Scoring Rule)**: Originally designed for prediction markets, employing logarithmic functions. As noted, "it allows for better risk and volatility control."

## Use Cases

While prediction markets represent the primary application, potential uses include GameFi rewards systems, conditional payment agreements, and derivative options.

## Real-World Implementation

Polymarket demonstrates successful CTF implementation, building additional features like limit orders on top of the framework's foundation.
