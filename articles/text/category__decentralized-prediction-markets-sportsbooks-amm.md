---
title: "Decentralized Prediction Markets and Sports Books"
url: "https://arxiv.org/html/2307.08768v3"
source: "arxiv"
date: "2025-05"
type: "academic_paper"
theme: "category"
lang: "en"
---

# Decentralized Prediction Markets and Sports Books

Amini, Bichuch & Feinstein (2025). arXiv:2307.08768

## Abstract and Motivation

The authors develop "a liquidity-based AMM structure for prediction markets" that maintains meaningful financial properties regarding betting costs and pricing mechanisms. A key innovation is enabling liquidity providers to dynamically enter or exit the market even after trading commences—a feature absent from prior centralized prediction market designs.

The motivation centers on democratizing finance through DeFi. Unlike traditional prediction markets managed by central operators, decentralized versions allow "investors to pool their own assets into the market in exchange for a fraction of the fees being collected from trading."

These markets exist for weather, political, sports, and economic forecasting.

## Core Framework: Generalized AMM Structure

The authors define an automated market maker as a utility function U mapping betting positions and cash reserves to real values. The cost for placing a bet is determined through utility indifference pricing: traders pay the minimum amount ensuring the market maker's utility doesn't decrease.

### Liquidity-Based AMMs (LBAMMs)

The paper focuses on AMMs where utility depends solely on remaining liquidity: U(π,L) := u(L·1−π). They establish three axioms:

1. Upper semi-continuity in the weak* topology
2. Strict monotonicity in available liquidity
3. Quasiconcavity preserving reasonable risk preferences

## Key Results

### Properties of Cost Functions (Theorem 3.4)

- No arbitrage: Pricing stays between payoff extremes except for riskless bets
- Liquidity-bounded loss: The AMM maintains positive remaining liquidity after any trade
- Convexity and monotonicity: Larger bets cost proportionally more
- Path independence: Sequential bets cost the same as combined trades

### Pricing Oracles (Theorem 3.6)

The framework guarantees existence of ask and bid pricing oracles with "a pricing measure sandwiched between these prices," enabling consistent probability assessments across market participants.

## Innovations

**Decentralized Liquidity Provision**: The paper formalizes how investors can inject liquidity mid-market by solving for the fractional ownership they receive, ensuring pricing oracles remain stable.

**Fee Structure**: A novel mechanism allows liquidity providers to collect profits without distorting market prices.

**Generalized Probability Spaces**: Unlike classical treatments assuming finite outcomes, this framework accommodates general probability spaces.

## Examples

The authors provide concrete implementations, including constant-product formulas (similar to Uniswap V2) and logarithmic utility functions, demonstrating how abstract theory translates to operational systems.

## Significance

This work bridges prediction markets and decentralized finance by rigorously establishing conditions under which crowd-sourced liquidity pools can efficiently price uncertain outcomes while preventing insolvency and arbitrage exploitation.
