---
title: "From PIN to VPIN: An Introduction to Order Flow Toxicity (Easley et al.)"
url: https://www.quantresearch.org/From%20PIN%20to%20VPIN.pdf
source: journal
date: "2012-01-01"
type: paper
theme: micro
lang: en
---

# From PIN to VPIN: An Introduction to Order Flow Toxicity

**Authors:** David Easley, Marcos Lopez de Prado, Maureen O'Hara

**Published:** The Spanish Review of Financial Economics, 2012

**Key Papers:**
- "The Microstructure of the Flash Crash" (2011)
- "Flow Toxicity and Liquidity in a High Frequency World" (2012)

**Practical reference:** https://www.quantresearch.org/VPIN.pdf

## Overview

VPIN (Volume-Synchronized Probability of Informed Trading) is a real-time measure of order flow toxicity — the probability that incoming order flow is driven by informed traders who will adversely select market makers.

## Core Definition

**VPIN = |V_b - V_s| / V**

where V_b = buy volume, V_s = sell volume, V = total volume in a bucket

Volume buckets are equal-volume (not equal-time) — this is the "Volume-Synchronized" aspect that makes VPIN more robust than time-based measures.

**Interpretation:**
- VPIN near 0.5 = balanced, uninformed flow → safe for market makers
- VPIN near 1.0 = one-sided, potentially informed flow → toxic for market makers

## Key Findings

1. **VPIN predicted the 2010 Flash Crash** over an hour before it occurred — CDF of VPIN was at historically high levels
2. **Toxicity cascade mechanism:** High VPIN → market makers withdraw → liquidity drops → more concentrated toxic flow → more market maker withdrawal → crash
3. **VPIN tracks adverse selection in real time** without requiring trade classification (Lee-Ready or other)
4. **Applications:** market maker risk management, regulator circuit breakers, algorithmic trading signal

## VPIN Adaptation for Prediction Markets

The Bartlett (2026) Kalshi study adapts VPIN to binary prediction markets:
- One-sided order flow (all YES buys or all NO buys) predicts maker losses in single-name markets
- VPIN-style metric works even with binary settlement mechanics
- Key difference: volume must be measured in USDC (collateral) not outcome shares to avoid minting/burning confounds

## Relevance to Polymarket CLOB Trading

- **Real-time toxicity gauge:** High imbalance of YES buys signals informed flow — widen spreads or pull liquidity
- **Toxicity cascade on Polymarket:** Can occur near major news events (election results, sports outcomes)
- **Adapted VPIN:** Use equal-volume buckets based on USDC notional traded, classify by YES-buy vs NO-buy
- **Circuit breaker logic:** If VPIN > threshold, pause maker activity — protect from flash adverse selection
- **Strategy signal:** High VPIN (one-sided) → fade the direction (contrarian) or exit inventory quickly
