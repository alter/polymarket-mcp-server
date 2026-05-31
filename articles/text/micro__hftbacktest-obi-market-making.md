---
title: "Market Making with Alpha - Order Book Imbalance (hftbacktest Tutorial)"
url: https://hftbacktest.readthedocs.io/en/latest/tutorials/Market%20Making%20with%20Alpha%20-%20Order%20Book%20Imbalance.html
source: blog
date: "2023-05-01"
type: tutorial
theme: micro
lang: en
---

# Market Making with Alpha - Order Book Imbalance

**Source:** hftbacktest documentation

**GitHub:** https://github.com/nkaz001/hftbacktest

**Notebook:** https://github.com/nkaz001/hftbacktest/blob/master/examples/Market%20Making%20with%20Alpha%20-%20Order%20Book%20Imbalance.ipynb

## Overview

Practical tutorial implementing order book imbalance-adjusted market making on Binance Futures BTC/USDT. Demonstrates how OBI signal improves market-making P&L over naive constant-spread approach.

## Strategy Architecture

**Three indicator options compared:**

1. **Standardized Order Book Imbalance:**
   `alpha = standardize(∑ᵢᴺ Qbid_i - ∑ᵢᴺ Qask_i)`

2. **VAMP (Volume-Weighted Average Market Price):**
   Cross-multiplication of bid/ask quantities and prices within 1% of mid

3. **Weighted-Depth Order Book Price:**
   Fixed total quantity instead of fixed price range

**Fair price adjustment:**
`fair_price = mid_price + c1 × alpha`

**Reservation price:**
`reservation_price = fair_price - skew × normalized_position`

**Grid-based quote placement** around reservation price with dynamic adjustment by imbalance.

## Backtest Results

**May 2023 (BTC/USDT):**
- Sharpe Ratio: 10.83
- Return: 34.24%
- Daily trades: 4,119.88
- Return per trade: 0.0139% (includes 0.005% rebate)

**February 2025 (BTC/USDT):**
- Sharpe Ratio: 5.37
- Return: 45.96%
- Daily trades: 4,533.74
- Return per trade: 0.0086% (rebate structure changed)

## Relevance to Polymarket CLOB Trading

- **Directly implementable on Polymarket:** Replace BTC/USDT with YES/NO contract; adapt OBI calculation to binary book
- **Rebate consideration:** Polymarket may have maker rebates — the 0.005% rebate on Binance drives much of the profitability
- **OBI for binary markets:** YES-buy quantity vs NO-buy quantity (or equivalently, YES-bid vs YES-ask volume)
- **Position skew:** When long YES inventory, lower YES-ask to reduce inventory; when short, raise YES-bid
- **hftbacktest framework:** Open-source Python/Numba — adaptable for Polymarket CLOB full tick data backtesting

## Framework Features

- Full order book reconstruction from L2/L3 data
- Accounts for feed and order latency
- Queue position tracking for fill simulation
- Multi-exchange support (Binance Futures, Bybit)
- Open-source: Apache license
