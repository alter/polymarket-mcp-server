---
title: "Flow Toxicity (Maverick Quant Substack)"
url: https://maverickquant.substack.com/p/flow-toxicity
source: blog
date: "2024-03-19"
type: blog
theme: micro
lang: en
---

# Flow Toxicity

**Author:** Jerome Busca

**Published:** March 19, 2024

**Source:** Maverick Quant (Substack)

## Overview

Extension of the Avellaneda-Stoikov market making framework to incorporate informed (toxic) order flow. Examines what happens when liquidity takers have predictive forecasts (alpha) and trade based on them.

## Key Concepts

**Information Coefficient (IC):** Quality of counterparty forecast, defined as the correlation between the forecast and the asset return it's supposed to predict.

**Flow toxicity hazard:** As IC of counterparties increases, market-maker Sharpe ratio declines significantly — illustrated through numerical simulation.

**Resonant frequency:** There is a specific forecast horizon at which the market maker is most vulnerable. Flow toxicity depends on:
- Forecast accuracy (IC)
- Trading frequency relative to market maker's quote horizon
- The "resonant" frequency creates disproportionate adverse selection

**Practical mitigation strategies:**
1. Segregate flow by frequency; adjust risk parameters for each frequency bucket
2. Mix flow types to dilute exposure to problematic frequencies
3. Widen spreads specifically at the vulnerable horizon

## Python Implementation

The post includes Python simulation code implementing A-S framework modified to incorporate informed trading behavior. Informed traders' aggregate position increases as a function of forecast strength.

## Relevance to Polymarket CLOB Trading

- **Informed flow detection on Polymarket:** When large, directional orders appear — potentially informed. Widen spreads.
- **Resonant frequency:** On Polymarket, informed traders may operate at specific timescales (pre-resolution, post-news) — identify the vulnerable horizons
- **Flow mixing:** If Polymarket maker strategy also takes directional positions, mixing can dilute toxicity exposure
- **IC estimation:** Measure counterparty IC by comparing their order direction to subsequent price moves — flag high-IC wallets

## Companion Post

See: "Liquidity Provision: Market-Making and the Avellaneda-Stoikov Model" (January 9, 2024) for the underlying A-S framework derivation.
