---
title: "Found The Weather Trading Bots Quietly Making $24,000 On Polymarket And Built One Myself For Free"
url: https://blog.devgenius.io/found-the-weather-trading-bots-quietly-making-24-000-on-polymarket-and-built-one-myself-for-free-120bd34d6f09
source: medium.com/devgenius
date: "2026-02"
type: blog
theme: strategies
lang: en
---

# Weather Trading Bots on Polymarket: NOAA Data Strategy

## Core Concept

A Polymarket weather bot monitors real-world weather data feeds, compares them against open Polymarket weather event contracts, identifies pricing discrepancies, and places trades programmatically when an edge exists.

## Why Weather Markets Have Structural Edge

- Weather outcomes determined by **objective, publicly available data** from NOAA and National Weather Service
- Professional weather models update on fixed six-hour schedules
- Market prices do not always reflect those updates in real time
- **Gap between what the science says and what the market prices imply = edge**

## How the Strategy Works

1. NOAA says temperature will be 43 degrees
2. Polymarket prices "40–45 degree" range at only 15 cents (market implies 15% probability)
3. NOAA's 1–2 day forecasts are accurate 85–90% of the time
4. Bot buys "40–45 degree" shares at $0.15
5. Temperature resolves at 43°F → shares pay $1.00
6. **85-cent profit per share**

Execute this across multiple cities, multiple times per day, 24/7.

## Advanced Implementation: 4-Model Ensemble (WeatherBot.fi)

Ensemble weighting:
- ECMWF: 35%
- GFS: 25%
- UKMO: 20%
- NWS: 20%

Features:
- Automatic outlier penalization
- Dynamic sigma calculation: adjusts forecast uncertainty based on time-to-resolution (0.8° at 6h → 5.5° at 10+ days)
- Bayesian blending with NOAA NCEI historical base rates
- If forecast diverges significantly from 10-year historical average → widen uncertainty band

## Technical Details (PolyBot Implementation)

Three complementary strategies:
1. **Weather Arbitrage (Gaussian Model):** NOAA data → probability model → compare to market price
2. **Crypto Price Prediction (Black-Scholes)**
3. **Binary Arbitrage**

**Critical detail:** Every Polymarket weather market resolves on a specific airport station:
- NYC resolves on LaGuardia (KLGA)
- Dallas resolves on Love Field (KDAL) — NOT DFW

Getting this wrong is a common bot failure mode.

## Risk Controls

- Only trade when weather model has sufficient historical accuracy for the market type
- Track model's Brier score before going live
- Build kill switch halting all order placement if daily loss exceeds threshold
- Minimum edge threshold: 5% (0.05 common starting point)

## Case Study Results

- Focused London weather pool: $1,000 → $24,000
- Multi-city parallel approach: $65,000 in silent profits

## APIs Used

- Gamma API: market discovery
- CLOB API: order execution
- NOAA / Open-Meteo: weather probability models

## No-Code Option

Pull NOAA forecast data → compare probabilities to Polymarket bucket prices → buy Yes/No shares when forecast shows strong edge (70%+ vs 40% market implied) → sell when odds converge or hit profit targets. Deployable via OpenClaw + Simmer SDK without coding.
