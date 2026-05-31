---
title: "Programmatically Identifying Positive Expected Value (+EV) Bets"
url: https://medium.com/@zacharyrgarrett/programmatically-identifying-positive-expected-value-ev-bets-f7cfa92cd8d3
source: medium
date: "2022-06-28"
type: blog
theme: sports
lang: en
---

# Programmatically Identifying Positive Expected Value (+EV) Bets

**Author:** Zachary Garrett, Medium

## System Overview

Python-based system to automatically detect and alert on favorable sports betting opportunities across 10 sportsbooks by scraping pricing discrepancies.

## Methodology

### Data Source
OddsJam (Selenium scraping) — already identified potential +EV opportunities across sportsbooks.

### The Filter Problem

Initial results showed losses despite sound math. Problem: most +EV bets were low-probability player props with high variance.

**Solution:** Logistic curve filter using bet odds as independent variable:
- Balances +EV% against realistic win probability
- Rejects low-probability bets with minimal +EV%
- Reduces impact of sportsbook juice

### Technical Implementation

- Refreshed page every **2 seconds** for real-time opportunities
- Cleared Chromium cache every 25 refreshes (memory leak prevention)
- Restarted driver every 25 cycles (intermittent HTML retrieval errors)
- Discord webhooks for real-time alerts → manual placement within the brief window

## Empirical Results

**Duration:** April 16 – June 28, 2022 (73 days)

| Metric | Value |
|---|---|
| Total bets | 1,136 |
| Win/Loss/Push | 468/628/40 (42.70% win rate) |
| Total wagered | $12,725.88 |
| Actual profit | **$217.90** |
| Expected profit (theoretical) | $1,111.57 (9.30% avg +EV%) |

## Why Results < Theory

The $217.90 actual vs $1,111.57 expected gap is primarily **variance** (73 days insufficient sample). Missing variables that would improve results:
- Bet sizing proportional to +EV% or odds
- Excluding player props for consistency
- Identifying lower-variance sports
- Targeting specific sharp sportsbooks per sport

## Key Practical Lessons

1. **+EV alone isn't enough** — need volume, proper sizing, and low-variance selection
2. **Manual placement at 40+ bets/day** is impractical → automation required
3. **Logistic curve filtering** outperformed naive all-EV-bets approach
4. **Discord alerting** system worked but execution lag remains a problem

## Transferable Insights

The architecture (real-time scraping → EV calculation → filter → alert) maps directly to Polymarket monitoring:
1. Monitor Polymarket/Kalshi prices in real-time
2. Calculate EV vs. model probability (or Pinnacle-equivalent benchmark)
3. Apply logistic curve filter to avoid extreme longshots with tiny edge
4. Alert system → manual or automated execution

The 73-day / 1,136-bet experiment confirms: even a correct +EV system needs patience and volume to demonstrate edge over variance.
