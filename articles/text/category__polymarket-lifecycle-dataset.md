---
title: "Unlocking the Forecasting Economy: A Suite of Datasets for the Full Lifecycle of Prediction Market"
url: "https://arxiv.org/html/2604.20421v1"
source: "arxiv"
date: "2026-04"
type: "academic_paper"
theme: "category"
lang: "en"
---

# Polymarket Lifecycle Dataset: Comprehensive Summary

arXiv:2604.20421 (April 2026)

## Overview

Researchers have created the first continuously maintained dataset capturing the complete lifecycle of decentralized prediction markets on Polymarket, spanning October 2020 to March 2026. The system integrates three data layers: market metadata, fill-level trading records, and oracle-resolution events.

## Dataset Scale

- **770,880 market records**
- **943.5 million OrderFilled (trade) records**
- **1.99 million oracle-resolution events**
- **2.49 million distinct trader addresses**
- **3.06 million materialized market-day observations**

## Market Category Distribution

Analysis reveals distinct topical concentration:
- **Sports markets dominate**: 357,359 markets with consistent high trading volume
- **Crypto category**: 243,242 markets, heavily weighted toward price-range contracts
- **Politics**: 46,883 markets
- **Games**: 22,152 markets
- **Science**: 20,330 markets

## Trading Activity Patterns

The platform experienced two growth phases. The first surge coincided with the 2024 U.S. presidential election. A stronger second expansion phase began in late 2025, with simultaneous increases in market variety, active participants, and transaction volume.

## Oracle and Settlement Findings

Among settled markets, only 2,358 experienced disputes (less than 1% of total). Notably, traders frequently continued active trading even after oracle-risk events, with 66% of resumed trading occurring within three hours of dispute initiation.

## Fee Structure Discovery

Fee implementation proved non-uniform across categories. The Pearson correlation between trading volume and effective fee rates was weak (r=0.168), suggesting fee determination relies on factors beyond volume. **Crypto and Sports categories showed substantially higher effective fees than other categories.**

## Downstream Applications

Two case studies demonstrated practical utility:

1. **NBA Outcome Calibration**: Market-implied pre-game probabilities demonstrated strong calibration with actual win frequencies, requiring minimal additional adjustment for trading decisions.

2. **CPI Expectation Reconstruction**: Polymarket-derived inflation expectations, reconstructed from discrete outcome-bucket prices, frequently aligned more closely with realized Consumer Price Index values than external nowcast benchmarks.

## Data Quality

Linkage completeness exceeded 99% across core metrics, with 78.18% of markets experiencing actual trading activity and 97.50% linked to oracle resolution processes.
