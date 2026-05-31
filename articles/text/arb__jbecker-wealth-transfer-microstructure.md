---
title: "The Microstructure of Wealth Transfer in Prediction Markets"
url: "https://www.jbecker.dev/research/prediction-market-microstructure"
source: "jbecker.dev"
date: "2026-01-01"
type: "research_blog"
theme: "arb"
lang: "en"
---

# The Microstructure of Wealth Transfer in Prediction Markets

**Author:** Jonathan Becker

**URL:** https://www.jbecker.dev/research/prediction-market-microstructure

---

## Overview

Analysis of 72.1 million trades totaling $18.26 billion in volume on Kalshi, a CFTC-regulated prediction market. Reveals systematic wealth transfer mechanism where unsophisticated traders (takers) consistently lose money to professional liquidity providers (makers).

---

## Key Findings

### The Longshot Bias
- Contracts priced at 5¢ win only **4.18%** of the time (vs. implied 5%)
- Pattern persists across price levels — confirms classic longshot bias from horse racing/parimutuel literature
- YES longshot contracts significantly underperform NO longshot contracts at equivalent prices

### Maker-Taker Divergence (Core Finding)
- **Takers:** average -1.12% excess returns
- **Makers:** average +1.12% excess returns
- Takers show negative returns at **80 of 99 price levels**
- The gap emerged after platform growth — not present at inception

### Category Variation (Efficiency by Domain)
| Category | Maker-Taker Gap |
|---|---|
| Finance | 0.17 pp (nearly efficient) |
| Politics | 1.02 pp |
| Sports | 2.23 pp |
| Entertainment/Media | 4.79-7.32 pp |

### The "Optimism Tax" Mechanism
- Takers disproportionately purchase YES contracts at longshot prices
- NO longshots outperform YES by up to **64 percentage points** at equivalent prices
- Makers need not forecast accurately — they simply provide liquidity to systematically biased flow
- Makers function as underwriters, not forecasters

### Temporal Evolution (Critical)
- **Before October 2024:** Takers earned +2.0%, makers lost -2.0% (takers had edge)
- **After October 2024 (Kalshi legal victory):** Roles reversed completely
- Inflection point: professional market makers entered after volume surge, not changes in taker behavior
- 72.1M trades analyzed: wealth transfer is post-2024 phenomenon

---

## Methodology Notes
- Kalshi API directly records which side is Maker/Taker for every trade
- Eliminates need for Lee-Ready (1991) algorithm — rare in microstructure literature
- Normalized trades by cost basis
- Excluded voided markets and low-volume contracts
- Decomposed returns by liquidity provider role

---

## Implications for Market Making
- Market making on prediction platforms became profitable precisely when institutional MMs entered
- The "Optimism Tax" is structural and persistent — taker bias toward YES longshots appears stable
- Category selection matters enormously: Finance almost efficient, Entertainment offers 7x larger edges
- Sports markets (2.23 pp gap) offer substantial maker alpha vs. near-zero in Finance
