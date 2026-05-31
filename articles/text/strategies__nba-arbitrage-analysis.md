---
title: "Arbitrage Analysis in Polymarket NBA Markets"
url: https://arxiv.org/abs/2605.00864
source: arxiv.org
date: "2026-04-30"
type: paper
theme: strategies
lang: en
---

# Arbitrage Analysis in Polymarket NBA Markets

## Abstract

"While decentralized prediction markets like Polymarket have gained significant traction, their market microstructure and high-frequency pricing efficiency remain underexplored. This paper conducts a systematic empirical analysis of algorithmic arbitrage within Polymarket's NBA game markets. By reconstructing continuous market states from over 75 million limit order book snapshots across 173 games, we evaluate the frequency, duration, and profitability of both single-market and combinatorial arbitrage opportunities."

## Dataset

- **75 million+ limit order book snapshots**
- **173 NBA games**
- Continuous market state reconstruction from CLOB data

## Key Findings

### Single-Market Opportunities
Pricing inefficiencies within individual markets are extremely uncommon. Only **7 executable in-game episodes** identified with **median persistence of just 3.6 seconds**, indicating rapid market correction.

### Combinatorial Inefficiencies (Cross-Market)
More prevalent than single-market: **290 active episodes** identified.
- Overwhelmingly concentrated in **final minutes of live play**
- The theoretical "Middle" jackpot scenario was **never realized empirically**

### Profitability & Constraints
- Combinatorial execution yielded **statistically meaningful median return of 101 basis points**
- **76.9% of combinatorial opportunities** faced execution restrictions
- Average tradeable size: **only 14.8 shares**
- Profitable arbitrage confined strictly to **retail-level positions**

## Unique Market Feature: Mirrored Liquidity

Polymarket's CLOB has a mirrored liquidity design — there is only **one shared pool of underlying liquidity** for any binary market. The matching engine automatically reflects orders across complementary tokens. For example, a limit order to buy 100 YES shares at $0.40 simultaneously generates a synthetic limit order to sell 100 NO shares at $0.60.

## Conclusion

The market exhibits **profound microstructural efficiency** for most of the game, with inefficiencies occurring only at specific high-intensity moments and closing within seconds when they do occur.
