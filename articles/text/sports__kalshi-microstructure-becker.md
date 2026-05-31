---
title: "The Microstructure of Wealth Transfer in Prediction Markets"
url: https://www.jbecker.dev/research/prediction-market-microstructure
source: jbecker.dev
date: "2026-01-01"
type: paper
theme: sports
lang: en
---

# The Microstructure of Wealth Transfer in Prediction Markets

**Author:** Jonathan Becker  
**Published:** 2026, jbecker.dev/research

## Data

- **72.1 million trades** on Kalshi
- **$18.26 billion** total volume
- Sports accounts for **72% of notional volume** in dataset

## Key Finding: Maker-Taker Wealth Transfer

| Role | Average Excess Return | 95% CI |
|---|---|---|
| **Taker** | **-1.12%** | [-1.13%, -1.11%] |
| **Maker** | **+1.12%** | [+1.11%, +1.13%] |

Takers exhibit negative excess returns at 80 of 99 price levels. Makers show positive returns at identical levels.

## Longshot Bias Quantification

- Contracts at **5 cents** win only **4.18%** of the time (implied: 5%) — longshots lose MORE than implied
- Contracts at **95 cents** win **95.83%** — slight outperformance

## Category Performance Variation

| Category | Taker Return | Maker Return | Gap |
|---|---|---|---|
| Finance | -0.08% | +0.08% | 0.17 pp |
| Politics | -0.51% | +0.51% | 1.02 pp |
| **Sports** | **-1.11%** | **+1.12%** | **2.23 pp** |
| Entertainment | -2.40% | +2.40% | 4.79 pp |
| Media | -3.64% | +3.64% | 7.28 pp |

Finance approaches "perfect efficiency." Sports shows significant maker advantage. Entertainment/Media show severe mispricing.

## YES/NO Asymmetry

At price of 1 cent:
- YES contract: **-41% expected value** for takers
- NO contract: **+23% expected value** for takers
- Gap: 64 percentage points

Takers disproportionately favor YES outcomes at longshot prices (41–47% of 1–10 cent YES volume). Makers dominate NO purchases at extreme prices.

## Market Evolution

Early Kalshi (2021–2023): Takers GAINED (+2.0%), Makers LOST (-2.0%)  
After October 2024 legal victory + volume surge ($30M → $820M quarterly):  
Takers now LOSE, Makers now GAIN. Professional market makers moved in.

## Extraction Mechanism: "Optimism Tax"

Makers profit via structural arbitrage: providing liquidity to taker population that exhibits a costly preference for **affirmative, longshot outcomes** (buying YES on unlikely events).

Makers do NOT possess superior forecasting ability — Cohen's d statistics show negligible directional prediction differences (0.02–0.03). They systematize **countervailing to biased taker flow**.

## Participant Selection Effects

Efficiency ↔ barrier to entry:
- Finance: technical barriers filter casual bettors → calibrated participants dominate
- Sports: home bias, recency effects, narrative attachment → systematic taker losses
- Entertainment: familiarity breeds overconfidence → extreme mispricing

## Conclusion

"Prediction market accuracy relies less on rational actors than on a mechanism for harvesting error." When emotional and tribal elements dominate, markets transform into "a mechanism for transferring wealth from the optimistic to the calculated."

## Transferable Edges for Polymarket/Kalshi

1. Be a **maker** (limit orders) not a taker in sports markets — 2.23 pp structural advantage
2. Fade YES longshots in sports — they win 4.18% when priced at 5%
3. Sports contracts are the most inefficient Kalshi category — largest opportunity
4. Platform maturation accelerates maker advantage — early mover benefit in new sports markets
