---
title: "A Practical Liquidity-Sensitive Automated Market Maker"
url: "https://www.cs.cmu.edu/~sandholm/liquidity-sensitive%20automated%20market%20maker.teac.pdf"
source: "cmu.edu"
date: "2013-01-01"
type: "paper"
theme: "amm"
lang: "en"
---

# A Practical Liquidity-Sensitive Automated Market Maker

**Authors:** Abraham Othman, David M. Pennock, Daniel M. Reeves, Tuomas Sandholm

**Published:** ACM Transactions on Economics and Computation (TEAC), 1(3), Article 14 (2013). Conference version at EC 2010.

**ACM link:** https://dl.acm.org/doi/10.1145/2509413.2509414

## Overview

Automated market makers from the literature suffer from two problems:
1. They are unable to adapt to liquidity — trades cause prices to move the same amount in both heavily and lightly traded markets
2. In typical circumstances, they run at a deficit

This work constructs the LS-LMSR (Liquidity-Sensitive LMSR) market maker that is both sensitive to liquidity and can run at a profit.

## Key Properties

- Bounded loss for any initial level of liquidity
- As initial liquidity approaches zero, worst-case loss approaches zero
- A boundary can be established in market state space such that if the market terminates within that boundary, the market maker books a profit regardless of realized outcome
- Liquidity parameter b is a function of quantities traders have wagered (vs. constant in LMSR)

## Technical Details

In LS-LMSR, the liquidity parameter b scales with the amount of money already wagered. Thin markets move meaningfully on small flow; thick markets become harder to shift as size accumulates. This fixes two key weaknesses of vanilla LMSR:
1. Liquidity inflexibility
2. Chronic market-maker losses

At the cost of: more complex math and more compute per trade.

## Practical Lessons

From the Gates Hillman Prediction Market (CMU): LMSR with b=32 led to price spikes because the market was too shallow. Many profitable traders solely exploited these price spikes induced by less sophisticated buyers. LS-LMSR would have adapted b dynamically to prevent this.

## Augur's Use

Augur v1 used LS-LMSR but moved away due to Ethereum gas costs — the logarithm and exponentiation required are expensive on-chain.
