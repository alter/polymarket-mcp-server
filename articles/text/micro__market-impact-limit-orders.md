---
title: "Market Impact: A Systematic Study of Limit Orders"
url: https://arxiv.org/abs/1802.08502
source: arxiv
date: "2018-02-23"
type: paper
theme: micro
lang: en
---

# Market Impact: A Systematic Study of Limit Orders

**Authors:** Emilio Said, Ahmed Bel Hadj Ayed, Alexandre Husson, Frédéric Abergel

**Submitted:** February 23, 2018; Last revised May 14, 2022

**arXiv:** 1802.08502

## Abstract

Examines how limit orders affect market prices using proprietary metaorder data. Provides empirical evidence of a power law behaviour for the temporary market impact of both aggressive and passive limit orders.

## Key Findings

1. **Power law for temporary impact:** I_temp ~ Q^α, where α is empirically estimated
2. **Long-term impact stabilizes at ~67% of peak impact** (two-thirds rule)
3. **Fair pricing conditions during metaorder lifecycle** empirically supported
4. Both **aggressive** (crosses spread) and **passive** (rests in book) limit orders studied
5. Impact lifecycle documented: buildup, peak, partial decay to permanent level

## Market Impact Lifecycle

1. **During metaorder:** Temporary impact rises as orders execute
2. **Peak impact:** At completion of metaorder
3. **Post-metaorder decay:** Temporary component reverts; permanent component remains
4. **Long-run level:** Approximately ⅔ of peak impact is permanent

This ⅔ rule is now a stylized empirical fact across many markets.

## Relevance to Polymarket CLOB Trading

- **Position sizing on Polymarket:** For large position builds, expected peak slippage is power-law in size — scale position to stay within acceptable impact
- **⅔ rule:** After building large YES position, expect ~⅓ of price impact to revert — useful for entry timing and P&L forecasting
- **Passive vs. aggressive:** On Polymarket, passive limit orders have lower immediate impact but adverse selection risk (see Albers et al. 2025)
- **Metaorder management:** Break large Polymarket positions into sub-orders with waiting periods to allow liquidity migration (Donier et al. latent book model)

**Classification:** Quantitative Finance (Trading and Market Microstructure)
