---
title: "What Happens When Institutional Liquidity Enters Prediction Markets: Identification, Measurement, and a Synthetic Proof of Concept"
url: "https://arxiv.org/html/2604.10005"
source: "arxiv"
date: "2026-04"
type: "academic_paper"
theme: "category"
lang: "en"
---

# What Happens When Institutional Liquidity Enters Prediction Markets

arXiv:2604.10005 (April 2026)

## Abstract Summary

This paper examines what occurs when institutional liquidity enters prediction markets through designated market makers, liquidity incentives, and automated trading systems. Rather than treating institutionalization as a single phenomenon, the authors decompose it into distinct channels and ask a crucial welfare question: "when institutional liquidity enters prediction markets, who really gets the benefit?"

## Core Research Question

The central tension: markets can simultaneously display tighter spreads while becoming harsher for slower traders. Before a CPI announcement, market-maker support may compress displayed spreads, yet when information arrives, some participants react instantly while others arrive late—creating an environment where "better average execution" coexists with "more selective extraction in shock states."

## Treatment Decomposition

Rather than omnibus "institutional participation," the paper separates four channels:

- Designated market-maker coverage and quoting obligations
- Liquidity-incentive program eligibility and exchange subsidies
- Automation/API intensity enabling rapid quoting
- Participant-composition shifts (treated as equilibrium outcomes, not primitive treatments)

## Synthetic Laboratory Results

The proof-of-concept uses 320 markets over 57,600 observations. Key findings:

**Execution Quality Gains:** Quoted spreads compress by approximately 14%, effective spreads by 19%, depth increases 32%, and price impact declines 9%.

**Channel Heterogeneity:** Market-maker coverage delivers the largest spread improvements; liquidity incentives show moderate effects; automation intensity most strongly affects internal consistency.

**Forecast Quality:** Changes prove "mixed, not a headline result"—Brier scores worsen slightly while calibration error improves modestly.

**Shock-State Incidence:** Institutional liquidity benefits concentrate among faster and better-hedged traders. In synthetic shock windows, slow takers can actually experience worse execution under high institutional liquidity compared to baseline conditions.

## Measurement Blueprint

Five outcome families:

1. Execution metrics: quoted spread, effective spread, realized spread, adverse selection, price impact
2. Forecast quality: Brier score, expected calibration error
3. Internal consistency: complementary-book gaps, simplex constraints, cross-venue price dispersion
4. Mediator proxies: cancel-to-trade ratios, fast replenishment, synchronized quoting
5. Pass-through: conversion of quoted-spread gains into effective-spread gains by trader type

## Key Domain Insights

Event contracts are "unusually shock-driven: macro releases, debate performances, court rulings, injuries, and protocol announcements can all move beliefs discontinuously." For that reason, market quality in prediction markets cannot be assessed from one dimension alone — spread, depth, resiliency, price impact, forecast calibration, and cross-contract consistency all matter.

## Central Claims

1. Market-maker coverage, incentives, and automation improve average displayed liquidity with channel-specific magnitudes
2. Some institutional channels improve price discovery, though forecast-quality gains may underperform execution-quality gains
3. Better average execution can coexist with worse shock-state adverse selection for slower participants
4. Quality improvements vary systematically with information density, hedgeability, and clarity rather than event categories alone
