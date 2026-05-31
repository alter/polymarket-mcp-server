---
title: "Automated Market Making and Loss-Versus-Rebalancing"
url: "https://arxiv.org/abs/2208.06046"
source: "arxiv"
date: "2022-08-11"
type: "paper"
theme: "amm"
lang: "en"
---

# Automated Market Making and Loss-Versus-Rebalancing

**Authors:** Jason Milionis, Ciamac C. Moallemi, Tim Roughgarden, Anthony Lee Zhang

**arXiv ID:** 2208.06046 | Last revised May 27, 2024

## Abstract

We consider the market microstructure of automated market makers (AMMs) from the perspective of liquidity providers (LPs). Our central contribution is a "Black-Scholes formula for AMMs." We identify the main adverse selection cost incurred by LPs, which we call "loss-versus-rebalancing" (LVR, pronounced "lever"). LVR captures costs incurred by AMM LPs due to stale prices that are picked off by better informed arbitrageurs. We derive closed-form expressions for LVR applicable to all automated market makers. Our model is quantitatively realistic, matching actual LP returns empirically, and shows how CFMM protocols can be redesigned to reduce or eliminate LVR.

## Key Contributions

- Establishes an analytical framework analogous to Black-Scholes pricing for AMMs
- Identifies and formalizes loss-versus-rebalancing (LVR) as the primary adverse selection cost for LPs
- Provides closed-form mathematical expressions for LVR across all AMM types
- Demonstrates empirical accuracy against real LP returns
- Proposes design improvements for CFMM protocols to reduce or eliminate LVR

## Technical Details

LVR is the performance gap between the AMM LP position and a dynamic rebalancing strategy trading the underlying asset at market prices. The source: passive liquidity provision means AMMs trade at worse-than-market prices whenever risky asset prices move. The pm-AMM paper by Moallemi and Robinson builds directly on this LVR framework to design prediction-market-specific AMMs.
