---
title: "Toward Black Scholes for Prediction Markets: A Unified Kernel and Market Maker's Handbook"
url: "https://arxiv.org/abs/2510.15205"
source: "arxiv"
date: "2025-10-17"
type: "paper"
theme: "amm"
lang: "en"
---

# Toward Black Scholes for Prediction Markets: A Unified Kernel and Market Maker's Handbook

**Author:** Shaw Dalen

**arXiv ID:** 2510.15205 | Revised April 6, 2026

## Abstract

Prediction markets, such as Polymarket, aggregate dispersed information into tradable probabilities, but they still lack a unifying stochastic kernel comparable to the one options gained from Black-Scholes. As these markets scale with institutional participation, exchange integrations, and higher volumes around elections and macro prints, market makers face belief volatility, jump, and cross-event risks without standardized tools for quoting or hedging. We propose such a foundation: a logit jump-diffusion with risk-neutral drift that treats the traded probability p_t as a Q-martingale and exposes belief volatility, jump intensity, and dependence as quotable risk factors. On top, we build a calibration pipeline that filters microstructure noise, separates diffusion from jumps using expectation-maximization, enforces the risk-neutral drift, and yields a stable belief-volatility surface.

## Key Contributions

- Develops a logit jump-diffusion framework treating prediction probabilities as martingales — analogous to Black-Scholes for options
- Creates calibration pipeline separating microstructure noise and diffusion/jump components via EM
- Defines derivative instruments (variance, correlation, corridor, first-passage) for belief risk trading
- Demonstrates empirical improvements in forecast accuracy versus baseline models
- Provides an implied-volatility analogue for prediction markets: tractable language for quoting, hedging, and transferring belief risk

## Significance

The first attempt to build a Black-Scholes-equivalent framework for prediction markets, enabling market makers to standardize quoting and hedging of belief risk across venues.
