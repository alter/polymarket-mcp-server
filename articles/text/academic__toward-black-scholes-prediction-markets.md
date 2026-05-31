---
title: "Toward Black-Scholes for Prediction Markets: A Unified Kernel and Market Maker's Handbook"
url: "https://arxiv.org/abs/2510.15205"
source: "arxiv"
date: "2025-10-17"
type: "paper"
theme: "academic"
lang: "en"
authors: ["Shaw Dalen"]
---

# Toward Black-Scholes for Prediction Markets: A Unified Kernel and Market Maker's Handbook

## Abstract

Prediction markets, such as Polymarket, aggregate dispersed information into tradable probabilities, but still lack a unifying stochastic kernel comparable to what options gained from Black-Scholes. As these markets scale with institutional participation and higher volumes around elections and macro prints, market makers face belief volatility, jump, and cross-event risks without standardized tools.

## Key Findings

- The paper proposes a logit jump-diffusion with risk-neutral drift that treats the traded probability as a Q-martingale and exposes belief volatility, jump intensity, and dependence as quotable risk factors
- A calibration pipeline separates diffusion from jumps using expectation-maximization while filtering microstructure noise and enforcing risk-neutral drift
- Testing on synthetic and real event data demonstrates reduced short-horizon belief-variance forecast error compared to diffusion-only and probability-space baselines
- The model provides an "implied-volatility analogue" for prediction markets, offering a practical framework for quoting, hedging, and transferring belief risk across venues

Version 1 submitted October 17, 2025; Version 2 revised April 6, 2026.
