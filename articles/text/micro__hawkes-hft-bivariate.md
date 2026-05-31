---
title: "Hawkes Processes in High-Frequency Trading (Bivariate Power-Law vs Exponential Kernels)"
url: https://arxiv.org/abs/2503.14814
source: arxiv
date: "2025-03-19"
type: paper
theme: micro
lang: en
---

# Modelling High-Frequency Data with Bivariate Hawkes Processes: Power-Law vs. Exponential Kernels

**Author:** Neal Batra

**Submitted:** March 19, 2025

**arXiv:** 2503.14814

## Abstract (verbatim)

"This study explores the application of Hawkes processes to model high-frequency data in the context of limit order books. Two distinct Hawkes-based models are proposed and analyzed: one utilizing exponential kernels and the other employing power-law kernels. These models are implemented within a bivariate framework. The performance of each model is evaluated using high-frequency trading data, with a focus on their ability to reproduce key statistical properties of limit order books. Through a comprehensive comparison, we identify the strengths and limitations of each kernel type, providing insights into their suitability for modeling high-frequency financial data. Simulations are conducted to validate the models, and the results are interpreted. Based on these insights, a trading strategy is formulated."

## Key Findings

1. **Two Hawkes kernel types compared:** exponential (faster decay) vs. power-law (long memory)
2. **Power-law kernel captures long-memory in order flow** — clusters persist over longer horizons
3. **Exponential kernel more tractable** for real-time calibration
4. **Bivariate framework** models buy and sell order arrivals jointly with cross-excitation
5. Trading strategy formulated from superior kernel model

## Hawkes Process Basics

The self-exciting point process with intensity:
λ(t) = μ + ∫₀ᵗ φ(t-s) dN(s)

- **Exponential kernel:** φ(t) = α exp(-βt) — fast decay, Markovian
- **Power-law kernel:** φ(t) = α (t+β)^(-γ) — slow decay, long memory

Bivariate version captures buy-sell cross-excitation: market buys excite more limit sells, and vice versa.

## Relevance to Polymarket CLOB Trading

- Hawkes process calibratable on Polymarket order-book event stream
- Self-excitation in YES-buy orders: one large buy often triggers follow-on buys
- Power-law kernel may be appropriate for markets with long-duration sustained imbalances (political markets)
- Exponential kernel better for sports markets with discrete information arrival
- Trading strategy from Hawkes calibration: detect when buy intensity spike is decaying → fade the move

**Classification:** Quantitative Finance (Trading and Market Microstructure)
