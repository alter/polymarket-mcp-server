---
title: "Diffusive Limit of Hawkes Driven Order Book Dynamics With Liquidity Migration"
url: https://arxiv.org/abs/2511.18117
source: arxiv
date: "2025-11-22"
type: paper
theme: micro
lang: en
---

# Diffusive Limit of Hawkes Driven Order Book Dynamics With Liquidity Migration

**Author:** Levon Mahseredjian

**Submitted:** November 22, 2025

**arXiv:** 2511.18117

## Abstract (verbatim excerpt)

"Develops a theoretical mesoscopic model of the limit order book driven by multivariate Hawkes processes, designed to capture temporal self-excitation and the spatial propagation of order flow across price levels. Introduces migration events between neighbouring price levels, whose intensities are themselves governed by the underlying Hawkes structure and derives a diffusion approximation which yields a reflected mesoscopic stochastic differential equation (SDE) system for queue volumes."

## Key Findings

1. **Mathematical bridge** connecting high-frequency event models to macroscopic stochastic descriptions
2. **Diffusion approximation** via reflected SDE system for queue volumes across price levels
3. **Liquidity migration:** Order flow propagates spatially across price levels — not just temporal clustering
4. Extends existing diffusion limits by incorporating correlated excitations AND price-level-to-price-level movement
5. **Purely theoretical** — no empirical calibration, provides mathematical foundations for practical models

## Contribution

The limiting generator is obtained through a Taylor expansion of the microscopic generator, showing how:
- Temporal excitation → determines drift and diffusion of LOB
- Spatial migration → determines cross-level correlations

This yields a mesoscopic SDE system that bridges tick-level (Hawkes) and macroscopic (diffusion) descriptions.

## Relevance to Polymarket CLOB Trading

- **LOB macro model:** Provides stochastic differential equation description of Polymarket order book dynamics
- **Liquidity migration:** When large YES buy occurs, liquidity migrates from nearby price levels — this model predicts how
- **Calibration pipeline:** Calibrate Hawkes parameters at tick level, use diffusion limit for strategy simulation at second/minute level
- **Spread SDE:** Derive spread dynamics as SDE — enables options-style spread risk management

**Classification:** Mathematical Finance (q-fin.MF)
