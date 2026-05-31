---
title: "Endogenous Formation of Limit Order Books: Dynamics Between Trades"
url: https://arxiv.org/abs/1605.09720
source: arxiv
date: "2016-05-31"
type: paper
theme: micro
lang: en
---

# Endogenous Formation of Limit Order Books: Dynamics Between Trades

**Authors:** Roman Gayduk, Sergey Nadtochiy

**Submitted:** May 31, 2016; Revised June 19, 2017

**arXiv:** 1605.09720

## Abstract

A continuous-time large-population game for modeling market microstructure between two consecutive trades. The LOB shape and dynamics arise endogenously from equilibrium among agents with differing beliefs about future asset demand — not assumed exogenously.

## Key Findings

1. **LOB shape arises endogenously** — emerges from agent equilibrium, not assumed
2. **Continuum-player control-stopping game** — each agent decides whether and where to post limit orders
3. **Mathematical contributions:** Novel existence results for Reflected Backward Stochastic Differential Equations (RBSDEs) and infinite-dimensional fixed-point problems
4. **Indirect market impact** modeled: information signals affect the LOB shape itself (not just prices)
5. Continuous-time extension of existing microstructure frameworks

## Model Structure

Agents have different beliefs about future demand → post limit orders at prices reflecting their beliefs → LOB shape = aggregated beliefs distribution

**Between-trade dynamics:**
- Orders arrive, are canceled, updated based on incoming information
- Equilibrium LOB = fixed point of agents' best responses

## Relevance to Polymarket CLOB Trading

- **LOB shape prediction:** In Polymarket, the LOB shape (depth at each price level) reflects traders' collective probability beliefs — this model predicts how the shape should look under different information structures
- **Indirect impact:** When news arrives on Polymarket, the LOB shape changes before prices — monitoring LOB shape changes gives early signal
- **Agent heterogeneity:** Different Polymarket traders have different probability assessments — the LOB aggregates these efficiently (or not, depending on informed trader fraction)
- **Between-trade dynamics:** During quiet periods, Polymarket LOB evolves via cancellations and new postings — this model describes that evolution

**Classification:** Quantitative Finance (Trading and Market Microstructure)
