---
title: "Prediction Markets as Bayesian Inverse Problems: Uncertainty Quantification, Identifiability, and Information Gain"
url: https://arxiv.org/abs/2601.18815
source: arxiv
date: "2026-01-01"
type: paper
theme: forecast
lang: en
---

# Prediction Markets as Bayesian Inverse Problems: Uncertainty Quantification, Identifiability, and Information Gain from Price–Volume Histories under Latent Types

**Authors:** Juan P. Madrigal-Cianci, Camilo Monsalve Maya, Lachlan Breakey
**arXiv:** 2601.18815v1

## Abstract

Frames prediction markets as statistical inverse problems, treating the unknown binary outcome as a quantity to be inferred from observed price-volume data. Proposes a mechanism-agnostic model where price changes in log-odds space arise from a latent mixture of trader types. Enables rigorous uncertainty quantification, identifiability analysis, and posterior concentration bounds.

## Introduction

The paper addresses the fundamental question: "what can be reliably inferred about the outcome from the observed market history, and with what quantified uncertainty?"

**Three primary contributions:**
1. Uncertainty quantification with exponentially decaying error bounds
2. Identifiability criteria — when inference is well-posed vs. ill-posed
3. Information-theoretic metrics measuring how effectively market histories function as outcome measurements

## Methodology

**Core modeling framework:**
- Binary outcome Y ∈ {0,1} observed indirectly through price-volume pairs (pₜ, vₜ)
- Log-odds representation Xₜ = logit(Pₜ) enables additive evidence accumulation
- Latent trader types K with volume-dependent gating functions ρₖ(v)
- Location-scale increment laws: ΔX = m_{k,y}(v) + s_k(v)ε

**Key theoretical results:**
- Theorem 4.1: Sufficient informed participation guarantees positive outcome separation
- Theorem 4.2: Bayes factors grow linearly with horizon when KL projection gap δₜ(y*, θ*) > 0
- Theorem 4.3: Posterior consistency under identifiability conditions

## Conclusion

The inverse-problem perspective provides diagnostics distinguishing regimes where market inference is statistically sound from those where latent structure renders conclusions unreliable. The framework accommodates heterogeneous trading behavior, heavy-tailed microstructure noise, and adversarial manipulation.

## Relevance to Prediction-Market Pricing

Provides a rigorous probabilistic framework for interpreting Polymarket price trajectories as Bayesian evidence. The log-odds formulation connects directly to practical calibration and price-updating models.
