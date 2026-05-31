---
title: "Prediction Markets as Bayesian Inverse Problems: Uncertainty Quantification, Identifiability, and Information Gain from Price-Volume Histories under Latent Types"
url: "https://arxiv.org/abs/2601.18815"
source: "arxiv"
date: "2026-01-01"
type: "paper"
theme: "academic"
lang: "en"
authors: ["Juan Pablo Madrigal-Cianci", "Camilo Monsalve Maya", "Lachlan Breakey"]
---

# Prediction Markets as Bayesian Inverse Problems

## Abstract

The paper formulates prediction markets as Bayesian inverse problems in which the unknown event outcome is inferred from an observed history of market-implied probabilities and traded volumes. The authors introduce a mechanism-agnostic observation model in log-odds space in which price increments conditional on volume arise from a latent mixture of trader types.

## Key Findings

The framework provides:
- Posterior uncertainty quantification for outcomes
- Identifiability criteria based on KL divergence gaps between outcome-conditional distributions
- Posterior concentration rates and finite-sample error bounds under general regularity assumptions
- Stability analysis of posterior beliefs under price-volume perturbations

The resulting likelihood class encompasses informed and uninformed trading, heavy-tailed microstructure noise, and adversarial or manipulative flow, while requiring only price and volume as observables.

The inverse-problem formulation yields explicit diagnostics for regimes in which market histories are informative and stable versus regimes in which inference is ill-posed. Prediction markets are argued to function as "stochastic sensing systems" rather than perfect information aggregators.

Synthetic experiments confirmed theoretical predictions about concentration rates and identifiability thresholds.
