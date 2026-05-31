---
title: "Prediction Markets as Bayesian Inverse Problems: Uncertainty Quantification, Identifiability, and Information Gain"
url: "https://arxiv.org/abs/2601.18815"
source: "arxiv"
date: "2026-01-22"
type: "paper"
theme: "amm"
lang: "en"
---

# Prediction Markets as Bayesian Inverse Problems: Uncertainty Quantification, Identifiability, and Information Gain from Price-Volume Histories under Latent Types

**Authors:** Juan Pablo Madrigal-Cianci, Camilo Monsalve Maya, Lachlan Breakey

**arXiv ID:** 2601.18815

## Abstract

Prediction markets are often described as mechanisms that "aggregate information" into prices, yet the mapping from dispersed private information to observed market histories is typically noisy, endogenous, and shaped by heterogeneous and strategic participation. This paper formulates prediction markets as Bayesian inverse problems in which the unknown event outcome Y in {0,1} is inferred from an observed history of market-implied probabilities and traded volumes. A mechanism-agnostic observation model in log-odds space is introduced in which price increments conditional on volume arise from a latent mixture of trader types. The resulting likelihood class encompasses informed and uninformed trading, heavy-tailed microstructure noise, and adversarial or manipulative flow, while requiring only price and volume as observables.

Within this framework: posterior uncertainty quantification for Y is defined; identifiability and well-posedness criteria in terms of Kullback-Leibler separation between outcome-conditional increment laws are provided; posterior concentration statements and finite-sample error bounds under general regularity assumptions are derived. Stability of posterior odds to perturbations of the observed price-volume path and realized/expected information gain via posterior-vs-prior KL divergence and mutual information are studied.

## Key Contributions

- Reframes prediction markets using Bayesian inverse problem methodology — novel theoretical framing
- Mechanism-agnostic models handling trader heterogeneity and noise
- Identifiability criteria and posterior concentration bounds
- Diagnostic tools distinguishing well-posed from ill-posed inference regimes (type-composition confounding)
- Extensive experiments on synthetic data validating theoretical predictions
