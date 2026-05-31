---
title: "Prediction Markets as Bayesian Inverse Problems: Uncertainty Quantification and Information Gain"
url: "https://arxiv.org/abs/2601.18815"
source: arXiv
date: "2026-01"
type: academic_paper
theme: manip
lang: en
---

# Prediction Markets as Bayesian Inverse Problems

**Authors:** Juan P. Madrigal-Cianci (Kosmos Ventures; MC SAS), Camilo Monsalve Maya (MC SAS), Lachlan Breakey (Kosmos Ventures)  
**arXiv:** 2601.18815  
**Submitted:** January 2026  
**HTML:** https://arxiv.org/html/2601.18815v1

## Core Contribution

Reformulates prediction markets as **Bayesian inverse problems** where analysts infer unknown binary outcomes from observed price-volume histories. Rather than assuming perfect information aggregation (as in standard Hayek-based models), the paper explicitly models:
- Latent trader types (informed traders, noise traders, manipulators)
- Mixture model representation of heterogeneous participation
- Observable price-volume data as noisy observations of underlying outcomes

## Framework

Prediction markets are "stochastic sensing systems" where market histories provide noisy observations of underlying event outcomes. Uses log-odds coordinates (logit transformation of probabilities) with volume-dependent gating functions allowing trader composition to vary with market activity intensity.

## Technical Contributions

1. **Uncertainty Quantification:** Derives posterior concentration rates and finite-sample error bounds that decay exponentially based on outcome distinguishability metrics (Kullback-Leibler separation gaps)

2. **Identifiability Analysis:** Establishes explicit criteria for when outcome inference becomes ill-posed. Three failure modes identified:
   - Type-composition confounding
   - Outcome-nuisance symmetries
   - **Adversarial mimicry** (manipulators disguising themselves as informed traders)

3. **Information Gain Metrics:** Information-theoretic measures quantifying how effectively market histories reveal true outcomes — enables comparison across market designs

## Key Insight: When Manipulation is Hard to Detect

The adversarial mimicry failure mode is particularly relevant: a sophisticated manipulator can match the trading signature of an informed trader sufficiently closely to make Bayesian inference about the underlying outcome ill-posed. This provides theoretical grounding for why blockchain transparency alone is insufficient to detect sophisticated manipulation.

## Relationship to Prior Literature

- Hayek (1945): Dispersed knowledge aggregation via prices — assumed; this paper questions when it holds
- Wolfers & Zitzewitz (2004): Prices track event probabilities well on average
- Manski (2006): Prices confound beliefs with risk preferences — supports this paper's latent-type decomposition approach
