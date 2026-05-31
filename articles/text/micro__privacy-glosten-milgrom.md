---
title: "The Privacy Subsidy in Glosten-Milgrom: Bid-Ask Spread and Welfare under Flip-Noise Direction Observation"
url: https://arxiv.org/abs/2605.19742
source: arxiv
date: "2026-05-19"
type: paper
theme: micro
lang: en
---

# The Privacy Subsidy in Glosten-Milgrom: Bid-Ask Spread and Welfare under Flip-Noise Direction Observation

**Author:** Yuki Nakamura

**Submitted:** May 19, 2026; Last revised May 27, 2026

**arXiv:** 2605.19742

## Abstract

Derives a closed-form bid-ask spread and welfare decomposition for the Glosten-Milgrom 1985 model when market makers observe trade direction with privacy noise (flip-noise).

**Equilibrium spread formula:** μ(1-2η)Δ

where:
- μ = informed-trader fraction
- η = flip probability (privacy noise level)
- Δ = asset value range

## Key Findings

1. **Privacy subsidy:** μηΔ per trade transferred from protocol to traders (from market maker perspective)
2. **Closed-form spread formula** for noisy direction observation — analytically tractable
3. **Extends GM model** from discrete two-state to continuous framework with privacy noise
4. Applies to **MPC-based matching engines** and differentially-private direction disclosure
5. Results robust across classical microstructure frameworks

## The Privacy Subsidy Concept

When trade direction is noisy (η > 0):
- Market maker cannot perfectly infer whether trade was buy or sell
- Spread widens: (1-2η) factor reduces effective learning from trade direction
- The "privacy subsidy" represents the information rent lost by the market maker due to privacy noise
- This value is effectively transferred to traders (both informed and noise traders)

## Relevance to Polymarket CLOB Trading

- **Polymarket order-book direction uncertainty:** Dubach (2026) shows only 59% accuracy in trade direction inference from public feed — effectively η ≈ 0.20 of flip noise in public data
- **Spread implications:** The 59% accuracy means spread estimation from public feed is biased — use on-chain data (as Dubach does) for accurate calibration
- **Privacy by design:** If Polymarket implements privacy-preserving execution, this model predicts spread widening
- **MPC relevance:** Decentralized exchanges using zero-knowledge proofs could introduce η > 0 in direction observation — model predicts market consequences

**Classification:** Computer Science and Game Theory; Trading and Market Microstructure (very recent, May 2026)
