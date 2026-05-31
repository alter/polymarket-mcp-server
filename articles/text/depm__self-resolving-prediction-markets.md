---
title: "Self-Resolving Prediction Markets for Unverifiable Outcomes"
url: "https://arxiv.org/abs/2306.04305"
source: arxiv
date: "2023-06-07"
type: paper
theme: depm
lang: en
---

# Self-Resolving Prediction Markets for Unverifiable Outcomes

**Authors:** Siddarth Srinivasan, Ezra Karger, Yiling Chen  
**arXiv:** 2306.04305  
**PDF:** https://arxiv.org/pdf/2306.04305

## Abstract

The paper addresses a fundamental challenge in prediction markets: outcomes that cannot be verified or accessed. The researchers propose a novel mechanism that aggregates agent predictions without observing the actual ground truth.

## The Problem

Traditional prediction markets require an oracle to verify outcomes. But many valuable questions have unverifiable outcomes:
- Long-term forecasts that resolve after participants lose interest
- Private information that can never be made public
- Subjective outcomes with no agreed ground truth

## The Solution

Participants are paid based on how closely their forecasts align with a strategically selected reference agent—one with access to superior information. The market self-resolves probabilistically after each report, with most agents compensated based on the final prediction. The final agent becomes the reference point, having observed the complete forecast history.

**Mechanism:**
1. Agents sequentially submit probabilistic predictions
2. Market terminates randomly with probability α after each prediction
3. All but the last few agents are paid based on the final agent's prediction
4. The final agent (with full history) serves as the reference

## Key Findings

- Achieves **incentive compatibility**: truthful reporting constitutes a "perfect Bayesian equilibrium" where all agents honestly reveal their beliefs
- Works for **both unverifiable and verifiable outcomes**—broader applicability than initially conceived
- Enables aggregation of dispersed knowledge without requiring external verification

## Significance for Decentralized Prediction Markets

This mechanism addresses a key limitation in oracle-dependent designs (Augur, Polymarket):
- Eliminates oracle dependency for subjective/unverifiable outcomes
- Creates self-contained truth-telling through sequential incentive alignment
- Opens prediction markets to domains previously inaccessible due to verification requirements
