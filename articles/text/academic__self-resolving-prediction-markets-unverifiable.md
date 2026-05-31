---
title: "Self-Resolving Prediction Markets for Unverifiable Outcomes"
url: "https://arxiv.org/abs/2306.04305"
source: "arxiv"
date: "2023-06-07"
type: "paper"
theme: "academic"
lang: "en"
authors: ["Siddarth Srinivasan", "Ezra Karger", "Yiling Chen"]
---

# Self-Resolving Prediction Markets for Unverifiable Outcomes

## Abstract

The researchers address a critical gap in prediction markets: many important questions have outcomes that are difficult or impossible to verify. Their mechanism pays agents the negative cross-entropy between their prediction and that of a carefully chosen reference agent instead of relying on ground truth verification.

## Key Findings

1. **Novel Mechanism Design:** The authors propose using a reference agent with superior information access as a proxy for ground truth, eliminating the need for actual outcome verification.

2. **Self-Resolving Structure:** The market terminates probabilistically after each report, with the final agent serving as the reference point since they observe the complete forecast history.

3. **Theoretical Guarantee:** The paper demonstrates that it is a perfect Bayesian equilibrium (PBE) for all agents to report truthfully, ensuring honest participation without external verification.

4. **Broader Applicability:** While designed for unverifiable outcomes, the mechanism also functions effectively for verifiable scenarios.
