---
title: "Semantic Non-Fungibility and Violations of the Law of One Price in Prediction Markets"
url: "https://arxiv.org/abs/2601.01706"
source: arXiv
date: "2026-01-05"
type: academic_paper
theme: manip
lang: en
---

# Semantic Non-Fungibility and Violations of the Law of One Price in Prediction Markets

**Authors:** Jonas Gebele, Florian Matthes  
**arXiv:** 2601.01706  
**Submitted:** January 5, 2026  
**HTML:** https://arxiv.org/html/2601.01706v1

## Problem Defined

The modern prediction market ecosystem spans dozens of platforms (Polymarket, Kalshi, Manifold, PredictIt, etc.) that independently list economically identical events. Without a shared notion of event identity, liquidity fragments, arbitrage becomes capital-intensive and unenforceable, and prices systematically violate the **Law of One Price**.

## Core Concept: Semantic Non-Fungibility

Economically equivalent contingent claims are **semantically non-fungible** across platforms: they cannot be treated as interchangeable assets, netted across venues, or risk-free arbitraged without committing capital until resolution — because:

1. Natural language descriptions differ
2. Resolution semantics differ (what counts as "Yes"?)
3. Temporal scope may differ
4. Platform-specific governance and oracle risks differ

## Dataset

- **100,000+ events** across 10 major prediction market venues
- Time period: 2018–2025
- First human-validated, cross-platform dataset of aligned prediction markets

## Key Findings

| Metric | Value |
|--------|-------|
| Events listed concurrently across platforms | ~6% of all events |
| Persistent price deviation (semantically equivalent markets) | **2–4% on average** |
| Setting | Highly liquid, information-rich markets (not low-volume niche) |

These mispricings are driven by **structural frictions** rather than informational disagreement.

## Arbitrage Opportunities

Cross-platform arbitrage is theoretically possible but practically constrained by:
- Settlement timing differences
- Liquidity mismatch
- Oracle/resolution risk (one platform might resolve differently than another)
- Capital commitment until resolution (no risk-free close-out)

The Polymarket–Kalshi comparison specifically illustrates that fragmentation is **structural rather than incidental**.

## Proposed Solution

A **semantic alignment framework** making cross-platform event identity explicit through joint analysis of:
- Natural-language market descriptions
- Resolution semantics
- Temporal scope

Authors argue this is a prerequisite for prediction markets to aggregate information at a global scale.

## Implications for Efficiency

Law of One Price is the most basic test of market efficiency. Persistent 2–4% deviations across tens of thousands of contracts over 7 years indicate deep structural inefficiency — not attributable to informational asymmetry but to institutional and semantic fragmentation.

This has direct implications for the "wisdom of crowds" narrative: crowds cannot aggregate globally if they are systematically fragmented into non-communicating silos.
