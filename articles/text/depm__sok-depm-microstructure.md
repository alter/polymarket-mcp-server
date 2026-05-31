---
title: "SoK: Market Microstructure for Decentralized Prediction Markets (DePMs)"
url: "https://arxiv.org/abs/2510.15612"
source: arxiv
date: "2025-10-17"
type: paper
theme: depm
lang: en
---

# SoK: Market Microstructure for Decentralized Prediction Markets (DePMs)

**Authors:** Nahid Rahman, Joseph Al-Chami, Jeremy Clark  
**arXiv:** 2510.15612  
**Submitted:** October 17, 2025; Last revised: March 13, 2026 (v3)  
**Subject:** Computational Engineering & Finance; Cryptography & Security; Trading and Market Microstructure

## Abstract

Decentralized prediction markets (DePMs) allow open participation in event-based wagering without fully relying on centralized intermediaries. The authors review the history of DePMs, which date back to 2011 and include hundreds of proposals. Perhaps surprisingly, modern DePMs like Polymarket deviate materially from earlier designs like Truthcoin and Augur v1.

## Motivation and Context

In late 2024, the United States was in the midst of a presidential election when Polymarket broke through mainstream news coverage. Stories focused on the fact that it offered odds more favourable to eventual winner Donald Trump than those reflected in conventional polls and forecasts. Polymarket's odds are not set by experts or pundits; instead, it is a specific type of betting market where odds are extrapolated from the prices of trades made in an open market.

## Modular Framework: Eight Stages

The paper presents a modular workflow comprising eight stages for analyzing decentralized prediction market design:

1. **Underlying infrastructure** — blockchain layer, consensus mechanism, smart contract platform
2. **Market topic** — event specification, question framing, scope definition
3. **Share structure and pricing** — binary/scalar/categorical tokens, AMM vs. CLOB
4. **Market initialization** — liquidity seeding, parameter setting, oracle designation
5. **Trading** — order matching, on-chain vs. off-chain components
6. **Market resolution** — oracle mechanisms, dispute resolution, forking
7. **Settlement** — payout distribution, token redemption
8. **Archiving** — historical record, data availability

For each module, the paper enumerates the design variants, analyzing trade-offs around decentralization, expressiveness, and manipulation resistance.

## Key Findings

- Modern DePMs like Polymarket deviate materially from earlier designs like Truthcoin and Augur v1
- The paper traces evolution from 2011 through hundreds of proposals to present-day architectures
- Identifies open problems for researchers interested in this ecosystem
- Synthesizes insights from computer science, cryptography, and quantitative finance

## Related Papers Identified

- The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election (arXiv:2603.03136)
- The Anatomy of a Decentralized Prediction Market: Microstructure Evidence from the Polymarket Order Book (arXiv:2604.24366)
