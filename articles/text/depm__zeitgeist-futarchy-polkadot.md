---
title: "Zeitgeist: Decentralized Prediction Markets and Futarchy on Polkadot"
url: "https://polkadotters.medium.com/zeigeist-prediction-markets-on-kusama-b94c55e5bec9"
source: medium.com
date: "2021-01-01"
type: article
theme: depm
lang: en
---

# Zeitgeist: Decentralized Prediction Markets and Futarchy on Polkadot

**Sources:**  
- https://polkadotters.medium.com/zeigeist-prediction-markets-on-kusama-b94c55e5bec9  
- https://medium.com/oneblock-community/decentralized-prediction-market-protocol-zeitgeist-pioneering-a-new-paradigm-for-future-governance-c845b2c3ef4d  
- https://github.com/zeitgeistpm/zeitgeist  
- https://zeitgeist.pm/

## Overview

Zeitgeist is an evolving blockchain for prediction markets and futarchy, built on the Substrate blockchain framework. The platform emphasizes permissionless market creation and integrates futarchy as its core governance model. It launched as a parachain of Kusama and migrated to Polkadot.

**GitHub:** https://github.com/zeitgeistpm/zeitgeist (Rust/Substrate)

## Prediction Market Types

- **Categorical markets:** Binary yes/no options or multiple choices
- **Scalar markets:** Range-based predictions (e.g., Bitcoin price forecasts)  
- **Combinatorial markets:** Composite event betting for strategy evaluation
- **LS-LMSR AMM:** An automated market maker adjusting prices based on user predictions (Zeitgeist's "neo-swaps" — LMSR as constant function market maker)

## Futarchy Governance Model

Zeitgeist is the first parachain project in the Substrate ecosystem to introduce futarchy as a governance form—possibly the first blockchain project to implement futarchy at all.

**How it works:** Rather than simple token voting ("rule of the rich"), futarchy uses prediction markets to inform decisions:
1. A governance proposal is submitted (e.g., "increase max supply of ZTG by 20%")
2. A prediction market is created on whether this action will have positive or negative price impact
3. People bet real money → incentivized to use their best knowledge
4. If majority predicts positive effect, proposal passes; if negative, it fails
5. On-chain execution based on market result

## Addressing Traditional Governance Failures

Senior Blockchain Engineer Malte Kliemann characterizes traditional on-chain voting as "rule of the rich" due to voting power concentration among large token holders. Futarchy uses prediction markets to inform decisions instead, separating values (human vote) from beliefs (market signal).

## Technical Architecture

- Built on **Substrate** (same framework as Polkadot)
- Uses **XCM (Cross-Consensus Messaging)** for modular cross-chain development
- Can operate across all Polkadot and Kusama parachains
- Accepts assets from other parachains (kUSD, aUSD, etc.)
- **Zeitgeist Court:** Dispute resolution for contested outcomes
- **SDK:** Any project can embed prediction markets or futarchy

## Polkassembly Partnership

Zeitgeist partnered with Polkassembly (open-source governance platform for Substrate chains) to allow Polkadot community members to predict success/failure of on-chain proposals and earn rewards. Markets denominated in $DOT tokens.

## Team Background

- Logan Saether (former Web3 Foundation)
- Chris Hutchinson (Polkadot ambassador program creator)  
- Dave Perry (prediction market veteran)

## Relationship to Hanson/Futarchy

Zeitgeist represents the first practical blockchain implementation of Robin Hanson's futarchy concept—where bets dictate actions. Unlike conventional systems where votes may not translate into real-world effects, futarchy aligns successful predictions with actionable decisions. As one analysis notes: "For the first time in human history, we can try to create a system that governs itself by market decisions of incentivized participants."
