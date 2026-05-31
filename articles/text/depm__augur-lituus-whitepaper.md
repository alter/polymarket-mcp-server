---
title: "The Augur Lituus Whitepaper: Modular Oracle for Outsourced Resolution"
url: "https://www.augur.net/blog/the-augur-lituus-whitepaper/"
source: augur.net
date: "2026-01-29"
type: whitepaper
theme: depm
lang: en
---

# The Augur Lituus Whitepaper: Modular Oracle for Outsourced Resolution

**Published:** January 29, 2026  
**URL:** https://www.augur.net/blog/the-augur-lituus-whitepaper/  
**GitHub:** https://github.com/AugurProject/whitepaper  
**Coverage:** https://thedefiant.io/news/defi/augur-reveals-augur-lituus-oracle-whitepaper  
**Medium:** https://medium.com/@lituusfoundation/generalized-augur-a-cross-chain-decentralized-truth-machine-d28b92bf64c2

## Overview

Augur Lituus is an Ethereum-native truth-telling protocol that generalizes oracle function into infrastructure. Rather than competing with existing prediction markets, it is designed as infrastructure to support them—providing a shared, manipulation-resistant resolution layer that prediction markets, DeFi protocols, and cross-chain systems can rely on.

The whitepaper presents a proposed oracle design and a comparative analysis of the security of existing decentralized oracle mechanisms. The paper is centered on a practical question: what does it actually cost to manipulate resolution, and how can that cost be pushed as high as possible?

## Two Core Contributions

1. A modular oracle design intended to serve as a shared resolution layer for other protocols
2. An evaluation of widely used oracle mechanisms with comparative analysis of attack costs under realistic economic assumptions

## How It Works: Algorithmic Forking

Augur's oracle relies on an algorithmic fork as its final backstop. When disputes escalate beyond a certain threshold, the protocol splits into parallel universes for each outcome, and every REP holder must actively choose which outcome they believe reflects reality by migrating their tokens. Universes built on false outcomes are expected to lose all economic value.

## New Anti-Manipulation Mechanism: Migration-Based Universe Forking with Supply Restoration

This mechanism re-mints and auctions REP tokens after a fork, forcing anyone trying to manipulate results to buy dominance twice. According to the document:
- Attacks under this design would cost roughly **134% of the oracle's fully diluted valuation**
- Up from about **92%** under the previous Augur design

## PBFM: Price-Based-Mintable-Forking

The Lituus Foundation introduces the PBFM (Price-Based-Mintable-Forking) mechanism designed by original game theorist Ryan Garner. This framework creates market-based incentives against attackers and enables third-party oracle use without compromising security.

## Cross-Chain Vision

Augur's oracle is envisioned as a general-purpose oracle-as-a-service, capable of resolving any applicable query submitted with a fee. Resolution layer remains on Ethereum L1 to preserve economic integrity, while queries can originate from any chain with cross-chain messaging capability.

This turns Augur into a modular, neutral source of truth—ready to plug into prediction markets, price feeds, DAOs, games, insurance protocols, and more.

## Development Status

The whitepaper marks the transition from research to implementation. Augur Lituus is one of two products currently being developed under the Lituus Foundation. Development proceeds openly with releases driven by readiness—no public fundraising round or promotional roadmap.

In 2026, the mechanism is being exercised through a planned test fork initiated independently by long-time Augur contributor Micah Zoltu.

## Market Reaction

Following the whitepaper release, the price of REP briefly shot up 30% from around $0.74 to $0.97, before retracing.

## Mission Statement

The Lituus Foundation aims to build "open-source, trustless systems that do not serve insider interests" and operate without central control or permission requirements.
