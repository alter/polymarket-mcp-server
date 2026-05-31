---
title: "Polymarket $7M Ukraine Mineral Deal: UMA Whale Governance Attack (March 2025)"
url: "https://www.coindesk.com/markets/2025/03/27/polymarket-uma-communities-lock-horns-after-usd7m-ukraine-bet-resolves"
source: CoinDesk
date: "2025-03-27"
type: news_article
theme: manip
lang: en
---

# Polymarket $7M Ukraine Mineral Deal: UMA Whale Governance Attack

**Date:** March 25–27, 2025  
**Primary sources:**
- CoinDesk: https://www.coindesk.com/markets/2025/03/27/polymarket-uma-communities-lock-horns-after-usd7m-ukraine-bet-resolves
- The Block: https://www.theblock.co/post/348171/polymarket-says-governance-attack-by-uma-whale-to-hijack-a-bets-resolution-is-unprecedented
- Orochi Network: https://orochi.network/blog/oracle-manipulation-in-polymarket-2025
- CoinTelegraph: https://cointelegraph.com/news/polymarket-trump-ukraine-bet-whale-governance-attack

## What Happened

A $7 million Polymarket prediction market — *"Ukraine agrees to Trump mineral deal before April?"* — saw its "Yes" probability surge from **9% to 100%** between March 24–25, 2025, despite no official agreement being reached.

## UMA Oracle System

Polymarket uses UMA Protocol's Optimistic Oracle (OO) for dispute resolution:
1. Anyone can propose an outcome by staking $750 USDC.e
2. 2-hour challenge period — anyone can dispute
3. If disputed → second proposal round → if again disputed → escalates to UMA DVM (Data Verification Mechanism)
4. UMA token holders vote; outcome is final

## The Attack

- Threat researcher Vladimir S. identified a single UMA whale controlling three accounts with **5 million UMA tokens** (25% of all votes in the dispute round)
- The whale voted to resolve the market as "Yes" despite no confirmed mineral deal
- The DVM process was not meaningfully challenged before resolution finalized
- Polymarket acknowledged: "This is an unprecedented situation"

## Structural Vulnerabilities Exposed

1. **Governance concentration:** Token-weighted voting allows large holders to override smaller participants. 25% stake proved sufficient for control when other voters were passive.
2. **Incentive misalignment:** Many UMA voters are also active Polymarket traders (WSJ: >60% of active UMA voters had Polymarket accounts; UMA voters held open positions in disputed markets in 300+ cases)
3. **Top-10 wallet dominance:** In the vast majority of dispute votes, top 10 wallet addresses account for >50% of all votes
4. **9 wallets controlled ~half of all UMA voting power** across 6,400+ addresses over 3 years

## Polymarket's Response

> "This is an unprecedented situation, and we have been in war rooms all day internally and with the UMA team to make sure this won't happen again. This is not a part of the future we want to build."

Platform acknowledged premature resolution but **did not issue refunds**, stating: "Because this wasn't a market failure, we are not able to issue refunds."

## Broader Pattern

This was not isolated:
- June 2024: UMA incorrectly resolved the Barron Trump / $DJT memecoin market → Polymarket overruled UMA and issued refunds
- January 2025: TikTok ban market ($120M volume) → disputed resolution, no refunds
- March 2025: Ukraine mineral deal → governance attack confirmed, no refunds

## Cryptographic Fix Proposed

Orochi Network analysis: zkDatabase with Zero-Knowledge Proofs could render governance attacks irrelevant — mathematical verification of outcomes would override any token-voting outcome. But no major platform has implemented ZKP-based oracles as of 2026.

## Key Statistic

As of April 2026: **230 disputed contracts** with combined volume exceeding **$1 billion** processed through UMA in one month (up from 79 contracts six months earlier).
