---
title: "Resolution - Polymarket Documentation (UMA Oracle)"
url: https://docs.polymarket.com/concepts/resolution
source: docs.polymarket.com
date: "2026-05-30"
type: documentation
theme: defi
lang: en
---

# Polymarket Market Resolution: UMA Optimistic Oracle

## Core Resolution Process

Polymarket employs the "UMA Optimistic Oracle for decentralized, permissionless resolution." Anyone can propose outcomes, and anyone may challenge proposals they believe are incorrect.

## Resolution Workflow

**Proposal Phase**: Users submit resolution proposals by selecting a winning outcome, posting a bond (typically $750 pUSD), and submitting to UMA. Incorrect or premature proposals forfeit the entire bond.

**Challenge Period**: A 2-hour window follows proposals where disputes can be filed. No dispute results in market resolution; disputes trigger additional proposal rounds or escalation.

**Dispute Mechanism**: Challengers post matching counter-bonds to trigger new proposal rounds or activate UMA's Data Verification Mechanism for token holder votes.

**UMA Voting**: If escalated, UMA token holders vote on the correct outcome over approximately 48 hours. Winners receive their bond plus half the loser's bond.

## Resolution Outcomes

- **Proposer Wins**: Original proposal accepted; proposer recovers bond plus disputer's half-bond
- **Disputer Wins**: Disputer receives bond plus proposer's half-bond
- **Too Early**: Event hasn't concluded; disputer recovers bond plus proposer's half-bond
- **Unknown/50-50**: Rare outcomes resolve 50/50; tokens redeem for $0.50 each

## Post-Resolution

Winning tokens redeem through the CTF collateral adapter for pUSD (100 tokens = $100 pUSD). Losing tokens become worthless. Trading ceases immediately upon resolution.

## Timeline

- Undisputed: ~2 hours
- Disputed: 4-6 days total

## Bond Scale by Market Size

- Standard markets ($0-$250k): $750
- High-value ($250k-$5m): $5,000
- Premium ($5m+): $10,000+

## Dual Oracle System

Polymarket uses Chainlink to resolve markets that depend on real-time numbers like crypto prices. With the integration of Chainlink Data Streams and Chainlink Automation, Polymarket can run fast-resolving markets such as 15-minute crypto price markets. UMA is used for questions where outcomes are harder to verify or do not have a single data source.

## Three Resolution Flows

1. **No dispute**: Propose then Resolve (fastest, ~2 hours)
2. **One dispute**: Propose, Challenge, second Propose, Resolve (second proposal accepted)
3. **Two disputes**: Propose, Challenge, second Propose, second Challenge, Resolve via DVM vote
