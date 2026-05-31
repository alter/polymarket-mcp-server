---
title: "NegRisk Market Rebalancing: How $29M Was Extracted From Multi-Condition Prediction Markets"
url: https://medium.com/@navnoorbawa/negrisk-market-rebalancing-how-29m-was-extracted-from-multi-condition-prediction-markets-2f1f91644c5b
source: medium.com
date: "2026-05-30"
type: article
theme: defi
lang: en
---

# NegRisk Market Rebalancing: $29M Extracted

## Key Findings

The research analyzed 86 million Polymarket bets over 12 months (April 2024–April 2025), documenting total arbitrage of $39.59M with NegRisk rebalancing accounting for $29M (73%) of profits.

## The Core Mechanism

NegRisk markets involve three or more mutually exclusive outcomes where prices must sum to 1.0. The inefficiency emerges when liquidity fragments unevenly—retail traders concentrate bets on favorite outcomes while complementary probabilities trade thinly, causing the sum to deviate from 1.0.

## Capital Efficiency Advantage

The analysis revealed a critical insight: NegRisk rebalancing generated 73% of total profits while representing only 8.6% of market opportunities, suggesting a 29× capital efficiency advantage over binary arbitrage.

## Example Mechanism

Consider a political prediction scenario where three outcomes (Trump 0.48, Harris 0.46, Other 0.03) sum to 0.97, creating a 3-cent arbitrage opportunity. An arbitrageur buys all YES tokens for $0.97 and holds until resolution — exactly one pays $1.00, guaranteeing $0.03 profit.

## Total Arbitrage Breakdown

- Single condition arbitrage: $10.6 million
- Within-market (NegRisk) arbitrage: $32.7 million  
- Cross-market arbitrage: ~$95k across 5 pairs
- Combined total: ~$39.6 million

The top arbitrageur extracted $2.0 million across 4,049 transactions.

## Trading Implications

For Polymarket traders:
1. NegRisk markets regularly underprice the complete probability set
2. Buying all YES tokens when sum < $1.0 is mathematically guaranteed profit if held to resolution
3. The "convert" function in the NegRiskAdapter is the mechanism for executing this strategy on-chain
4. Sports markets showed consistent opportunities throughout the year vs. Politics markets peaking around election cycles
