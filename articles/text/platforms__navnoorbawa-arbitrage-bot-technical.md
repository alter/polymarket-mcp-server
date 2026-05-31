---
title: "Building a Prediction Market Arbitrage Bot: Technical Implementation"
url: "https://navnoorbawa.substack.com/p/building-a-prediction-market-arbitrage"
source: substack
date: "2025"
type: article
theme: platforms
lang: en
---

# Building a Prediction Market Arbitrage Bot: Technical Implementation

## Core Strategy
$40 million in arbitrage profits extracted from Polymarket between April 2024–April 2025 through exploitation of structural mispricings.

### Primary Opportunities
- **Single-condition arbitrage**: When YES and NO token prices don't sum to $1.00. $10.58M extracted via this mechanism across 86 million bets.
- **Cross-platform fragmentation**: Between Kalshi and Polymarket. During September 2025, Kalshi captured 62% of prediction market volume while Polymarket held 37%.

## Technical Architecture

### API Integration
- Polymarket: `py_clob_client` with WebSocket monitoring on Polygon Layer 2 (Chain ID 137). Off-chain matching with on-chain settlement via conditional token framework.
- Kalshi rate limits: 20 requests/second reads on basic tier, 100 requests/second on premier tier.

### Mispricing Detection
```
Expected profit = |Sum price - $1.00| × Position size
```
Notable: one trader converted $0.02 into $58,983.36 exploiting severe mispricing where both YES/NO traded below $0.02.

### Execution Management
**Leg risk** is the critical constraint — non-atomic execution means one position might fill while the hedge fails. Solution: 5-second timeout windows with order cancellation safeguards for partial fills.

75% of matched orders execute within 950 blocks (~1 hour on Polygon).

## Risk Management

### Gas Optimization
Polygon transaction costs at typical 30-100 gwei gas prices, 150,000 unit limits per CLOB order.

### Oracle Risk
Cross-platform arbitrage faces resolution divergence risks. March 2025 incident: whale with 25% of UMA voting power manipulated resolution on a $7M market, biggest loser forfeited $73,000. Mitigation: avoid cross-platform positions unless spreads exceed 15 cents.

## Performance Metrics (April 2024–April 2025)
- Single-condition: $10.58M
- Market rebalancing: $28.99M
- Combinatorial arbitrage: ~$95K
- **Total: $39.6M**

Top performer: 4,049 transactions → $2.01M ($496 average per trade).

## Market Trajectory
ICE invested $2B in Polymarket at $8B valuation in October 2025. Opportunity window narrowing as institutional capital enters (analogous to early crypto exchange arbitrage before institutional market makers compressed spreads).
