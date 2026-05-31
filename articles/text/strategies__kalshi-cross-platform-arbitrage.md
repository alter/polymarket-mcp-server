---
title: "Prediction Market Arbitrage Strategies: Cross-Platform Trading Between Kalshi and Polymarket"
url: https://ahasignals.com/research/prediction-market-arbitrage-strategies/
source: ahasignals.com
date: "2026"
type: blog
theme: strategies
lang: en
---

# Cross-Platform Arbitrage: Polymarket vs Kalshi

## Core Framework

AhaSignals presents prediction market arbitrage as "evidence of segmented consensus and limits of arbitrage" rather than purely risk-free opportunity.

## The Three-Step Checklist

1. **Gross Spread Identification:** Compare settlement language across platforms — ensure genuine comparability
2. **After-Fees Adjustment:** Deduct platform fees, bid-ask spreads, slippage, conversion friction, capital lockup costs
3. **Consensus Signal Interpretation:** Treat remaining gaps as market-structure evidence vs. executable trades

## Fee Analysis

| Platform | Taker Fees | Maker Fees |
|----------|-----------|-----------|
| Polymarket | Zero (most markets) | Zero + 20-25% rebate |
| Kalshi | ~1.2% of contract value | Varies |

**Break-even:** Need gross spread ≥ 1.75–2.5 cents per contract after fees. Combined YES+NO < $0.98 is worth calculating.

## Why Price Divergences Persist

1. **Regulatory segmentation:** Kalshi = CFTC-regulated US exchange; Polymarket = crypto-native global
2. **Different user bases:** Crypto-native global traders vs US-focused regulated finance users
3. **News reaction speed:** Different rates of price discovery
4. **Transaction costs:** Friction prevents instant equalization
5. **Settlement risk premium:** Platforms have different resolution risks

## The Settlement Mismatch Risk (Critical)

**2024 US Government Shutdown Case:**
- Polymarket judged: "OPM issues shutdown announcement" → YES
- Kalshi judged: "Actual shutdown exceeding 24 hours" → NO

Same event, opposite resolution. Arbitrageurs lost both legs.

**Lesson:** ALWAYS verify resolution criteria before executing cross-platform arbitrage. Read every clause.

## Six Key Risks

1. **Execution risk:** Prices shift between initiating trades on different venues
2. **Settlement risk:** Platforms may resolve events differently or experience technical failures
3. **Regulatory risk:** US traders face legal uncertainty accessing offshore markets
4. **Smart contract risk:** Polymarket blockchain infrastructure inherent vulnerabilities
5. **Liquidity risk:** Thin markets may prevent favorable exit pricing
6. **Capital lockup:** Funds remain tied until event resolution

## Partial Fill Problem

Most common failure mode: Kalshi fills 60 of 100 contracts but Polymarket runs dry at 40. You're holding net directional position on 20 contracts.

**Policy options before this happens:**
- Accept the directional exposure
- Immediately close the unfilled leg
- Set minimum fill threshold (never trade if <80% fill available)

## Automation Requirements

Cross-platform arbitrage has become too fast for manual execution:
- WebSockets over REST (200–800ms savings per trade)
- Multicall/batching: bundle both orders into single call
- If one side fails → smart contract wrapper reverts everything → prevents one-legged trades
- Pre-funded accounts on both platforms (capital fragmentation cost)

## Open-Source Tools

- `github.com/ImMike/polymarket-arbitrage`: Python, watches 10,000+ markets with AI market matcher
- `github.com/TopTrenDev/polymarket-kalshi-arbitrage-bot`: Rust implementation
- EventArb.com: Manual calculator accounting for all fees
- Apify scraper: polymarket-kalshi-arb-finder

## Scale of Historical Opportunity

~$40 million in cross-platform arbitrage profits documented over 12-month period. But: "Real prediction markets are highly efficient" with rare executable opportunities. Most gross spreads are eliminated after fees.
