---
title: "Unravelling the Probabilistic Forest: Arbitrage in Prediction Markets"
url: https://arxiv.org/html/2508.03474v1
source: arxiv.org
date: "2026-05-30"
type: paper
theme: defi
lang: en
---

# Arbitrage in Prediction Markets: A Comprehensive Analysis

## Abstract

This research examines arbitrage opportunities on Polymarket, a blockchain-based prediction market platform. The study identifies two distinct arbitrage types: Market Rebalancing Arbitrage (within single markets) and Combinatorial Arbitrage (across dependent markets). The authors estimate approximately $40 million in realized profits extracted during their measurement period (April 2024 - April 2025).

## Introduction

Prediction markets aggregate collective forecasts about future events. Polymarket, built on the Polygon blockchain, enables users to trade conditional tokens representing different outcomes. The platform's design requires that mutually exclusive conditions sum to a total probability of 1. When pricing discrepancies violate this principle, sophisticated traders can exploit these inefficiencies for guaranteed profits.

The research addresses three core questions:
- What market conditions enable arbitrage?
- Does arbitrage actually occur on Polymarket?
- Have participants exploited these opportunities?

## Methodology

### Market Dependency Detection

The researchers employed a novel approach combining heuristic reduction with large language models. They used DeepSeek-R1-Distill-Qwen-32B to analyze semantic relationships between markets. The LLM evaluated logical dependencies between conditions by determining valid outcome combinations. Markets were grouped by topic using text embeddings and temporal proximity to reduce the computational complexity from O(2^(n+m)) comparisons.

For single markets, the LLM achieved 81.45% accuracy in identifying valid outcome spaces. To handle LLM limitations with longer inputs, the team reduced markets with >4 conditions to their top 4 conditions by trading volume plus a catch-all condition.

### Data Collection

The study analyzed:
- 8,659 single-condition markets
- 1,578 multiple-condition markets (8,559 total conditions)
- 86 million executed bids from January 2024 - April 2025

Data sources included the Polymarket API for market descriptions and Alchemy nodes for on-chain transaction data. The researchers extracted three key event types: OrderFilled, PositionSplit, and PositionsMerge.

### Arbitrage Opportunity Analysis

The team calculated volume-weighted average prices (VWAP) for each position using one-block windows, carrying forward prices for up to 5,000 blocks (approximately 2.5 hours) without trading. They only analyzed conditions with <95% probability to capture periods of genuine market uncertainty. They focused on opportunities yielding ≥$0.05 profit per dollar.

## Key Findings

### Market Dependencies

Among 46,360 market pairs in the U.S. Politics group (November 5, 2024), the researchers identified 1,576 dependent pairs after filtering invalid outputs. Manual verification confirmed 13 pairs satisfying strict combinatorial arbitrage definitions.

### Arbitrage Opportunities Detected

**Single Conditions:**
- 7,051 of 17,218 conditions exhibited at least one arbitrage opportunity
- Median profit: ~$0.60 per dollar (remarkable inefficiency)
- Long arbitrage dominated (sum <$1)

**Within Markets:**
- 662 of 1,578 NegRisk markets showed arbitrage opportunities
- Average of ~100 opportunities per affected market
- Both long and short arbitrage observed

**Across Markets:**
- Limited inter-market arbitrage detected
- Most opportunities during lower-liquidity periods

### Realized Arbitrage Extraction

The analysis revealed significant profit extraction:
- Single condition arbitrage: $10.6 million total
- Within-market arbitrage: $32.7 million total
- Cross-market arbitrage: ~$95k across 5 pairs
- Combined total: Approximately $39.6 million

The top arbitrageur extracted $2.0 million across 4,049 transactions.

## Notable Patterns

The "Tutaaa91" account demonstrated exceptional profit extraction through simultaneous YES/NO token purchases below $0.02 each, realizing single-trade profits exceeding $58,000. This suggests markets occasionally experience severe mispricing relative to actual probabilities.

Sports markets demonstrated consistent arbitrage opportunities throughout the measurement period, while Politics markets concentrated opportunities during the U.S. election cycle. Buying NO positions proved surprisingly profitable, contradicting typical market expectations.

## Limitations and Future Work

The LLM occasionally encountered logical loops when processing complex markets, suggesting constraints in handling large condition sets. The researchers identified weaker dependency relationships (such as temporal dependencies in tournament structures) as open problems for future investigation.

## Conclusion

This research provides empirical evidence that prediction markets exhibit substantial pricing inefficiencies enabling arbitrage. While arbitrage volumes remain modest compared to decentralized finance mechanisms, the $40 million extraction during the measurement period represents significant value capture. The methodology combining LLMs with on-chain analysis offers scalable approaches for future prediction market analysis as platforms evolve toward greater decentralization.
