---
title: "Market Making in Sports Betting: How Quant Firms Extract Alpha from Exchange Microstructure"
url: https://navnoorbawa.substack.com/p/market-making-in-sports-betting-how
source: substack
date: "2025-12-01"
type: blog
theme: sports
lang: en
---

# Market Making in Sports Betting: Exchange Microstructure & Quant Strategies

**Source:** Navnoor Bawa, Substack (December 2025)

## Exchange Architecture (Betfair)

Betfair operates as an **order-driven continuous double auction** (established 2000). Back orders = ask prices (buying outcome bets). Lay orders = bid prices (selling bets). 5% commission on net profits (reducible to 2%).

**Scale:** 7 million+ transactions daily — more than all European stock exchanges combined.

## Institutional Players

- **Susquehanna International Group (SIG)**: Operates Nellie Analytics (Dublin, 2017) — focuses on in-game wagering, applies same statistical modeling used in options trading
- **Jane Street**: Reportedly built specialized sports betting teams
- **Jump Trading**: Active in prediction market microstructure

## Three Core Trading Strategies

### 1. Market Making
Place simultaneous back and lay orders across all outcomes. Target **105–110% book percentage** (overround).

Revenue formula:
```
Gross = Total matched volume × (book% - 100%)
Net = Gross - exchange commission
Example: £50,000 market, 108% book → £4,000 gross, £3,800 net
```

Requirements: continuous rebalancing, millisecond response times, automated algorithms.

### 2. Cross-Book Arbitrage

Identify discrepancies where implied probabilities sum to <100% across platforms.

Challenges:
- 98% of opportunities yield <1.2% profit
- Odds shift within seconds; partial fills eliminate edge
- Bookmakers impose account limits on consistent winners

### 3. In-Play Order Flow Trading

Monitor order book depth and bet size asymmetry to predict short-term odds movements.

Predictive signals:
- Large back orders accumulating 3–5 ticks below market → institutional positioning
- Sudden volume spikes → information flow
- Order book imbalances → imminent price movement
- Correlation with live events (score changes, injuries)

SIG reportedly adjusts win probabilities on **microsecond timescales** per individual play decisions.

## Success Case: Priomha Capital

- Established 2009 as The Cloney Multi-Sport Investment Fund
- **118% returns by end of 2011** (S&P 500/ASX 200 lost 17.4%)
- **17% average annual ROI** (2010–2015 after fees)
- Sports: Premier League, cricket, horse racing, golf, tennis
- 30% performance fee + management fee

## Revenue Sources & Risks

**Revenue sources:**
1. Bid-ask spread capture (2–8 ticks)
2. Book percentage premium above 100%
3. Arbitrage edge (0.5–1.2% per opportunity)
4. Momentum scalping on in-play volatility
5. Transaction rebates for liquidity provision

**Critical risks:**
- Account restrictions: bookmakers systematically limit winning traders
- Liquidity fragmentation: sports spreads across thousands of events, preventing institutional-scale capital (beyond £10–20M per strategy)
- Execution risk: Centaur Galileo hedge fund collapsed 2012, lost $2.5M investor capital

## Core Insight

"Sports betting represents options trading with alternative underlying assets, where quants apply existing quantitative frameworks to new markets rather than requiring domain-specific sports expertise."

Key: **Alpha derives from microstructure rather than prediction.** Scale constraints create persistent inefficiencies because markets cannot support large-scale capital deployment.

## Transferable Edges

1. Back/Lay structure on Betfair mirrors Bid/Ask — financial market microstructure tools apply directly
2. Order flow imbalance as a leading price indicator
3. Zero correlation to traditional asset classes → genuine portfolio diversification
4. Event contract platforms (Kalshi, Polymarket) are converging toward this exchange model
