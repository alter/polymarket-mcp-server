---
title: "Polymarket Liquidity Rewards Program: Complete Documentation"
url: "https://docs.polymarket.com/market-makers/liquidity-rewards"
source: "docs.polymarket.com"
date: "2026-04-28"
type: "documentation"
theme: "arb"
lang: "en"
---

# Polymarket Liquidity Rewards Program

**Primary URL:** https://docs.polymarket.com/market-makers/liquidity-rewards

**Also:**
- https://help.polymarket.com/en/articles/13364466-liquidity-rewards
- https://docs.polymarket.com/polymarket-learn/trading/maker-rebates-program
- https://github.com/Polymarket/polymarket-liq-mining

---

## Two Parallel Incentive Programs

### 1. Liquidity Rewards Program
- Pays for having resting limit orders near midpoint
- Orders do NOT need to be filled to earn
- Daily payouts at midnight UTC
- Minimum payout: $1 USDC

### 2. Maker Rebates Program
- Pays share of taker fees when resting limit orders ARE filled
- Makers pay zero trading fees
- 20-25% of taker fees redistributed daily in PUSD

**These stack:** A resting order near midpoint earns both programs simultaneously.

---

## Reward Formula

**Order Score:** `S(v, s) = ((v - s) / v)² × b`

Where:
- `v` = market's max spread parameter (in cents)
- `s` = your actual spread from midpoint
- `b` = order size in shares

**Quadratic effect:** Moving from 3¢ away to 1¢ away doesn't triple your score — it increases it by ~9×. Strong incentive for tight quoting.

---

## Market-Level Aggregation

**Side scores:** Separate Q_one and Q_two scores aggregate weighted scores for YES and NO contract sides.

**Minimum score adjustment:** System takes minimum of both sides to incentivize balanced quoting. But single-sided orders still earn at 1/3 value when midpoint is between 0.10-0.90.

**Two-sided requirement near extremes:** When midpoint is outside 0.10-0.90 range, single-sided orders earn NOTHING. Both sides required.

**Normalization:** Individual scores / total market scores (per sample), summed across 10,080 weekly samples, then normalized against epoch totals.

---

## April 2026 Incentive Pools

- **Total:** $5M+ in liquidity incentives for sports and esports
- **English Premier League:** $10,000 per game (72% live, 28% pre-game)
- **Esports tier-A (CS2, LoL):** $5,500 per game
- **Esports tier-C:** $500 per game
- **CLOB v2 launch bonus:** $1M in rewards on April 28, 2026 (distributed in first hours post-migration)

---

## Historical Program Evolution

**Legacy AMM era:** LP-based system where users added liquidity to AMM pools, earned fee share
**CLOB v1:** Reward formula copied from dYdX, adapted for binary markets
**CLOB v2 (April 28, 2026):** New pUSD collateral token, rewritten backend, $1M launch rewards

---

## Eligibility

Market categories with taker fees enabled (eligible for maker rebates):
- Crypto, Sports, Finance, Politics, Economics, Culture, Weather, Tech, Mentions, Other/General

---

## Practical Earning Potential

**Early program (open-source LP):** $200-300 USDC/day with ~$10,000 capital at peak
**Current (competitive):** Rewards now more of a "thin bonus" on top of real trading edge
**Professional market makers:** $150-300/day per market with $100K+ daily volume markets

---

## Key Risk: Reward vs. Market Making Tension

Reward farming → incentivizes tight quotes (maximize score)
Market making → incentivizes wider quotes (protect against adverse selection)

Every cent of spread sacrificed for reward eligibility = cent less protection against adverse selection. Traders must consciously balance both objectives.
