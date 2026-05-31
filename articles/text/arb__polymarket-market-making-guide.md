---
title: "Market Making on Polymarket: Earn the Spread Without Directional Bets"
url: "https://startpolymarket.com/strategies/market-making/"
source: "startpolymarket.com"
date: "2026-01-01"
type: "guide"
theme: "arb"
lang: "en"
---

# Market Making on Polymarket: Earn the Spread Without Directional Bets

**URL:** https://startpolymarket.com/strategies/market-making/

---

## Core Concept

Market making involves posting simultaneous buy and sell orders to capture the bid-ask spread. Rather than predicting outcomes, you provide liquidity and earn from the difference between your quotes.

**Key principle:** A market maker is agnostic about outcome. Goal is not to predict what will happen, but to capture the bid-ask spread as frequently as possible while managing inventory accumulation.

---

## Fee Structure Advantage

Polymarket's fee design makes market making highly attractive:
- **Makers pay zero fees** on every limit order that adds liquidity
- **Makers EARN rebates:** Platform redistributes 20-25% of taker fees as daily PUSD payments
- A maker whose orders fill at flat mid (zero spread P&L) is still **net positive on rebates alone**

This is unusual: on most exchanges, makers pay reduced fees rather than earning rebates.

---

## Two Income Sources (can stack)

1. **Spread capture:** P&L from buying bid and selling ask across fills
2. **Maker rebates:** Daily PUSD payments from taker fee redistribution
3. **Liquidity rewards:** Separate program paying for resting orders near midpoint (even unfilled)

Programs 2 and 3 can stack — a resting limit order near midpoint earns liquidity rewards for being there AND a maker rebate if it gets filled.

---

## Quote Setting

**Width tradeoff:**
- Wider spread: more cushion per trade, fewer fills
- Tighter spread: more fills, less safety margin against adverse moves
- Reward farming incentivizes tight quotes (maximize reward score)
- Market making incentivizes wider quotes (protect against adverse selection)

**Tension:** Every cent of spread sacrificed to qualify for rewards is a cent less protection against adverse selection.

---

## Inventory Management

As orders fill unevenly, directional exposure accumulates. Management techniques:
- Adjust quote midpoint to lean against inventory
- Trade out of positions in secondary market
- Hedge across correlated markets (e.g., two related political outcomes)
- Use NegRisk convert to rebalance without slippage

---

## The Critical Risk: Adverse Selection on Binary Markets

Binary contracts pose unique challenges vs. traditional market making:
- **News breaks → prices don't gradually shift** — they can move from 55¢ to 2¢ in seconds
- Informed traders exploit stale quotes
- Monthly spread earnings can evaporate in one bad event

**Mitigation strategies:**
- Avoid markets near resolution dates
- Widen spreads before major scheduled events
- Monitor news actively, cancel stale orders immediately
- Cap position sizes per market
- Prefer long-dated, information-diffuse markets

---

## Capital Requirements

- **Minimum to experiment:** A few hundred PUSD in single low-volume market
- **Serious multi-market operation:** Several thousand PUSD (absorb inventory swings)
- **Professional ($150-300/day reported per market):** $100K+ daily volume markets

---

## Practical Implementation Steps

1. Start with one market — understand its dynamics before scaling
2. Use Polymarket API for automated order management across multiple positions
3. Track P&L rigorously — spread income accumulates slowly, losses arrive suddenly
4. Run a market-neutral inventory book: track net delta at all times
5. Set hard stop-losses: X% adverse move triggers position exit

---

## Reward Formula

Liquidity rewards use quadratic scoring: `S(v,s) = ((v-s)/v)² * b`
- v = max spread parameter for that market
- s = your actual spread from midpoint
- b = order size in shares

Being 2× closer to midpoint doesn't double your score — it approximately **quadruples** it (quadratic). Strong incentive for professional MMs to continuously narrow spreads.

---

## Reality Check (from practitioners)

- Published open-source bots (e.g., warproxxx/poly-maker) are **not profitable as-is** due to competition
- Bot author explicitly states "will lose money" — use as reference, not deployment
- Market maker who tried clone: "didn't end up making any net profit overall" (operational errors)
- Competition from Wintermute, Jump Trading now in market
- Profitability requires: accurate probability modeling, fast order management, low-latency infrastructure
