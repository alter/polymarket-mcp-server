---
title: "Market Making on Polymarket — Earn the Spread Without Directional Bets"
url: https://startpolymarket.com/strategies/market-making/
source: startpolymarket.com
date: "2026"
type: blog
theme: strategies
lang: en
---

# Market Making on Polymarket CLOB

## What Market Makers Do

Market makers are agnostic about the outcome. The goal is not to predict what will happen but to **capture the bid-ask spread** as frequently as possible while managing the inventory that accumulates along the way.

Market making involves providing liquidity by continuously quoting both buy and sell prices for event contracts. You profit from the **difference between your bid and ask prices — the spread**.

## Fee Structure (2026)

**Makers pay zero fees.** Every limit order adding liquidity is free.

Makers also **receive 20–25% of all taker fees as daily PUSD rebates**.

A maker whose orders fill at flat mid — capturing zero P&L on the spread itself — is still net positive on rebates alone. This is the single most compelling reason to market-make on Polymarket.

## Two Official Keeper Strategies

The official `poly-market-maker` bot implements two strategies:

1. **Bands Strategy**: Places and cancels orders to keep open orders near the midpoint price within configurable price bands
2. **AMM Strategy**: Mimics automated market maker curve behavior for liquidity provision

## The Core Risk: Adverse Selection

Market making on binary outcome contracts is **not the same as market making on equities**.

**Adverse selection** — informed traders picking off stale quotes — can vaporize months of rebate income in a single market.

**Key risk:** News events can instantly move markets 40–50 points. If you're quoting 0.50/0.52 and the market should be at 0.90, you'll get filled on your 0.52 offers before you can cancel — locking in massive losses.

**Mitigation:** The trader who can **cancel stale orders fastest** when news breaks suffers fewer adverse fills.

## Infrastructure Requirements

- **VPS location:** Low latency to London (Polymarket's servers)
- **Language:** Python (`py_clob_client`), TypeScript (`@polymarket/clob-client`), or Rust/Go for lowest latency
- **WebSockets:** Use over REST; polling is legacy. Active sockets save 200–800ms
- **Capital:** $500–$2,000 for market making; $1,000+ for arbitrage

## Category Strategy

- Avoid markets with high news-event risk (elections near resolution, breaking political events)
- Focus on markets with predictable, slow-moving probability distributions
- Target new markets with wide spreads and thin books

## Practical Warning

One market maker noted: "In today's market, this bot is not profitable and suggests using it as a reference implementation for building your own market-making strategies."

The competitive landscape has intensified significantly in 2025–2026.

## Kelly Criterion for Market Makers

For market making, the relevant risk is not direction but inventory accumulation. Strategies:
- Cap maximum inventory per market (e.g., ≤5% of bankroll one-sided)
- Hedge accumulated inventory when it exceeds threshold
- Track per-market PnL and pause market-making when consecutive losing fills occur

## Referenced Tools

- `github.com/Polymarket/poly-market-maker` — official keeper
- `github.com/warproxxx/poly-maker` — community implementation with Google Sheets config
- `github.com/octavi42/prediction-market-maker` — placed #2 in Paradigm Prediction Market Challenge
