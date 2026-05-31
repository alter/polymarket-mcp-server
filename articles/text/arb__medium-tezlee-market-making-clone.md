---
title: "I Cloned a Polymarket Market-Making Bot and Ran It: Here's What I Learned"
url: "https://tezlee.substack.com/p/i-cloned-a-polymarket-market-making"
source: "substack.com"
date: "2025-01-01"
type: "blog"
theme: "arb"
lang: "en"
---

# I Cloned a Polymarket Market-Making Bot and Ran It: Here's What I Learned

**URL:** https://tezlee.substack.com/p/i-cloned-a-polymarket-market-making

**Bot referenced:** https://github.com/warproxxx/poly-maker (@defiance_cr on X)

---

## Why Polymarket for Market Making?

- Competition relatively lower than TradFi/DeFi at time of experiment
- Maker rewards are attractive (zero fees + daily PUSD rebates)
- Binary YES/NO outcomes simplify risk management vs. continuous assets
- Open-source reference available

---

## Bot Architecture (warproxxx/poly-maker)

**Four Main Components:**

1. **Market Discovery:** Scrapes active Polymarket markets via API, calculates profitability scores using reward formula: `S = ((v - s) / v)² * b`. Filters and ranks markets by reward potential.

2. **Trade Execution:** WebSocket connection streams price updates. Maintains order book state. Decision logic → position sizing → order management → execution layers.

3. **Position Management:** Merges matching YES/NO inventory positions into USDC "without incurring slippage" (NegRisk convert function). Critical for capital efficiency.

4. **Risk Controls:** Stop-loss when PnL falls below threshold or volatility spikes. Take-profit logic exits at predetermined targets.

---

## What Went Wrong: Critical Learnings

### 1. Market Selection is Everything
"Low-volatility and tight-spread markets are far more forgiving." Volatile markets prevented timely hedging of directional risk. News events caused 40-50 point swings before cancel orders could execute.

### 2. Real-Time Position Tracking is Non-Optional
Early versions lacked accurate position snapshots. Without knowing exact inventory at each moment, risk management fails. After implementing proper inventory tracking, bot could manage risk effectively.

### 3. The Profitability Problem
Final result: **"didn't end up making any net profit overall."**

Despite generating maker rewards and spread earnings, operational errors (incorrect position sizing, missed cancellations, stale quotes) completely offset gains.

### 4. Operational Execution Rivals Strategy in Importance
Small inaccuracies compounded quickly. A missed cancel on a stale quote + adverse news = large single loss > weeks of accumulated spread income.

---

## The Poly Merger Module

A critical component often overlooked: `poly_merger` handles position merging on Polymarket. Built on open-source Polymarket code, it consolidates matched YES+NO positions into USDC via NegRisk convert. This:
- Reduces gas fees (one merge vs. two separate sells)
- Improves capital efficiency (recover USDC faster to redeploy)
- Eliminates spread cost on unwinding matched positions

---

## Key Recommendation

**Run manually before automating.** "Observe how adverse selection impacts your earnings" firsthand before deploying automated systems.

- Week 1: Manual quoting, track fill patterns
- Week 2: Paper-trade automation logic
- Week 3: Live automation with strict position limits
- Only scale after demonstrating consistent manual profitability

---

## Tech Stack (warproxxx/poly-maker)

**Dependencies:**
- `py-clob-client` (official Polymarket CLOB SDK)
- `web3` (Polygon interaction)
- `gspread` (Google Sheets for market parameter config)
- `pandas` (position tracking)
- `websockets` (real-time price feed)
- `eth-account` (transaction signing)

**Package manager:** UV (fast, reliable)

**Configuration:** Google Sheets — allows live parameter updates without redeployment
