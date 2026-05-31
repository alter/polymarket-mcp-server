---
title: "Polymarket vs. Betfair Arbitrage and Cross-Platform Prediction Market Arbitrage"
url: "https://arbusers.com/polymarket-vs-betfair-arbitrage-t11041/"
source: "arbusers.com"
date: "2024-10-01"
type: "forum"
theme: "arb"
lang: "en"
---

# Polymarket vs. Betfair Arbitrage: Cross-Platform Prediction Market Strategies

**Primary URLs:**
- https://arbusers.com/polymarket-vs-betfair-arbitrage-t11041/
- https://arbcalculator.cc/ — Polymarket Arbitrage Calculator (Kalshi, Betfair, Smarkets)
- https://betmetricslab.com/arbitrage-betting/prediction-market-arbitrage/
- https://dune.com/the_liolik/99c — Polymarket/Kalshi arbitrage scanner on Dune

---

## Why Arbs Persist Between Polymarket and Betfair

Documented forum discussion: some arbitrage opportunities between Polymarket and Betfair lasted **weeks and months** — not seconds or hours. This surprised even experienced derivatives traders.

**Explanatory factors:**

1. **Differing resolution terms:** e.g., event cancellation: Polymarket settles as "Other," Betfair voids the market → arb breaks down at resolution

2. **Credit risk:** Polymarket is crypto/USDC (counterparty = smart contract); Betfair is GBP/fiat (regulated UK exchange) — different counterparty risk profiles

3. **Transaction costs:** Crypto gas + Betfair 2-5% commission on winnings significantly erodes margins

4. **Operational risk:** Crypto wallet management, private key risk, exchange account risk

5. **Legal/regulatory risk:** Polymarket is not legal in all jurisdictions where Betfair is. Cross-platform arb may violate T&Cs.

6. **FX risk:** Polymarket is USD; Betfair is multi-currency. EUR/GBP traders face currency exposure.

7. **Liquidity mismatch:** Betfair market may have £10K liquidity; Polymarket $100K — different fill sizes achievable

---

## Why These Arbs Don't Close Instantly

The risk premium from items 1-7 above means "true risk-free" arbitrage doesn't exist between platforms. What looks like riskless profit actually carries:
- Resolution risk (2 different oracles may disagree)
- Capital lock-up risk (3-14 days for Polymarket UMA disputes)
- Operational execution risk

Rational arbitrageurs require 3-8% gross spread (after fees) to justify cross-platform positions, vs. <1% for pure on-chain binary arb.

---

## Cross-Platform Arbitrage Tools

**Calculators:**
- https://arbcalculator.cc/ — Supports Polymarket, Kalshi, Betfair, Smarkets, PredictIt; accounts for all fees
- https://www.tradetheoutcome.com/polymarket-arbitrage-calculator/ — Polymarket-specific

**Scanners:**
- https://dune.com/the_liolik/99c — On-chain Polymarket/Kalshi arbitrage scanner
- ArbBets (https://polymark.et/product/arbbets) — AI-driven platform: arbitrage finder + positive EV locator
- Polytrage (https://polymark.et/product/polytrage) — Real-time Telegram alert service

---

## Key Stats on Cross-Platform Arbitrage

- Typical discrepancy range: **1%-2.5%** between Polymarket and Kalshi
- After fees (Kalshi ~2% fee, Polymarket taker fee + gas): margins are thin
- Speed requirement: sub-second execution to beat other arbitrageurs
- Capital on multiple platforms simultaneously required — capital inefficient

---

## The 2024 Government Shutdown Case Study

Documented failure of cross-platform arbitrage:
- Traders bought YES on Polymarket ("shutdown occurred") and NO on Kalshi
- Polymarket resolved YES (OPM issued shutdown announcement)
- Kalshi resolved NO (required >24 hours of actual shutdown operations)
- Both legs lost money despite the "guaranteed" cross-platform position
- **Lesson:** Always verify resolution criteria before cross-platform arb
