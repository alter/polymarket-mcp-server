---
title: "Negative Risk Markets: Polymarket Technical Documentation"
url: "https://docs.polymarket.com/developers/neg-risk/overview"
source: "docs.polymarket.com"
date: "2026-01-01"
type: "documentation"
theme: "arb"
lang: "en"
---

# Negative Risk Markets: Polymarket Technical Documentation

**Primary URL:** https://docs.polymarket.com/developers/neg-risk/overview
**Also:** https://startpolymarket.com/learn/converting-negative-risk/
**Source code:** https://github.com/Polymarket/neg-risk-ctf-adapter

---

## What Are NegRisk Markets?

NegRisk markets = collections of mutually exclusive outcomes where only one can resolve YES.

Examples:
- Presidential election: exactly one candidate wins
- Super Bowl: exactly one team wins
- Award: exactly one nominee wins

**Mathematical constraint:** Σ(all YES prices) must equal approximately $1.00

---

## The Convert Function

The core mechanism enabling capital efficiency:

> "A No share in any market can be converted into 1 Yes share in every other market."

**How it works:**
1. Hold 1 NO token for any outcome
2. Invoke convert function via NegRiskAdapter contract
3. Receive 1 YES token for each remaining outcome in the event

This operation is atomic — no intermediary settlement. No conversion fees, only blockchain gas costs.

**API parameter:** `negRisk: true` in order options

---

## Critical Distinction: Capital Efficiency vs. True Arbitrage

**Common misconception:** NegRisk convert creates arbitrage profit.

**Reality:** 
- Convert costs $1.00 (1 NO token) and returns tokens totaling $1.00 value
- No guaranteed profit from convert alone
- **True arbitrage only via standard market interface** when prices are mispriced

**Correct arbitrage logic:**
- If Σ(YES prices) = $0.98 via standard market: buy all YES for $0.98, payout = $1.00, profit = $0.02
- NegRisk approach for same: costs $1.00, pays $1.00 — zero profit

---

## Augmented Negative Risk (Advanced)

Handles scenarios where new outcomes emerge post-launch:

**Types:**
- **Named outcomes:** Predetermined possibilities
- **Placeholder outcomes:** Reserved slots clarified later
- **Explicit Other:** Catches unspecified outcomes

**Critical constraint:** Only trade on named outcomes. Placeholder outcomes must be ignored until named or until resolution occurs.

When placeholders are assigned, the "Other" definition narrows accordingly. This creates temporary mispricing opportunities during transitions.

---

## Developer Integration

**Gamma API:** Both events and market objects have `negRisk` boolean field
**Order placement:** Must include `negRisk: true` parameter

**Contract addresses:** Available in Polymarket Contracts documentation

---

## Arbitrage via NegRisk Markets (Practical)

The most profitable approach documented in IMDEA research ($28.99M over 12 months):

**When Σ(YES prices) < $1.00:**
- Buy all YES tokens via standard market order
- Cost < $1.00, guaranteed payout = $1.00 (one winner)
- Profit = $1.00 - total cost - fees

**When Σ(YES prices) > $1.00:**
- Buy all NO tokens via standard market
- One NO token pays $1.00, others pay $0 — but complementary positions created via convert
- Complex but mechanically sound

**Speed requirements:** These opportunities close quickly in liquid markets. Automation essential.

---

## Go Implementation

**Example scanner:** https://pkg.go.dev/github.com/ivanzzeth/polymarket-go-gamma-client/examples/find-negrisk-opportunities

Scans all NegRisk market groups and computes current Σ(YES prices) to detect deviations from $1.00.
