---
title: "Polymarket CLOB: Spread Capture and Order Book Strategy"
url: "https://docs.polymarket.com/concepts/prices-orderbook"
source: "docs.polymarket.com"
date: "2026-01-01"
type: "documentation"
theme: "arb"
lang: "en"
---

# Polymarket CLOB: Spread Capture and Order Book Strategy

**Primary URLs:**
- https://docs.polymarket.com/concepts/prices-orderbook
- https://www.alphascope.app/blog/polymarket-order-book-explained
- https://www.quantvps.com/blog/polymarket-clob-central-limit-order-book
- https://github.com/Polymarket/py-clob-client

---

## CLOB Architecture

Polymarket uses a **hybrid-decentralized Central Limit Order Book (CLOB)**:
- Orders matched **off-chain** for speed
- Settlement and execution happen **on-chain** (Polygon)
- Full API access for automated market making
- Unlike AMM-based prediction markets — exact limit order placement at specific prices

---

## Order Book Mechanics

**Bids:** Buy orders, highest to lowest
**Asks:** Sell orders, lowest to highest
**Spread:** Gap between best bid and best ask

**Unified order book design:** Every BUY order for Outcome 1 at price X is simultaneously visible as a SELL order for Outcome 2 at price (100¢ − X). This creates deeper liquidity than separate order books.

**Example:**
- Best bid for YES: $0.34
- Best ask for YES: $0.40
- Displayed midpoint: $0.37
- Market order to buy: pays $0.40 (ask)
- Market order to sell: receives $0.34 (bid)
- Spread: $0.06 — what you lose as taker

---

## Spread Capture Strategy

Act as market maker: place limit orders on both sides near midpoint, profit from bid-ask spread when both sides fill.

**Example:**
- Current spread: 0.48 bid / 0.52 ask
- Place limit buy at 0.49, limit sell at 0.51
- If both fill: earn 0.02 per share (vs. 0.04 for taker crossing full spread)

**Fee advantage:** Makers pay ZERO fees. Only takers pay. Spread capture is before fees — 100% of spread goes to maker.

---

## Key Tactical Details

### Post-Only Orders
Orders can be flagged to reject if they would immediately match (prevents accidentally becoming taker). Essential for fee avoidance.

### Batch Requests
- Up to 15 orders per batch
- Up to 500 tokens per query
- Critical for multi-market market makers

### WebSocket vs REST
- **WebSocket:** ~100ms latency — required for real-time spread capture
- **REST polling:** ~1 second lag — inadequate for competitive quoting

### py-clob-client
Official Python SDK: https://github.com/Polymarket/py-clob-client
- Programmatic order book reading
- Midpoint calculation
- Limit order placement
- Market making automation

---

## Fee-Adjusted Spread Capture (Real P&L)

For a market maker earning the spread:
- **Revenue:** Spread captured when both sides fill
- **Cost (makers):** Zero trading fees + gas (Polygon: typically $0.001-0.01 per order)
- **Revenue from rebates:** 20-25% of taker fees paid by counterparties
- **Revenue from liquidity rewards:** Daily PUSD for resting orders near midpoint

**Net economics:** Competitive with or better than traditional market making venues.

---

## Risks Specific to Prediction Market CLOB

1. **News shock:** Binary outcomes can jump 0.55 → 0.02 in seconds. Stale quotes get adversely selected.
2. **Resolution risk:** Near-expiry markets have shrinking depth (documented: 0.55 log-log slope decay).
3. **Oracle dispute:** Capital lock-up 3-14 days if UMA dispute triggered.
4. **Informed flow:** On event-driven markets, counterparty may have better information than you.

---

## Historical Evolution

- **Early Polymarket:** AMM-based (Gnosis CTF), pool LP model
- **CLOB v1:** Switched to order book for more efficient price discovery
- **CLOB v2 (April 28, 2026):** New pUSD collateral, rewritten backend, $1M launch rewards
