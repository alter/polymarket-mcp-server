#!/usr/bin/env python3
"""
Bias Scanner v2 — Category-aware NO bias exploitation.

Key insight from 2,100 resolved markets:
  - Overall: 75.1% of markets resolve NO
  - $1M+ volume: 64.6% resolve NO
  - Politics/elections: 95% resolve NO
  - FDV markets: 76.5% resolve NO
  - Sports "will win": 100% resolve NO (longshots)

Strategy: Buy NO tokens on markets where the NO probability is
higher than what the current price implies.

Edge = (historical_NO_rate - implied_NO_rate) where implied_NO_rate = 1 - YES_price
"""

import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
OUT_DIR = "bot-data"

# Historical NO rates by category (from 2100 resolved markets)
CATEGORY_NO_RATES = {
    "politics":     (0.950, 323),   # "will win election/nomination"
    "sports":       (0.970, 29),    # "will win cup/tournament" (longshots)
    "fdv_above":    (0.765, 353),   # "FDV above $X"
    "fed_rate":     (0.903, 31),    # "Fed rate" related
    "auction":      (0.867, 15),    # "auction price above"
    "will_generic": (0.773, 1000),  # "Will X..." generic
    "other":        (0.571, 231),   # everything else
}

# Min absolute edge (NO_rate - implied_NO) to signal
MIN_EDGE = 0.05
# Min volume to consider
MIN_VOLUME = 5000
# Max spread to consider tradeable
MAX_SPREAD = 0.05


def classify(question):
    """Classify market into bias category."""
    q = question.lower()
    if re.search(r"(fdv|fully diluted).*(above|reach|hit|over)", q):
        return "fdv_above"
    if re.search(r"(auction|clearing).*(above|price)", q):
        return "auction"
    if re.search(r"(fed\b|interest rate|bps|basis point)", q):
        return "fed_rate"
    if re.search(r"will .*(win|elected|nomination|president|governor|mayor|prime minister|senator|congress)", q):
        return "politics"
    if re.search(r"will .*(win|beat).*\b(game|match|cup|finals|championship|tournament|series|round|league)\b", q):
        return "sports"
    if re.match(r"^will\s", q):
        return "will_generic"
    return "other"


def get_price(client, token_id):
    """Get bid/ask for a token."""
    try:
        r1 = client.get(f"{CLOB}/price", params={"token_id": token_id, "side": "buy"})
        r2 = client.get(f"{CLOB}/price", params={"token_id": token_id, "side": "sell"})
        bid = float(r1.json().get("price", 0)) if r1.status_code == 200 else 0
        ask = float(r2.json().get("price", 0)) if r2.status_code == 200 else 0
        return bid, ask
    except Exception:
        return 0, 0


def main():
    client = httpx.Client(timeout=20)
    now = datetime.now(timezone.utc)
    print(f"Bias Scanner v2 — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 100)

    # Fetch active markets
    print("Fetching active markets...")
    markets = []
    for offset in range(0, 600, 100):
        resp = client.get(f"{GAMMA}/markets", params={
            "active": "true", "closed": "false",
            "limit": 100, "offset": offset,
            "order": "volume24hr", "ascending": "false",
        })
        if resp.status_code != 200:
            break
        batch = resp.json()
        if not batch:
            break
        markets.extend(batch)
        time.sleep(0.2)
    print(f"  {len(markets)} active markets")

    # Classify and score
    signals = []
    category_counts = defaultdict(int)

    for m in markets:
        question = m.get("question", "")
        vol24h = float(m.get("volume24hr", 0) or 0)
        if vol24h < MIN_VOLUME:
            continue

        clob_ids = m.get("clobTokenIds", "[]")
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                continue
        if len(clob_ids) < 2:
            continue

        category = classify(question)
        category_counts[category] += 1

        no_rate, sample_n = CATEGORY_NO_RATES.get(category, (0.571, 100))

        # Get YES token price to calculate implied probability
        yes_bid, yes_ask = get_price(client, clob_ids[0])
        if yes_bid <= 0 and yes_ask <= 0:
            continue

        yes_mid = (yes_bid + yes_ask) / 2 if yes_bid > 0 and yes_ask > 0 else (yes_bid or yes_ask)
        yes_spread = yes_ask - yes_bid if yes_bid > 0 and yes_ask > 0 else None

        # Get NO token price for entry
        no_bid, no_ask = get_price(client, clob_ids[1])
        if no_ask <= 0 or no_ask >= 1.0:
            continue

        no_spread = no_ask - no_bid if no_bid > 0 and no_ask > 0 else None

        # Skip wide spreads
        if no_spread and no_spread > MAX_SPREAD:
            continue

        # Calculate edge
        # We buy NO at no_ask price
        # Expected payout: no_rate * $1 + (1-no_rate) * $0 = no_rate
        # Cost: no_ask
        # EV per dollar = no_rate - no_ask
        # EV per trade = (no_rate * (1 - no_ask) - (1 - no_rate) * no_ask) * size
        # Simplified: EV/unit = no_rate - no_ask
        edge = no_rate - no_ask
        ev_per_trade = no_rate * (1.0 - no_ask) - (1.0 - no_rate) * no_ask

        if edge < MIN_EDGE:
            continue

        # Position sizing: based on edge and confidence
        confidence = "high" if sample_n >= 100 and edge > 0.15 else \
                     "medium" if sample_n >= 30 and edge > 0.08 else "low"
        size = 50 if edge > 0.30 else 30 if edge > 0.15 else 20 if edge > 0.08 else 10
        if confidence == "low":
            size = min(size, 15)

        signals.append({
            "question": question,
            "slug": m.get("slug", ""),
            "category": category,
            "no_rate": round(no_rate, 3),
            "sample_n": sample_n,
            "yes_mid": round(yes_mid, 4),
            "no_ask": round(no_ask, 4),
            "no_bid": round(no_bid, 4),
            "no_spread": round(no_spread, 4) if no_spread else None,
            "edge": round(edge, 4),
            "ev_per_unit": round(ev_per_trade, 4),
            "confidence": confidence,
            "size_usd": size,
            "volume_24h": round(vol24h, 2),
            "token_id_no": clob_ids[1],
        })
        time.sleep(0.1)

    signals.sort(key=lambda s: -s["edge"])

    # Print results
    print(f"\n{'#':>3} {'Cat':<15} {'NO%':>5} {'NO ask':>7} {'Edge':>6} {'EV/u':>6} {'Sprd':>6} {'Conf':>6} {'$':>4} {'Vol24h':>10}  Question")
    print("-" * 130)
    for i, s in enumerate(signals, 1):
        q = s["question"][:50]
        sprd = f"{s['no_spread']:.3f}" if s["no_spread"] else "  n/a"
        print(
            f"{i:>3} {s['category']:<15} {s['no_rate']:>4.0%} "
            f"${s['no_ask']:>5.3f} {s['edge']:>5.1%} "
            f"{s['ev_per_unit']:>+5.3f} {sprd:>6} "
            f"{s['confidence']:>6} ${s['size_usd']:<3} "
            f"{s['volume_24h']:>10,.0f}  {q}"
        )

    print(f"\n{'=' * 80}")
    print(f"SUMMARY")
    print(f"  Total signals: {len(signals)}")
    print(f"  Categories scanned: {dict(category_counts)}")

    # Confidence breakdown
    for conf in ["high", "medium", "low"]:
        subset = [s for s in signals if s["confidence"] == conf]
        if subset:
            total_size = sum(s["size_usd"] for s in subset)
            avg_edge = sum(s["edge"] for s in subset) / len(subset)
            avg_ev = sum(s["ev_per_unit"] * s["size_usd"] for s in subset) / total_size
            print(f"\n  [{conf}] {len(subset)} signals, ${total_size} capital")
            print(f"    Avg edge: {avg_edge:.1%}, Weighted EV/unit: {avg_ev:+.3f}")
            exp_profit = sum(s["ev_per_unit"] * s["size_usd"] for s in subset)
            print(f"    Expected profit: ${exp_profit:+.2f}")

    # Overall
    if signals:
        total_capital = sum(s["size_usd"] for s in signals)
        total_ev = sum(s["ev_per_unit"] * s["size_usd"] for s in signals)
        print(f"\n  PORTFOLIO TOTAL:")
        print(f"    Capital: ${total_capital:,}")
        print(f"    Expected profit: ${total_ev:+,.2f}")
        print(f"    Expected ROI: {total_ev/total_capital*100:+.1f}%")

    # Save
    os.makedirs(OUT_DIR, exist_ok=True)
    output = {
        "generated_at": now.isoformat(),
        "model": "NO bias exploitation v2",
        "base_rates": CATEGORY_NO_RATES,
        "markets_scanned": len(markets),
        "signals_count": len(signals),
        "signals": signals,
    }
    path = os.path.join(OUT_DIR, "bias_signals_v2.json")
    with open(path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved to {path}")

    client.close()


if __name__ == "__main__":
    main()
