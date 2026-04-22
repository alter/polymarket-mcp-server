#!/usr/bin/env python3
"""
Category-aware bias scanner for Polymarket.
Scans active markets, classifies by pattern, applies category-specific
bias probabilities, and outputs filtered trade signals.

Based on analysis of 500+ resolved markets:
  - FDV above $X:        23.5% YES (n=277) → strong NO signal
  - Will X (generic):    76.9% YES (n=104) → strong YES signal
  - Crypto reach/dip:    100% YES  (n=28)  → BUT sample bias (bull market)
  - Sports will win:     varies by context
  - Geopolitics by date: varies
"""

import json
import os
import re
import time
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
OUT_DIR = "bot-data"
MIN_EV = 0.05
MIN_VOLUME = 5000  # minimum 24h volume to consider

# Category patterns and their historical YES resolution rates
# Order matters: first match wins
CATEGORY_PATTERNS = [
    # FDV markets: almost always NO
    ("fdv_above", r"(fdv|fully diluted).*(above|reach|hit|over)", 0.235, "NO"),
    # Crypto price targets "above" with specific date: depends on target vs current
    ("crypto_above_date", r"(bitcoin|btc|ethereum|eth|solana|sol|xrp|crypto).*(above|below).*\d{4}", None, None),
    # Crypto "reach" targets: historically 100% but unreliable (bull market sample)
    ("crypto_reach", r"(bitcoin|btc|ethereum|eth|solana|sol|xrp).*(reach|hit)\s*\$", None, None),
    # Crypto "dip" targets: historically 100% but unreliable
    ("crypto_dip", r"(bitcoin|btc|ethereum|eth|solana|sol|xrp).*(dip|drop|fall)\s*(to|below)", None, None),
    # Token launch by date: "Will X launch a token by..."
    ("token_launch", r"launch.*(token|coin).*by", 0.769, "YES"),
    # Generic "will" questions (not sports/crypto): 77% YES
    ("will_generic", r"^will\s", 0.769, "YES"),
]

# Additional keyword modifiers applied AFTER category classification
KEYWORD_ADJUSTMENTS = {
    # These only apply within the will_generic category
    "above": -0.20,   # "above" in question reduces YES probability
    "drop": -0.30,    # "drop" strongly reduces YES probability
    "below": -0.15,
}


def fetch_active_markets(client):
    """Fetch active markets, paginating to get more."""
    all_markets = []
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
        all_markets.extend(batch)
        time.sleep(0.2)
    return all_markets


def classify_market(question):
    """Classify market into a category. Returns (category, base_yes_rate, direction) or None."""
    q = question.lower()
    for cat_name, pattern, yes_rate, direction in CATEGORY_PATTERNS:
        if re.search(pattern, q):
            return cat_name, yes_rate, direction
    return None, None, None


def compute_bias(question):
    """
    Compute bias probability for a market question.
    Returns (bias_prob, direction, category, keywords) or (None, None, None, []).
    direction is which side to BUY (YES or NO).
    """
    category, base_rate, direction = classify_market(question)

    if category is None:
        return None, None, None, []

    # Skip categories where we don't have reliable data
    if base_rate is None:
        return None, None, category, []

    # Apply keyword adjustments for generic "will" category
    q_lower = question.lower()
    adjusted_rate = base_rate
    matched_kw = [category]

    if category == "will_generic":
        for kw, adj in KEYWORD_ADJUSTMENTS.items():
            if kw in q_lower:
                adjusted_rate += adj
                matched_kw.append(kw)

    # Clamp to [0.05, 0.95]
    adjusted_rate = max(0.05, min(0.95, adjusted_rate))

    return adjusted_rate, direction, category, matched_kw


def fetch_yes_price(client, token_id):
    """Get bid/ask/mid for a token."""
    try:
        r1 = client.get(f"{CLOB}/price", params={"token_id": token_id, "side": "buy"})
        r2 = client.get(f"{CLOB}/price", params={"token_id": token_id, "side": "sell"})
        bid = float(r1.json().get("price", 0)) if r1.status_code == 200 else 0
        ask = float(r2.json().get("price", 0)) if r2.status_code == 200 else 0
        mid = (bid + ask) / 2 if bid > 0 and ask > 0 else (bid or ask)
        spread = ask - bid if bid > 0 and ask > 0 else 999
        return bid, ask, mid, spread
    except Exception:
        return 0, 0, 0, 999


def calc_ev(win_prob, entry_price):
    """Expected value per dollar. win_prob is prob of the token we're buying paying out."""
    return win_prob * (1.0 - entry_price) - (1.0 - win_prob) * entry_price


def size_from_ev(ev, spread):
    """Position size based on EV and spread quality."""
    if spread > 0.10:
        return 0  # Too wide, skip
    base = 10
    if ev >= 0.30:
        base = 50
    elif ev >= 0.20:
        base = 40
    elif ev >= 0.15:
        base = 30
    elif ev >= 0.10:
        base = 20
    # Reduce size for wide spreads
    if spread > 0.05:
        base = max(10, base // 2)
    return base


def main():
    client = httpx.Client(timeout=20)

    print("Fetching active markets...")
    markets = fetch_active_markets(client)
    print(f"Fetched {len(markets)} active markets\n")

    signals = []
    skipped_categories = {}

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
        if not clob_ids or len(clob_ids) < 2:
            continue

        bias_prob, direction, category, keywords = compute_bias(question)

        if category and bias_prob is None:
            skipped_categories[category] = skipped_categories.get(category, 0) + 1
            continue
        if bias_prob is None:
            continue

        # Determine which token to buy and the win probability for that token
        if direction == "YES":
            token_id = clob_ids[0]  # YES token
            win_prob = bias_prob
        else:
            token_id = clob_ids[1]  # NO token
            win_prob = 1.0 - bias_prob  # P(NO wins)

        bid, ask, mid, spread = fetch_yes_price(client, token_id)
        if mid <= 0 or mid >= 1.0:
            continue

        # Use ask price for buying (realistic execution)
        entry = ask if ask > 0 else mid
        if entry <= 0 or entry >= 1.0:
            continue

        ev = calc_ev(win_prob, entry)
        if ev < MIN_EV:
            continue

        size = size_from_ev(ev, spread)
        if size == 0:
            continue

        signals.append({
            "question": question,
            "slug": m.get("slug", ""),
            "condition_id": m.get("conditionId", ""),
            "category": category,
            "direction": direction,
            "bias_prob": round(bias_prob, 3),
            "win_prob": round(win_prob, 3),
            "keywords": keywords,
            "token_id": token_id,
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "mid": round(mid, 4),
            "spread": round(spread, 4),
            "entry_price": round(entry, 4),
            "ev": round(ev, 4),
            "size_usd": size,
            "volume_24h": round(vol24h, 2),
            "liquidity": float(m.get("liquidity", 0) or 0),
        })
        time.sleep(0.15)

    signals.sort(key=lambda s: -s["ev"])

    # Print results
    print(f"\n{'#':<3} {'Dir':<4} {'Cat':<15} {'Bias':>5} {'Entry':>6} {'Sprd':>6} {'EV':>7} {'$':>4} {'Vol24h':>10}  Question")
    print("-" * 130)
    for i, s in enumerate(signals, 1):
        q = s["question"][:55]
        print(
            f"{i:<3} {s['direction']:<4} {s['category']:<15} "
            f"{s['bias_prob']:>4.0%} ${s['entry_price']:>5.2f} "
            f"{s['spread']:>5.3f} {s['ev']:>6.1%} "
            f"${s['size_usd']:<3} {s['volume_24h']:>10,.0f}  {q}"
        )

    print(f"\n{len(signals)} actionable signals (min EV={MIN_EV:.0%}, min vol=${MIN_VOLUME:,})")

    if skipped_categories:
        print(f"\nSkipped categories (no reliable bias data):")
        for cat, cnt in sorted(skipped_categories.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {cnt} markets")

    # Summary stats
    if signals:
        total_capital = sum(s["size_usd"] for s in signals)
        weighted_ev = sum(s["ev"] * s["size_usd"] for s in signals) / total_capital
        print(f"\nPortfolio summary:")
        print(f"  Total signals: {len(signals)}")
        print(f"  Total capital needed: ${total_capital:,}")
        print(f"  Weighted avg EV: {weighted_ev:.1%}")
        print(f"  Expected profit: ${total_capital * weighted_ev:,.2f}")

        # By category
        by_cat = {}
        for s in signals:
            cat = s["category"]
            if cat not in by_cat:
                by_cat[cat] = {"count": 0, "capital": 0, "ev_sum": 0}
            by_cat[cat]["count"] += 1
            by_cat[cat]["capital"] += s["size_usd"]
            by_cat[cat]["ev_sum"] += s["ev"] * s["size_usd"]
        print(f"\n  By category:")
        for cat, stats in sorted(by_cat.items(), key=lambda x: -x[1]["count"]):
            avg_ev = stats["ev_sum"] / stats["capital"] if stats["capital"] else 0
            print(f"    {cat}: {stats['count']} signals, ${stats['capital']} capital, avg EV {avg_ev:.1%}")

    # Save
    os.makedirs(OUT_DIR, exist_ok=True)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "markets_scanned": len(markets),
        "signals_count": len(signals),
        "min_ev": MIN_EV,
        "min_volume": MIN_VOLUME,
        "signals": signals,
    }
    with open(os.path.join(OUT_DIR, "bias_signals.json"), "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved to {OUT_DIR}/bias_signals.json")

    client.close()


if __name__ == "__main__":
    main()
