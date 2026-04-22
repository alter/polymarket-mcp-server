#!/usr/bin/env python3
"""
Multi-outcome arbitrage scanner for Polymarket.
Finds events where mutually exclusive outcomes have prices that don't sum to 1.0.

Two types of arb:
  OVERROUND (sum > 1.0): Sell all outcomes. Guaranteed profit = sum - 1.0 per dollar.
    Execute: sell YES on every outcome (= buy NO on each).
  UNDERROUND (sum < 1.0): Buy all outcomes. Guaranteed profit = 1.0 - sum per dollar.
    Execute: buy YES on every outcome.

Filters out non-mutually-exclusive events (e.g., "Bitcoin above $56K, $58K, $60K").
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

# Events with these patterns are NOT mutually exclusive — skip them
NON_EXCLUSIVE_PATTERNS = [
    r"above.*\$\d",       # "above $X" at different thresholds
    r"below.*\$\d",
    r"hit.*\$\d",         # "hit $X" at different thresholds
    r"reach.*\$\d",
    r"dip.*\$\d",
    r"over/under",        # over/under sports lines
    r"o/u\s*\d",
    r"total\s*(kills|goals|points|games)",
    r"first blood",
    r"ceasefire by",      # "by date X, Y, Z" — NOT exclusive (if by March, also by April)
    r"conflict ends by",
    r"announces.*by",
    r"no longer.*by",
    r"fdv above",
    r"set \d games",      # tennis set games totals
    r"match o/u",
    r"set handicap",
]


def is_mutually_exclusive_event(event_title, sample_questions):
    """Heuristic: check if event markets are mutually exclusive."""
    title = event_title.lower()
    # Winner events are mutually exclusive
    if any(w in title for w in ["winner", "presidential", "election", "nomination",
                                  "best ai model", "cup winner", "finals",
                                  "who will", "next prime minister"]):
        return True

    # Check sample questions for non-exclusive patterns
    for q in sample_questions:
        q_lower = q.lower()
        for pat in NON_EXCLUSIVE_PATTERNS:
            if re.search(pat, q_lower):
                return False

    # If all questions follow "Will X win..." pattern, likely exclusive
    will_win_count = sum(1 for q in sample_questions if re.search(r"will .*(win|be)", q.lower()))
    if will_win_count > len(sample_questions) * 0.8:
        return True

    return False


def fetch_events(client, limit=200):
    """Fetch active events."""
    events = []
    for offset in range(0, limit, 50):
        resp = client.get(f"{GAMMA}/events", params={
            "closed": "false", "limit": 50, "offset": offset,
            "order": "volume24hr", "ascending": "false",
        })
        if resp.status_code != 200:
            break
        batch = resp.json()
        if not batch:
            break
        events.extend(batch)
        time.sleep(0.3)
    return events


def get_market_prices(client, markets):
    """Get bid/ask for YES token of each market. Returns list of dicts."""
    prices = []
    for m in markets:
        clob_ids = m.get("clobTokenIds", "[]")
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                prices.append(None)
                continue
        if not clob_ids:
            prices.append(None)
            continue

        yes_token = clob_ids[0]
        no_token = clob_ids[1] if len(clob_ids) > 1 else None

        try:
            r_bid = client.get(f"{CLOB}/price", params={"token_id": yes_token, "side": "buy"})
            r_ask = client.get(f"{CLOB}/price", params={"token_id": yes_token, "side": "sell"})
            bid = float(r_bid.json().get("price", 0)) if r_bid.status_code == 200 else 0
            ask = float(r_ask.json().get("price", 0)) if r_ask.status_code == 200 else 0
        except Exception:
            bid, ask = 0, 0

        mid = (bid + ask) / 2 if bid > 0 and ask > 0 else (bid or ask)

        # Fallback: use outcomePrices from market data
        if mid <= 0:
            op = m.get("outcomePrices", "")
            if isinstance(op, str):
                try:
                    op = json.loads(op)
                except Exception:
                    op = []
            if op and len(op) >= 1:
                try:
                    mid = float(op[0])
                except Exception:
                    pass

        prices.append({
            "question": m.get("question", ""),
            "slug": m.get("slug", ""),
            "yes_token": yes_token,
            "no_token": no_token,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread": ask - bid if ask > 0 and bid > 0 else None,
            "volume_24h": float(m.get("volume24hr", 0) or 0),
            "liquidity": float(m.get("liquidity", 0) or 0),
        })
        time.sleep(0.1)

    return prices


def analyze_event(event_title, market_prices):
    """Analyze an event for arb opportunity."""
    valid_prices = [p for p in market_prices if p and p["mid"] > 0]
    if len(valid_prices) < 3:
        return None

    total_mid = sum(p["mid"] for p in valid_prices)
    overround = total_mid - 1.0

    if abs(overround) < 0.02:
        return None  # Within noise range

    # For OVERROUND: profit from selling all YES (buying all NO)
    # We need to buy NO at (1-bid) for each market, which costs us ask_no = 1 - bid_yes
    # For UNDERROUND: profit from buying all YES at ask prices
    if overround > 0:
        # SELL ALL YES strategy: buy NO tokens
        # Cost to buy NO = 1 - yes_bid for each market
        # But we get $1 when exactly one resolves NO (all others YES)
        # Actually: we sell YES on each market. To sell YES = buy NO.
        # NO price = 1 - YES price (approximately)
        # Total cost of buying all NOs = sum(1 - yes_bid) = n - sum(yes_bid)
        # Payout: exactly (n-1) NOs pay $1 each = n-1
        # Wait no — in binary markets, selling YES = buying NO token.
        # If we buy NO on every outcome, exactly (n-1) of them pay $1.
        # Cost: sum of NO ask prices = sum(1 - YES bid)
        # Revenue: n-1 (guaranteed, since exactly one outcome wins)
        # Profit = (n-1) - sum(1 - YES_bid) = (n-1) - n + sum(YES_bid) = sum(YES_bid) - 1
        total_bid = sum(p["bid"] for p in valid_prices if p["bid"] > 0)
        executable_n = sum(1 for p in valid_prices if p["bid"] > 0)
        if executable_n < len(valid_prices):
            return None  # Can't price all markets
        profit_per_share = total_bid - 1.0
        strategy = "SELL_ALL_YES"
        direction = "Sell YES (buy NO) on all outcomes"
    else:
        # BUY ALL YES strategy
        # Cost: sum of YES ask prices
        # Revenue: exactly 1 YES pays $1
        # Profit = 1 - sum(YES_ask)
        total_ask = sum(p["ask"] for p in valid_prices if p["ask"] > 0)
        executable_n = sum(1 for p in valid_prices if p["ask"] > 0)
        if executable_n < len(valid_prices):
            return None
        profit_per_share = 1.0 - total_ask
        strategy = "BUY_ALL_YES"
        direction = "Buy YES on all outcomes"

    if profit_per_share <= 0:
        return None  # No profit after bid/ask spread

    # Position sizing: $10 per outcome, profit scales
    size_per_outcome = 10
    total_cost = size_per_outcome * len(valid_prices)
    expected_profit = profit_per_share * size_per_outcome
    roi = profit_per_share  # Profit per $1 deployed per outcome

    return {
        "event": event_title,
        "strategy": strategy,
        "direction": direction,
        "n_markets": len(valid_prices),
        "total_mid_sum": round(total_mid, 4),
        "overround_pct": round(overround * 100, 2),
        "profit_per_share": round(profit_per_share, 4),
        "roi_pct": round(roi * 100, 2),
        "size_per_outcome": size_per_outcome,
        "total_cost": total_cost,
        "expected_profit": round(expected_profit, 2),
        "top_markets": [
            {
                "question": p["question"][:60],
                "bid": p["bid"],
                "ask": p["ask"],
                "mid": p["mid"],
                "spread": p["spread"],
            }
            for p in sorted(valid_prices, key=lambda x: -x["mid"])[:10]
        ],
    }


def main():
    client = httpx.Client(timeout=20)

    print("Fetching events...")
    events = fetch_events(client, limit=200)
    print(f"Fetched {len(events)} events\n")

    multi_events = [e for e in events if len(e.get("markets", [])) >= 3]
    print(f"Events with 3+ markets: {len(multi_events)}")

    arb_opps = []
    skipped = 0

    for e in multi_events:
        title = e.get("title", "")
        markets = e.get("markets", [])
        questions = [m.get("question", "") for m in markets[:10]]

        if not is_mutually_exclusive_event(title, questions):
            skipped += 1
            continue

        print(f"\nChecking: {title[:60]}... ({len(markets)} markets)")
        prices = get_market_prices(client, markets)

        result = analyze_event(title, prices)
        if result:
            arb_opps.append(result)
            print(f"  → {result['strategy']} | overround={result['overround_pct']:+.1f}% | "
                  f"profit/share=${result['profit_per_share']:.3f} | ROI={result['roi_pct']:.1f}%")
        else:
            print(f"  → No arb (sum too close to 1.0 or not executable)")

    arb_opps.sort(key=lambda x: -x["profit_per_share"])

    print("\n" + "=" * 100)
    print("ARBITRAGE OPPORTUNITIES (mutually exclusive events only)")
    print("=" * 100)
    print(f"\n{'#':<3} {'Strategy':<15} {'Markets':>7} {'Sum':>6} {'OR%':>6} {'Profit/$':>9} {'ROI':>6} Event")
    print("-" * 100)
    for i, a in enumerate(arb_opps, 1):
        print(f"{i:<3} {a['strategy']:<15} {a['n_markets']:>7} "
              f"{a['total_mid_sum']:>6.3f} {a['overround_pct']:>+5.1f}% "
              f"${a['profit_per_share']:>7.4f} {a['roi_pct']:>5.1f}% "
              f"{a['event'][:45]}")

    print(f"\nTotal: {len(arb_opps)} arb opportunities")
    print(f"Skipped: {skipped} non-exclusive events")

    if arb_opps:
        total_profit = sum(a["expected_profit"] for a in arb_opps)
        total_cost = sum(a["total_cost"] for a in arb_opps)
        print(f"\nPortfolio ($10/outcome):")
        print(f"  Total cost: ${total_cost:,.0f}")
        print(f"  Expected profit: ${total_profit:,.2f}")
        print(f"  Portfolio ROI: {total_profit/total_cost*100:.1f}%")

    # Save
    os.makedirs(OUT_DIR, exist_ok=True)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "events_scanned": len(multi_events),
        "exclusive_events_checked": len(multi_events) - skipped,
        "opportunities": arb_opps,
    }
    with open(os.path.join(OUT_DIR, "arb_opportunities.json"), "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved to {OUT_DIR}/arb_opportunities.json")

    client.close()


if __name__ == "__main__":
    main()
