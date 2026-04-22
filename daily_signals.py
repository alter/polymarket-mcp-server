#!/usr/bin/env python3
"""
Daily trade signal generator for Polymarket.
Combines multiple edges:
  1. Multi-outcome arbitrage (buy all outcomes when sum < 1.0)
  2. Category bias exploitation (FDV, token launch patterns)
  3. Whale flow following (smart money tracking)
  4. Price dislocation detection (mispriced markets)

Run daily to get actionable signals. Outputs to bot-data/daily_signals.json
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
DATA_API = "https://data-api.polymarket.com"
OUT_DIR = "bot-data"

client = httpx.Client(timeout=20)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Utility functions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_price(token_id):
    """Get bid/ask/mid for a token."""
    try:
        r1 = client.get(f"{CLOB}/price", params={"token_id": token_id, "side": "buy"})
        r2 = client.get(f"{CLOB}/price", params={"token_id": token_id, "side": "sell"})
        bid = float(r1.json().get("price", 0)) if r1.status_code == 200 else 0
        ask = float(r2.json().get("price", 0)) if r2.status_code == 200 else 0
        mid = (bid + ask) / 2 if bid > 0 and ask > 0 else (bid or ask)
        spread = ask - bid if bid > 0 and ask > 0 else None
        return {"bid": bid, "ask": ask, "mid": mid, "spread": spread}
    except Exception:
        return {"bid": 0, "ask": 0, "mid": 0, "spread": None}


def parse_clob_ids(raw):
    """Parse clobTokenIds which can be string or list."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return []
    return []


def fetch_markets(active=True, limit=500):
    """Fetch markets with pagination."""
    markets = []
    for offset in range(0, limit, 100):
        params = {
            "active": str(active).lower(),
            "closed": str(not active).lower(),
            "limit": 100, "offset": offset,
            "order": "volume24hr", "ascending": "false",
        }
        resp = client.get(f"{GAMMA}/markets", params=params)
        if resp.status_code != 200:
            break
        batch = resp.json()
        if not batch:
            break
        markets.extend(batch)
        time.sleep(0.2)
    return markets


def fetch_events(limit=200):
    """Fetch events."""
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Strategy 1: Multi-outcome arbitrage
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXCLUSIVE_KEYWORDS = ["winner", "presidential", "election", "nomination",
                       "champion", "cup winner", "who will", "next prime minister",
                       "first place", "prize"]
NON_EXCLUSIVE_PATTERNS = [
    r"above.*\$\d", r"below.*\$\d", r"hit.*\$\d", r"reach.*\$\d", r"dip.*\$\d",
    r"over/under", r"o/u\s*\d", r"total\s*(kills|goals|points|games)",
    r"ceasefire by", r"conflict ends by", r"announces.*by", r"no longer.*by",
    r"fdv above", r"set \d", r"match o/u", r"handicap", r"first blood",
]


def is_exclusive(title, questions):
    """Check if event markets are mutually exclusive."""
    t = title.lower()
    if any(w in t for w in EXCLUSIVE_KEYWORDS):
        return True
    for q in questions[:10]:
        ql = q.lower()
        for pat in NON_EXCLUSIVE_PATTERNS:
            if re.search(pat, ql):
                return False
    will_count = sum(1 for q in questions if re.match(r"^will .*(win|be)", q.lower()))
    return will_count > len(questions) * 0.7


def scan_arb(events):
    """Find multi-outcome arbitrage opportunities."""
    print("  Scanning arbitrage opportunities...")
    signals = []

    for e in events:
        title = e.get("title", "")
        markets = e.get("markets", [])
        if len(markets) < 3:
            continue

        questions = [m.get("question", "") for m in markets]
        if not is_exclusive(title, questions):
            continue

        # Get prices for all markets
        total_ask = 0
        total_bid = 0
        all_priced = True
        market_details = []

        for m in markets:
            clob_ids = parse_clob_ids(m.get("clobTokenIds", "[]"))
            if not clob_ids:
                all_priced = False
                break

            p = get_price(clob_ids[0])
            if p["ask"] <= 0:
                # Use outcomePrices as fallback
                op = m.get("outcomePrices", "")
                if isinstance(op, str):
                    try:
                        op = json.loads(op)
                    except Exception:
                        op = []
                if op:
                    mid = float(op[0])
                    p = {"bid": mid, "ask": mid, "mid": mid, "spread": 0}
                else:
                    all_priced = False
                    break

            total_ask += p["ask"]
            total_bid += p["bid"]
            market_details.append({
                "question": m.get("question", "")[:60],
                "bid": p["bid"], "ask": p["ask"],
            })
            time.sleep(0.08)

        if not all_priced:
            continue

        # UNDERROUND: buy all YES tokens
        if total_ask < 0.97:  # At least 3% margin
            profit = 1.0 - total_ask
            signals.append({
                "strategy": "arb_buy_all",
                "event": title,
                "n_markets": len(markets),
                "total_ask": round(total_ask, 4),
                "profit_per_dollar": round(profit, 4),
                "roi_pct": round(profit * 100, 1),
                "confidence": "high",
                "action": f"Buy YES on all {len(markets)} outcomes",
                "details": market_details[:10],
            })

        # OVERROUND: sell all YES tokens (= buy all NO tokens)
        if total_bid > 1.03:
            profit = total_bid - 1.0
            signals.append({
                "strategy": "arb_sell_all",
                "event": title,
                "n_markets": len(markets),
                "total_bid": round(total_bid, 4),
                "profit_per_dollar": round(profit, 4),
                "roi_pct": round(profit * 100, 1),
                "confidence": "high",
                "action": f"Sell YES (buy NO) on all {len(markets)} outcomes",
                "details": market_details[:10],
            })

    print(f"    Found {len(signals)} arb signals")
    return signals


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Strategy 2: Category bias exploitation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BIAS_CATEGORIES = [
    # (name, pattern, yes_rate, min_sample, direction_to_buy)
    ("fdv_above", r"(fdv|fully diluted).*(above|reach|over)", 0.235, 277, "NO"),
    ("token_launch_by", r"launch.*(token|coin).*by", 0.769, 50, "YES"),
    ("auction_above", r"auction.*(above|clearing)", 0.143, 14, "NO"),
]


def scan_bias(markets):
    """Find markets with category-based bias edge."""
    print("  Scanning category bias signals...")
    signals = []

    for m in markets:
        q = m.get("question", "")
        q_lower = q.lower()
        vol24h = float(m.get("volume24hr", 0) or 0)
        if vol24h < 1000:
            continue

        clob_ids = parse_clob_ids(m.get("clobTokenIds", "[]"))
        if len(clob_ids) < 2:
            continue

        for cat_name, pattern, yes_rate, min_sample, buy_direction in BIAS_CATEGORIES:
            if not re.search(pattern, q_lower):
                continue

            # Determine token to buy
            if buy_direction == "YES":
                token_id = clob_ids[0]
                win_prob = yes_rate
            else:
                token_id = clob_ids[1]
                win_prob = 1.0 - yes_rate  # P(NO winning)

            p = get_price(token_id)
            if p["ask"] <= 0 or p["ask"] >= 1.0:
                continue

            entry = p["ask"]
            ev = win_prob * (1.0 - entry) - (1.0 - win_prob) * entry

            if ev < 0.05:
                continue

            # Size based on EV and sample quality
            confidence = "high" if min_sample > 100 else "medium" if min_sample > 30 else "low"
            size = 50 if ev > 0.30 else 30 if ev > 0.15 else 20 if ev > 0.08 else 10
            if confidence == "low":
                size = min(size, 20)

            signals.append({
                "strategy": "category_bias",
                "category": cat_name,
                "question": q,
                "slug": m.get("slug", ""),
                "direction": buy_direction,
                "win_prob": round(win_prob, 3),
                "entry_price": round(entry, 4),
                "ev": round(ev, 4),
                "ev_pct": round(ev * 100, 1),
                "size_usd": size,
                "spread": p["spread"],
                "confidence": confidence,
                "historical_sample": min_sample,
                "volume_24h": vol24h,
                "action": f"Buy {buy_direction} at ${entry:.3f} (bias={win_prob:.0%}, EV={ev:.1%})",
            })
            time.sleep(0.1)
            break  # Only first matching category

    print(f"    Found {len(signals)} bias signals")
    return signals


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Strategy 3: Whale flow detection (recent large trades)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def scan_whale_flow(markets):
    """Detect large recent trades that might signal informed activity."""
    print("  Scanning whale flow signals...")
    signals = []

    # Get top 50 markets by volume
    top_markets = sorted(markets, key=lambda m: -float(m.get("volume24hr", 0) or 0))[:50]

    for m in top_markets:
        mid = m.get("id", "")
        if not mid:
            continue

        # Fetch recent trades
        resp = client.get(f"{DATA_API}/trades", params={
            "market": mid, "limit": 50,
        })
        if resp.status_code != 200:
            continue

        trades = resp.json()
        if not trades:
            continue

        # Analyze trade flow
        buy_vol = 0
        sell_vol = 0
        large_buys = 0
        large_sells = 0

        for t in trades:
            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            volume = size * price
            side = t.get("side", "")

            if side == "BUY":
                buy_vol += volume
                if volume > 500:
                    large_buys += 1
            elif side == "SELL":
                sell_vol += volume
                if volume > 500:
                    large_sells += 1

        total_vol = buy_vol + sell_vol
        if total_vol < 1000:
            continue

        # Net flow signal
        net_flow = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0
        large_net = large_buys - large_sells

        # Only signal if strong directional flow
        if abs(net_flow) < 0.3 and abs(large_net) < 2:
            continue

        clob_ids = parse_clob_ids(m.get("clobTokenIds", "[]"))
        if len(clob_ids) < 2:
            continue

        # Get current price
        p = get_price(clob_ids[0])
        if p["mid"] <= 0:
            continue

        direction = "YES" if net_flow > 0 else "NO"

        signals.append({
            "strategy": "whale_flow",
            "question": m.get("question", ""),
            "slug": m.get("slug", ""),
            "direction": direction,
            "net_flow": round(net_flow, 3),
            "buy_volume": round(buy_vol, 2),
            "sell_volume": round(sell_vol, 2),
            "large_buys": large_buys,
            "large_sells": large_sells,
            "current_mid": round(p["mid"], 4),
            "spread": p["spread"],
            "confidence": "medium" if abs(net_flow) > 0.5 else "low",
            "volume_24h": float(m.get("volume24hr", 0) or 0),
            "action": f"{'Buy' if direction == 'YES' else 'Sell'} — whale flow {net_flow:+.0%} "
                      f"({large_buys} large buys, {large_sells} large sells)",
        })
        time.sleep(0.2)

    print(f"    Found {len(signals)} whale flow signals")
    return signals


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Strategy 4: Price dislocation (extreme prices that seem wrong)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def scan_dislocations(markets):
    """Find markets where price seems disconnected from reality."""
    print("  Scanning price dislocations...")
    signals = []

    for m in markets:
        q = m.get("question", "")
        q_lower = q.lower()
        vol24h = float(m.get("volume24hr", 0) or 0)
        if vol24h < 5000:
            continue

        clob_ids = parse_clob_ids(m.get("clobTokenIds", "[]"))
        if len(clob_ids) < 2:
            continue

        p = get_price(clob_ids[0])
        if p["mid"] <= 0:
            continue

        # Look for markets that have already expired/resolved but still have
        # prices far from 0 or 1 (stale markets)
        end_date = m.get("endDate", "")
        if end_date:
            try:
                end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                if end_dt < now and 0.10 < p["mid"] < 0.90:
                    signals.append({
                        "strategy": "stale_market",
                        "question": q,
                        "slug": m.get("slug", ""),
                        "end_date": end_date,
                        "current_mid": round(p["mid"], 4),
                        "confidence": "low",
                        "action": f"Possible stale market — ended {end_date}, price still {p['mid']:.2f}",
                    })
            except Exception:
                pass

        # Markets where YES+NO doesn't sum to ~1.0 (single market mispricing)
        no_price = get_price(clob_ids[1]) if len(clob_ids) > 1 else None
        if no_price and no_price["mid"] > 0:
            total = p["mid"] + no_price["mid"]
            if abs(total - 1.0) > 0.05:
                signals.append({
                    "strategy": "yes_no_dislocation",
                    "question": q,
                    "slug": m.get("slug", ""),
                    "yes_mid": round(p["mid"], 4),
                    "no_mid": round(no_price["mid"], 4),
                    "sum": round(total, 4),
                    "gap": round(abs(total - 1.0), 4),
                    "confidence": "medium",
                    "action": f"YES({p['mid']:.3f}) + NO({no_price['mid']:.3f}) = {total:.3f} ≠ 1.0",
                })
        time.sleep(0.1)

    print(f"    Found {len(signals)} dislocation signals")
    return signals


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    t0 = time.time()
    now = datetime.now(timezone.utc)
    print(f"Daily Signal Generator — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 80)

    # Fetch data
    print("\nFetching active markets...")
    markets = fetch_markets(active=True, limit=500)
    print(f"  {len(markets)} active markets")

    print("Fetching events...")
    events = fetch_events(limit=200)
    print(f"  {len(events)} events")

    # Run all strategies
    print("\nRunning strategies...")
    all_signals = []

    arb_signals = scan_arb(events)
    all_signals.extend(arb_signals)

    bias_signals = scan_bias(markets)
    all_signals.extend(bias_signals)

    whale_signals = scan_whale_flow(markets)
    all_signals.extend(whale_signals)

    dislocation_signals = scan_dislocations(markets[:100])
    all_signals.extend(dislocation_signals)

    # Print summary
    elapsed = time.time() - t0
    print(f"\n{'=' * 80}")
    print(f"DAILY SIGNALS SUMMARY — {now.strftime('%Y-%m-%d')}")
    print(f"{'=' * 80}")

    by_strategy = defaultdict(list)
    for s in all_signals:
        by_strategy[s["strategy"]].append(s)

    for strat, sigs in sorted(by_strategy.items()):
        print(f"\n{'─' * 40}")
        print(f"  {strat.upper()} ({len(sigs)} signals)")
        print(f"{'─' * 40}")
        for s in sigs[:10]:
            action = s.get("action", "")
            conf = s.get("confidence", "?")
            q = s.get("question", s.get("event", ""))[:55]
            ev = s.get("ev_pct", s.get("roi_pct", ""))
            if ev:
                print(f"  [{conf:>6}] EV={ev}% | {q}")
            else:
                print(f"  [{conf:>6}] {q}")
            print(f"           → {action[:80]}")

    # High confidence summary
    high_conf = [s for s in all_signals if s.get("confidence") == "high"]
    med_conf = [s for s in all_signals if s.get("confidence") == "medium"]
    print(f"\n{'=' * 80}")
    print(f"CONFIDENCE BREAKDOWN:")
    print(f"  High: {len(high_conf)} signals")
    print(f"  Medium: {len(med_conf)} signals")
    print(f"  Low: {len(all_signals) - len(high_conf) - len(med_conf)} signals")

    # Total expected value
    total_ev = 0
    total_capital = 0
    for s in all_signals:
        if "size_usd" in s:
            total_capital += s["size_usd"]
            total_ev += s.get("ev", 0) * s["size_usd"]
        elif "profit_per_dollar" in s:
            cost = s.get("n_markets", 10) * 10
            total_capital += cost
            total_ev += s["profit_per_dollar"] * 10

    if total_capital > 0:
        print(f"\nESTIMATED PORTFOLIO:")
        print(f"  Total capital: ${total_capital:,.0f}")
        print(f"  Expected profit: ${total_ev:,.2f}")
        print(f"  Expected ROI: {total_ev/total_capital*100:.1f}%")

    print(f"\nGenerated in {elapsed:.0f}s")

    # Save
    os.makedirs(OUT_DIR, exist_ok=True)
    output = {
        "generated_at": now.isoformat(),
        "markets_scanned": len(markets),
        "events_scanned": len(events),
        "total_signals": len(all_signals),
        "by_strategy": {k: len(v) for k, v in by_strategy.items()},
        "signals": all_signals,
    }
    path = os.path.join(OUT_DIR, "daily_signals.json")
    with open(path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Saved to {path}")

    client.close()


if __name__ == "__main__":
    main()
