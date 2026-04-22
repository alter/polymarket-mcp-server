#!/usr/bin/env python3
"""
Polymarket bias analytics collector.
Collects trade data, resolution outcomes, whale activity, and market patterns.
Saves structured data for bias analysis.
"""

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
OUT_DIR = "bot-data/analytics"
os.makedirs(OUT_DIR, exist_ok=True)

client = httpx.Client(timeout=20)


# ── 1. Collect resolved markets with outcomes ──

def collect_resolved_markets(limit=500):
    """Get recently resolved markets with their outcomes and metadata."""
    print(f"Collecting resolved markets (limit={limit})...")
    markets = []
    offset = 0
    batch = 100

    while offset < limit:
        resp = client.get(f"{GAMMA}/markets", params={
            "closed": "true", "limit": min(batch, limit - offset),
            "offset": offset, "order": "endDate", "ascending": "false",
        })
        if resp.status_code != 200:
            print(f"  Error {resp.status_code} at offset {offset}")
            break
        batch_data = resp.json()
        if not batch_data:
            break
        markets.extend(batch_data)
        offset += len(batch_data)
        print(f"  Fetched {offset} resolved markets...")
        time.sleep(0.3)

    # Extract key fields
    results = []
    for m in markets:
        outcome_prices = m.get("outcomePrices", "")
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except:
                outcome_prices = []

        # Determine YES/NO resolution
        yes_won = None
        if outcome_prices and len(outcome_prices) >= 2:
            try:
                yes_won = float(outcome_prices[0]) > 0.5
            except:
                pass

        results.append({
            "id": m.get("id"),
            "question": m.get("question", ""),
            "slug": m.get("slug", ""),
            "outcome": m.get("outcome"),
            "yes_won": yes_won,
            "volume": float(m.get("volume", 0) or 0),
            "volume_24h": float(m.get("volume24hr", 0) or 0),
            "liquidity": float(m.get("liquidity", 0) or 0),
            "end_date": m.get("endDate", ""),
            "created_at": m.get("createdAt", ""),
            "fee_type": m.get("feeType"),
            "fees_enabled": m.get("feesEnabled", False),
            "tags": m.get("tags", []),
            "outcome_prices": outcome_prices,
        })

    path = os.path.join(OUT_DIR, "resolved_markets.json")
    with open(path, "w") as f:
        json.dump(results, f, indent=1)
    print(f"  Saved {len(results)} resolved markets to {path}")
    return results


# ── 2. Collect trades for markets ──

def collect_trades_for_markets(market_ids, max_per_market=200):
    """Collect recent trades for given market IDs."""
    print(f"Collecting trades for {len(market_ids)} markets...")
    all_trades = []

    for i, mid in enumerate(market_ids):
        resp = client.get(f"{DATA_API}/trades", params={
            "market": mid, "limit": max_per_market
        })
        if resp.status_code == 200:
            trades = resp.json()
            for t in trades:
                t["market_id"] = mid
            all_trades.extend(trades)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(market_ids)} markets, {len(all_trades)} trades")
        time.sleep(0.2)

    path = os.path.join(OUT_DIR, "trades.json")
    with open(path, "w") as f:
        json.dump(all_trades, f)
    print(f"  Saved {len(all_trades)} trades to {path}")
    return all_trades


# ── 3. Collect active market orderbooks for spread analysis ──

def collect_active_orderbooks(limit=100):
    """Get orderbook snapshots for active markets."""
    print(f"Collecting active market orderbooks (limit={limit})...")

    resp = client.get(f"{GAMMA}/markets", params={
        "active": "true", "closed": "false", "limit": limit,
        "order": "volume24hr", "ascending": "false"
    })
    markets = resp.json()

    snapshots = []
    for m in markets:
        tokens = m.get("clobTokenIds", "[]")
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        if not tokens:
            continue

        token_yes = tokens[0]
        try:
            resp_price = client.get(f"{CLOB}/price", params={
                "token_id": token_yes, "side": "buy"
            })
            bid = float(resp_price.json().get("price", 0)) if resp_price.status_code == 200 else 0

            resp_price2 = client.get(f"{CLOB}/price", params={
                "token_id": token_yes, "side": "sell"
            })
            ask = float(resp_price2.json().get("price", 0)) if resp_price2.status_code == 200 else 0
        except:
            bid, ask = 0, 0

        snapshots.append({
            "market_id": m.get("id"),
            "question": m.get("question", ""),
            "bid": bid, "ask": ask,
            "spread": ask - bid if ask > 0 and bid > 0 else None,
            "mid": (ask + bid) / 2 if ask > 0 and bid > 0 else None,
            "volume_24h": float(m.get("volume24hr", 0) or 0),
            "fee_type": m.get("feeType"),
        })
        time.sleep(0.15)

    path = os.path.join(OUT_DIR, "orderbook_snapshots.json")
    with open(path, "w") as f:
        json.dump(snapshots, f, indent=1)
    print(f"  Saved {len(snapshots)} orderbook snapshots")
    return snapshots


# ── 4. Whale activity detection ──

def analyze_whale_activity(trades):
    """Find whales and their patterns."""
    print("Analyzing whale activity...")

    wallet_stats = defaultdict(lambda: {
        "total_volume": 0, "trade_count": 0, "buy_volume": 0, "sell_volume": 0,
        "markets": set(), "avg_size": 0, "max_size": 0, "pseudonym": "",
    })

    for t in trades:
        w = t.get("proxyWallet", "")
        size = float(t.get("size", 0))
        price = float(t.get("price", 0))
        volume = size * price
        side = t.get("side", "")

        wallet_stats[w]["total_volume"] += volume
        wallet_stats[w]["trade_count"] += 1
        wallet_stats[w]["markets"].add(t.get("market_id", ""))
        wallet_stats[w]["max_size"] = max(wallet_stats[w]["max_size"], size)
        wallet_stats[w]["pseudonym"] = t.get("pseudonym", "")
        if side == "BUY":
            wallet_stats[w]["buy_volume"] += volume
        else:
            wallet_stats[w]["sell_volume"] += volume

    # Convert sets to lists for JSON
    for w in wallet_stats:
        wallet_stats[w]["markets"] = list(wallet_stats[w]["markets"])
        n = wallet_stats[w]["trade_count"]
        wallet_stats[w]["avg_size"] = wallet_stats[w]["total_volume"] / n if n else 0

    # Sort by volume
    whales = sorted(wallet_stats.items(), key=lambda x: -x[1]["total_volume"])

    result = [{"wallet": w, **stats} for w, stats in whales[:200]]

    path = os.path.join(OUT_DIR, "whale_activity.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=1)
    print(f"  Found {len(whales)} unique wallets, saved top 200")
    return result


# ── 5. Question wording bias analysis ──

def analyze_question_bias(resolved):
    """Analyze how question wording correlates with YES/NO outcomes."""
    print("Analyzing question wording bias...")

    keywords = [
        "will", "above", "below", "before", "after", "over", "under",
        "more", "less", "win", "lose", "increase", "decrease", "rise", "fall",
        "higher", "lower", "exceed", "reach", "hit", "break", "drop",
        "vs.", "by", "end of", "before", "positive", "negative",
    ]

    # Track keyword -> [yes_won_count, total_count]
    kw_stats = defaultdict(lambda: [0, 0])
    category_stats = defaultdict(lambda: [0, 0])

    for m in resolved:
        if m["yes_won"] is None:
            continue
        q = m["question"].lower()
        tags = m.get("tags", [])

        for kw in keywords:
            if kw.lower() in q:
                kw_stats[kw][1] += 1
                if m["yes_won"]:
                    kw_stats[kw][0] += 1

        # By tag/category
        for tag in tags:
            category_stats[tag][1] += 1
            if m["yes_won"]:
                category_stats[tag][0] += 1

    # Compute rates
    results = {
        "keyword_bias": {},
        "category_bias": {},
    }

    print("\n  Keyword -> YES win rate (n >= 10):")
    for kw, (yes, total) in sorted(kw_stats.items(), key=lambda x: -x[1][1]):
        if total >= 10:
            rate = yes / total
            results["keyword_bias"][kw] = {
                "yes_rate": round(rate, 4), "total": total, "yes_count": yes
            }
            bias = "YES-biased" if rate > 0.55 else ("NO-biased" if rate < 0.45 else "neutral")
            print(f"    '{kw}': {rate:.1%} YES ({yes}/{total}) - {bias}")

    print("\n  Category -> YES win rate (n >= 5):")
    for cat, (yes, total) in sorted(category_stats.items(), key=lambda x: -x[1][1]):
        if total >= 5:
            rate = yes / total
            results["category_bias"][cat] = {
                "yes_rate": round(rate, 4), "total": total, "yes_count": yes
            }
            print(f"    [{cat}]: {rate:.1%} YES ({yes}/{total})")

    path = os.path.join(OUT_DIR, "question_bias.json")
    with open(path, "w") as f:
        json.dump(results, f, indent=1)
    return results


# ── 6. Resolution convergence analysis ──

def analyze_trade_patterns(trades, resolved_map):
    """Analyze trade patterns: taker vs maker edge, size patterns, timing."""
    print("Analyzing trade patterns...")

    # Bucket trades by time-to-resolution
    buy_at_price = defaultdict(list)  # price bucket -> list of PnLs
    size_buckets = defaultdict(lambda: {"count": 0, "total_pnl": 0})

    for t in trades:
        mid = t.get("market_id", "")
        if mid not in resolved_map:
            continue

        m = resolved_map[mid]
        if m["yes_won"] is None:
            continue

        price = float(t.get("price", 0))
        side = t.get("side", "")
        size = float(t.get("size", 0))

        # Calculate PnL: if YES won, YES buyers profit, NO buyers lose
        # Outcome index 0 = YES, 1 = NO
        outcome_idx = t.get("outcomeIndex", 0)
        won = (outcome_idx == 0 and m["yes_won"]) or (outcome_idx == 1 and not m["yes_won"])

        if side == "BUY":
            pnl = size * (1.0 - price) if won else -size * price
        else:  # SELL
            pnl = size * price if not won else -size * (1.0 - price)

        # Price bucket (round to nearest 0.05)
        bucket = round(price * 20) / 20
        buy_at_price[bucket].append(pnl)

        # Size bucket
        if size < 10:
            sb = "small(<$10)"
        elif size < 100:
            sb = "medium($10-100)"
        elif size < 1000:
            sb = "large($100-1K)"
        else:
            sb = "whale(>$1K)"
        size_buckets[sb]["count"] += 1
        size_buckets[sb]["total_pnl"] += pnl

    print("\n  Price bucket -> avg PnL (BUY side):")
    for bucket in sorted(buy_at_price.keys()):
        pnls = buy_at_price[bucket]
        if len(pnls) >= 5:
            avg = sum(pnls) / len(pnls)
            wr = sum(1 for p in pnls if p > 0) / len(pnls)
            print(f"    price={bucket:.2f}: avg_pnl=${avg:+.2f}, WR={wr:.0%}, n={len(pnls)}")

    print("\n  Size bucket -> avg PnL:")
    for sb, stats in sorted(size_buckets.items()):
        avg = stats["total_pnl"] / stats["count"] if stats["count"] else 0
        print(f"    {sb}: avg_pnl=${avg:+.2f}, n={stats['count']}")

    result = {
        "price_buckets": {str(k): {"avg_pnl": sum(v)/len(v), "count": len(v),
                                    "win_rate": sum(1 for p in v if p > 0)/len(v)}
                          for k, v in buy_at_price.items() if len(v) >= 3},
        "size_buckets": dict(size_buckets),
    }

    path = os.path.join(OUT_DIR, "trade_patterns.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=1)
    return result


# ── Main ──

def main():
    t0 = time.time()

    # Step 1: Resolved markets
    resolved = collect_resolved_markets(limit=500)

    # Step 2: Trades for top resolved markets
    top_markets = sorted(resolved, key=lambda m: -m["volume"])[:100]
    market_ids = [m["id"] for m in top_markets]
    trades = collect_trades_for_markets(market_ids, max_per_market=200)

    # Step 3: Active orderbooks
    collect_active_orderbooks(limit=50)

    # Step 4: Whale activity
    analyze_whale_activity(trades)

    # Step 5: Question bias
    analyze_question_bias(resolved)

    # Step 6: Trade patterns
    resolved_map = {m["id"]: m for m in resolved}
    analyze_trade_patterns(trades, resolved_map)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"Data saved to {OUT_DIR}/")


if __name__ == "__main__":
    main()
