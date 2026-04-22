#!/usr/bin/env python3
"""
Experiment framework v2: uses ACTUAL trade prices from resolved markets.
Each experiment starts $1000, simulates entry at real mid-market prices.

We have trades data: {conditionId: {question, volume, trades: [{side, price, size, timestamp, outcomeIndex}]}}
And resolved markets with outcomes.

Strategy: for each resolved market where we have trade data,
simulate entering at the MEDIAN trade price during the market's lifetime,
then check if we won or lost based on resolution.
"""

import json
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone

# ━━━ Load data ━━━

def load_data():
    raw = json.load(open("bot-data/analytics_v2/resolved_markets_raw.json"))
    trades_data = json.load(open("bot-data/analytics_v2/trades_top500.json"))

    # Build resolved market lookup by conditionId
    resolved = {}
    for m in raw:
        cid = m.get("conditionId", "")
        if not cid:
            continue
        op = m.get("outcomePrices", "")
        if isinstance(op, str):
            try:
                op = json.loads(op)
            except:
                continue
        if not op or len(op) < 2:
            continue
        try:
            yes_won = float(op[0]) > 0.5
        except:
            continue

        resolved[cid] = {
            "question": m.get("question", ""),
            "yes_won": yes_won,
            "volume": float(m.get("volume", 0) or 0),
            "fee_type": m.get("feeType") or "none",
            "fees_enabled": m.get("feesEnabled", False),
            "neg_risk": m.get("negRisk", False),
            "q_lower": m.get("question", "").lower(),
        }

    # Build trade-enriched markets
    markets = []
    for cid, market_data in trades_data.items():
        if cid not in resolved:
            continue

        r = resolved[cid]
        trade_list = market_data.get("trades", [])
        if not trade_list:
            continue

        # Separate YES and NO trades
        yes_prices = []
        no_prices = []
        yes_buy_vol = 0
        yes_sell_vol = 0

        for t in trade_list:
            price = float(t.get("price", 0))
            size = float(t.get("size", 0))
            side = t.get("side", "")
            outcome_idx = t.get("outcomeIndex", 0)

            if outcome_idx == 0:  # YES token trades
                yes_prices.append(price)
                if side == "BUY":
                    yes_buy_vol += size * price
                else:
                    yes_sell_vol += size * price
            else:  # NO token trades
                no_prices.append(price)

        if not yes_prices:
            continue

        # Compute entry price metrics
        median_yes = statistics.median(yes_prices)
        mean_yes = statistics.mean(yes_prices)
        # Exclude extreme prices (< 0.02 or > 0.98) for "mid-market" estimate
        mid_prices = [p for p in yes_prices if 0.02 < p < 0.98]
        if mid_prices:
            mid_yes = statistics.median(mid_prices)
        else:
            mid_yes = median_yes

        markets.append({
            **r,
            "condition_id": cid,
            "median_yes_price": median_yes,
            "mean_yes_price": mean_yes,
            "mid_yes_price": mid_yes,
            "yes_buy_vol": yes_buy_vol,
            "yes_sell_vol": yes_sell_vol,
            "net_flow": (yes_buy_vol - yes_sell_vol) / (yes_buy_vol + yes_sell_vol + 0.001),
            "n_trades": len(trade_list),
            "n_yes_trades": len(yes_prices),
        })

    return markets


# ━━━ Categories ━━━

CATEGORIES = [
    ("fdv_above", r"(fdv|fully diluted).*(above|reach|hit|over)"),
    ("fed_rate", r"(fed\b|interest rate|bps|basis point|fed chair)"),
    ("us_strike", r"will.*(us|u\.s\.|united states).*(strike|bomb|invade|attack)"),
    ("crypto_target", r"(bitcoin|btc|ethereum|eth|solana|sol|xrp).*(above|below|reach|dip|hit|greater)"),
    ("token_launch", r"(launch|list).*(token|coin)"),
    ("sports_champ", r"will .*(win|champion).*\b(cup|finals|championship|tournament|masters|open)\b"),
    ("election", r"will .*(win|elected|nomination).*(president|governor|senator|prime minister)"),
    ("trump", r"\btrump\b"),
    ("oil_price", r"(wti|crude oil).*(hit|above|below)"),
    ("elon", r"elon.*(tweet|post)"),
    ("geopolitics", r"(ceasefire|conflict|military|invade|regime|peace|war)"),
    ("will_generic", r"^will\s"),
]


def classify(q_lower):
    for name, pattern in CATEGORIES:
        if re.search(pattern, q_lower):
            return name
    return "other"


# ━━━ Experiment runner ━━━

def run_exp(markets, name, filter_fn, side, entry_type="mid", size=20, capital=1000.0):
    """
    side: "YES" or "NO"
    entry_type: "mid" (mid-market), "median" (median trade), "mean" (mean trade),
                "pessimistic" (worst quartile for us)
    """
    bal = capital
    wins = 0
    losses = 0
    pnl_total = 0
    trade_log = []

    for m in markets:
        if not filter_fn(m):
            continue

        # Determine entry price
        if entry_type == "mid":
            yes_entry = m["mid_yes_price"]
        elif entry_type == "median":
            yes_entry = m["median_yes_price"]
        elif entry_type == "mean":
            yes_entry = m["mean_yes_price"]
        else:
            yes_entry = m["mid_yes_price"]

        if yes_entry <= 0.01 or yes_entry >= 0.99:
            continue

        if side == "NO":
            entry = 1.0 - yes_entry  # NO price ≈ 1 - YES price
        else:
            entry = yes_entry

        if entry <= 0.01 or entry >= 0.99:
            continue

        trade_size = min(size, bal)
        if trade_size <= 0:
            break

        shares = trade_size / entry
        won = (side == "YES" and m["yes_won"]) or (side == "NO" and not m["yes_won"])

        if won:
            payout = shares * 1.0
            fee = 0
            if m["fees_enabled"] and m["fee_type"] not in ("none", "FREE", None, ""):
                fee = trade_size * 0.02 * (1.0 - entry)
            pnl = payout - trade_size - fee
            wins += 1
        else:
            pnl = -trade_size
            losses += 1

        bal += pnl
        pnl_total += pnl
        trade_log.append({"q": m["question"][:40], "entry": round(entry, 3),
                          "won": won, "pnl": round(pnl, 2)})

        if bal <= 0:
            break

    n = wins + losses
    return {
        "name": name,
        "trades": n,
        "wins": wins,
        "wr": round(wins / n * 100, 1) if n else 0,
        "pnl": round(pnl_total, 2),
        "roi": round((bal - capital) / capital * 100, 1),
        "final": round(bal, 2),
        "avg_pnl": round(pnl_total / n, 2) if n else 0,
        "samples": trade_log[:3],
    }


# ━━━ Generate all experiments ━━━

def main():
    print("Loading data...")
    markets = load_data()
    print(f"Markets with trade data + resolution: {len(markets)}")

    # Classify
    for m in markets:
        m["category"] = classify(m["q_lower"])

    # Category stats
    cat_stats = defaultdict(lambda: {"n": 0, "no": 0, "avg_yes": []})
    for m in markets:
        cat_stats[m["category"]]["n"] += 1
        if not m["yes_won"]:
            cat_stats[m["category"]]["no"] += 1
        cat_stats[m["category"]]["avg_yes"].append(m["mid_yes_price"])

    print(f"\n{'Category':<18} {'N':>4} {'NO%':>5} {'AvgYES':>7}")
    print("-" * 40)
    for cat, s in sorted(cat_stats.items(), key=lambda x: -x[1]["n"]):
        no_rate = s["no"] / s["n"] * 100 if s["n"] else 0
        avg_yes = statistics.mean(s["avg_yes"]) if s["avg_yes"] else 0
        print(f"{cat:<18} {s['n']:>4} {no_rate:>4.0f}% {avg_yes:>6.3f}")

    # Generate experiments
    results = []

    # 1. Category × Side × EntryType × Size
    for cat in cat_stats:
        if cat_stats[cat]["n"] < 5:
            continue
        for side in ["NO", "YES"]:
            for entry_type in ["mid", "median", "mean"]:
                for size in [10, 20, 50]:
                    r = run_exp(markets, f"cat_{cat}_{side}_{entry_type}_sz{size}",
                                lambda m, c=cat: m["category"] == c,
                                side, entry_type, size)
                    results.append(r)

    # 2. Volume filters
    for vol_min in [0, 50000, 100000, 500000]:
        for side in ["NO", "YES"]:
            r = run_exp(markets, f"vol_gt{vol_min//1000}k_{side}",
                        lambda m, v=vol_min: m["volume"] >= v,
                        side, "mid", 20)
            results.append(r)

    # 3. Net flow (whale direction)
    for flow_thresh in [-0.3, -0.5, 0.3, 0.5]:
        if flow_thresh > 0:
            # Follow whales: buy what they're buying
            r = run_exp(markets, f"follow_whales_flow_gt{flow_thresh}_YES",
                        lambda m, ft=flow_thresh: m["net_flow"] > ft,
                        "YES", "mid", 20)
            results.append(r)
            r = run_exp(markets, f"fade_whales_flow_gt{flow_thresh}_NO",
                        lambda m, ft=flow_thresh: m["net_flow"] > ft,
                        "NO", "mid", 20)
            results.append(r)
        else:
            r = run_exp(markets, f"follow_whales_flow_lt{flow_thresh}_NO",
                        lambda m, ft=flow_thresh: m["net_flow"] < ft,
                        "NO", "mid", 20)
            results.append(r)
            r = run_exp(markets, f"fade_whales_flow_lt{flow_thresh}_YES",
                        lambda m, ft=flow_thresh: m["net_flow"] < ft,
                        "YES", "mid", 20)
            results.append(r)

    # 4. Trade count (liquidity proxy)
    for min_trades in [5, 10, 20, 50]:
        for side in ["NO", "YES"]:
            r = run_exp(markets, f"trades_gt{min_trades}_{side}",
                        lambda m, t=min_trades: m["n_trades"] >= t,
                        side, "mid", 20)
            results.append(r)

    # 5. Entry price filters: only enter when price is "cheap"
    for max_entry in [0.20, 0.30, 0.40, 0.50]:
        r = run_exp(markets, f"cheap_yes_lt{int(max_entry*100)}",
                    lambda m, me=max_entry: m["mid_yes_price"] <= me,
                    "YES", "mid", 20)
        results.append(r)
        r = run_exp(markets, f"cheap_no_lt{int(max_entry*100)}",
                    lambda m, me=max_entry: (1 - m["mid_yes_price"]) <= me,
                    "NO", "mid", 20)
        results.append(r)

    # 6. Combined: category + price filter
    for cat in ["fdv_above", "election", "fed_rate", "geopolitics", "trump"]:
        if cat_stats[cat]["n"] < 5:
            continue
        for max_entry in [0.30, 0.50, 0.70]:
            r = run_exp(markets, f"{cat}_NO_entry_lt{int(max_entry*100)}",
                        lambda m, c=cat, me=max_entry: m["category"] == c and (1-m["mid_yes_price"]) <= me,
                        "NO", "mid", 20)
            results.append(r)

    # 7. Kelly sizing
    for cat in cat_stats:
        if cat_stats[cat]["n"] < 20:
            continue
        no_rate = cat_stats[cat]["no"] / cat_stats[cat]["n"]
        r = run_exp(markets, f"kelly_{cat}_NO",
                    lambda m, c=cat: m["category"] == c,
                    "NO", "mid",
                    size=0)  # Override size below
        # Re-run with Kelly sizing
        bal = 1000.0
        wins = losses = 0
        pnl_total = 0
        for m in markets:
            if m["category"] != cat:
                continue
            entry = 1.0 - m["mid_yes_price"]
            if entry <= 0.01 or entry >= 0.99:
                continue
            # Kelly: f = (p*b - q)/b where b=(1-entry)/entry
            b = (1 - entry) / entry
            f = (no_rate * b - (1 - no_rate)) / b
            f = max(0, min(f, 0.15))  # Cap at 15%
            trade_size = f * bal
            if trade_size < 1:
                continue
            shares = trade_size / entry
            won = not m["yes_won"]
            if won:
                pnl = shares - trade_size
                wins += 1
            else:
                pnl = -trade_size
                losses += 1
            bal += pnl
            pnl_total += pnl
            if bal <= 0:
                break
        n = wins + losses
        if n >= 5:
            results.append({
                "name": f"kelly_{cat}_NO",
                "trades": n, "wins": wins,
                "wr": round(wins/n*100, 1),
                "pnl": round(pnl_total, 2),
                "roi": round((bal-1000)/1000*100, 1),
                "final": round(bal, 2),
                "avg_pnl": round(pnl_total/n, 2),
                "samples": [],
            })

    # Sort by ROI
    results.sort(key=lambda r: -r["roi"])

    # Print results
    profitable = [r for r in results if r["pnl"] > 0 and r["trades"] >= 5]
    losing = [r for r in results if r["pnl"] <= 0 and r["trades"] >= 5]

    print(f"\n{'='*100}")
    print(f"TOP PROFITABLE ({len(profitable)} of {len(results)} experiments)")
    print(f"{'='*100}")
    print(f"{'#':>3} {'ROI%':>7} {'PnL$':>8} {'Trades':>6} {'WR%':>5} {'AvgPnL':>7} {'Final$':>8}  Strategy")
    print("-" * 100)
    for i, r in enumerate(profitable[:60], 1):
        print(f"{i:>3} {r['roi']:>+6.1f}% ${r['pnl']:>+7.0f} {r['trades']:>6} "
              f"{r['wr']:>4.0f}% ${r['avg_pnl']:>+6.2f} ${r['final']:>7.0f}  {r['name']}")

    print(f"\n{'='*100}")
    print(f"WORST 20")
    print(f"{'='*100}")
    losing.sort(key=lambda r: r["roi"])
    for i, r in enumerate(losing[:20], 1):
        print(f"{i:>3} {r['roi']:>+6.1f}% ${r['pnl']:>+7.0f} {r['trades']:>6} "
              f"{r['wr']:>4.0f}% ${r['avg_pnl']:>+6.02f} ${r['final']:>7.0f}  {r['name']}")

    # Summary
    with_trades = [r for r in results if r["trades"] >= 5]
    print(f"\n{'='*100}")
    print(f"SUMMARY: {len(results)} experiments, {len(with_trades)} with 5+ trades")
    print(f"  Profitable: {len(profitable)} ({len(profitable)/len(with_trades)*100:.0f}%)" if with_trades else "")
    if profitable:
        print(f"  Best: {profitable[0]['name']} — ROI {profitable[0]['roi']:+.1f}%, {profitable[0]['trades']} trades")
    if losing:
        print(f"  Worst: {losing[0]['name']} — ROI {losing[0]['roi']:+.1f}%, {losing[0]['trades']} trades")

    # Save
    with open("bot-data/experiments_v2_results.json", "w") as f:
        json.dump({"total": len(results), "profitable": len(profitable), "results": results}, f, indent=1)
    print(f"\nSaved to bot-data/experiments_v2_results.json")


if __name__ == "__main__":
    main()
