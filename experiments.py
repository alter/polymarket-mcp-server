#!/usr/bin/env python3
"""
Massive experiment framework for Polymarket strategy discovery.
Tests hundreds of strategy variants on 2100 resolved markets.
Each experiment starts with $1000 and simulates paper trading.

Experiments cover:
  - Category-based NO bias (per sub-category)
  - Price-level entry filters
  - Volume filters
  - Time-to-expiry filters
  - Keyword combinations
  - Multi-outcome arb simulation
  - Spread/liquidity filters
  - Question length/complexity patterns
  - Fee type patterns
  - Contrarian strategies
"""

import json
import re
import itertools
from collections import defaultdict
from datetime import datetime, timezone

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Load and prepare data
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_markets():
    raw = json.load(open("bot-data/analytics_v2/resolved_markets_raw.json"))
    markets = []
    for m in raw:
        op = m.get("outcomePrices", "")
        if isinstance(op, str):
            try:
                op = json.loads(op)
            except:
                continue
        if not op or len(op) < 2:
            continue
        try:
            yes_price = float(op[0])
            no_price = float(op[1])
        except:
            continue

        yes_won = yes_price > 0.5
        vol = float(m.get("volume", 0) or m.get("volumeNum", 0) or 0)
        spread = float(m.get("spread", 0) or 0)
        best_bid = float(m.get("bestBid", 0) or 0)
        best_ask = float(m.get("bestAsk", 0) or 0)
        last_price = float(m.get("lastTradePrice", 0) or 0)

        # Parse dates
        created = m.get("createdAt", "")
        closed = m.get("closedTime", "") or m.get("endDate", "")
        try:
            dt_created = datetime.fromisoformat(created.replace("Z", "+00:00"))
            dt_closed = datetime.fromisoformat(closed.replace("Z", "+00:00"))
            duration_days = (dt_closed - dt_created).total_seconds() / 86400
        except:
            duration_days = None

        # Event info
        events = m.get("events", [])
        event_slug = events[0].get("slug", "") if events else ""
        neg_risk = m.get("negRisk", False)
        fee_type = m.get("feeType") or "none"
        fees_enabled = m.get("feesEnabled", False)
        group_title = m.get("groupItemTitle", "")

        markets.append({
            "id": m.get("id"),
            "question": m.get("question", ""),
            "yes_won": yes_won,
            "yes_price": yes_price,
            "no_price": no_price,
            "volume": vol,
            "spread": spread,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "last_price": last_price,
            "duration_days": duration_days,
            "event_slug": event_slug,
            "neg_risk": neg_risk,
            "fee_type": fee_type,
            "fees_enabled": fees_enabled,
            "group_title": group_title,
            "q_lower": m.get("question", "").lower(),
            "q_len": len(m.get("question", "")),
        })
    return markets


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Category classifiers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CATEGORIES = [
    ("fdv_above", r"(fdv|fully diluted).*(above|reach|hit|over)"),
    ("auction_above", r"(auction|clearing).*(above|price)"),
    ("fed_rate", r"(fed\b|interest rate|bps|basis point|fed chair)"),
    ("us_strike", r"will.*(us|u\.s\.|united states).*(strike|bomb|invade|attack)"),
    ("ceasefire_by", r"ceasefire.*by"),
    ("conflict_ends", r"conflict.*(ends|end).*by"),
    ("crypto_above", r"(bitcoin|btc|ethereum|eth|solana|sol|xrp).*(above|greater|reach|hit)"),
    ("crypto_dip", r"(bitcoin|btc|ethereum|eth|solana|sol|xrp).*(dip|drop|fall|below)"),
    ("token_launch", r"(launch|list).*(token|coin)"),
    ("sports_daily", r"(fc|cf|united|city|atletico|madrid|roma|barcelona|milan|inter|juventus|liverpool|chelsea|arsenal|psg|marseille|monaco|lyon|dortmund|bayern|lakers|celtics|warriors|heat|bulls|thunder|suns|bucks|76ers|knicks|nets|mavericks|nuggets|clippers|cavaliers) (win|vs)"),
    ("sports_championship", r"will .*(win|champion).*\b(cup|finals|championship|tournament|league|masters|open|world series|stanley cup|super bowl)\b"),
    ("election_longshot", r"will .*(win|elected|nomination).*(president|governor|senator|prime minister)"),
    ("eurovision", r"eurovision"),
    ("temperature", r"(temperature|degrees|°|highest temp)"),
    ("elon_tweets", r"elon.*(tweet|post)"),
    ("oil_price", r"(wti|crude oil|brent).*(hit|above|below|price)"),
    ("ai_model", r"(best ai model|claude|gpt|gemini|anthropic|openai)"),
    ("esports", r"(lol:|dota|csgo|valorant|counter-strike|league of legends)"),
    ("soccer_ou", r"o/u \d"),
    ("will_above_price", r"will.*price.*(above|below|greater)"),
    ("up_or_down", r"up or down"),
    ("mrbeast", r"mrbeast|mr\.?\s?beast"),
    ("trump", r"\btrump\b"),
    ("will_generic", r"^will\s"),
]


def classify(q_lower):
    for name, pattern in CATEGORIES:
        if re.search(pattern, q_lower):
            return name
    return "other"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Keyword extraction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KEYWORDS = [
    "will", "above", "below", "before", "after", "by", "hit", "reach",
    "drop", "fall", "rise", "win", "lose", "over", "under", "more", "less",
    "increase", "decrease", "higher", "lower", "exceed", "break", "dip",
    "positive", "negative", "end", "start", "launch", "vs", "or",
]


def get_keywords(q_lower):
    words = set(q_lower.split())
    return [kw for kw in KEYWORDS if kw in words]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Experiment runner
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_experiment(markets, name, filter_fn, side, entry_price_fn, size_fn,
                   initial_capital=1000.0):
    """
    Run a single experiment.

    filter_fn(m) -> bool: should we trade this market?
    side: "YES" or "NO" — which side to buy
    entry_price_fn(m) -> float: what price we enter at (simulated)
    size_fn(m, capital) -> float: position size in USD
    """
    capital = initial_capital
    trades = []
    wins = 0
    losses = 0
    total_pnl = 0

    for m in markets:
        if not filter_fn(m):
            continue

        entry = entry_price_fn(m)
        if entry <= 0 or entry >= 1.0:
            continue

        size = size_fn(m, capital)
        if size <= 0 or size > capital:
            continue

        shares = size / entry

        # Did we win?
        if side == "NO":
            won = not m["yes_won"]
        else:
            won = m["yes_won"]

        if won:
            payout = shares * 1.0  # Full payout
            # Deduct fees if applicable
            fee = 0
            if m["fees_enabled"] and m["fee_type"] not in ("none", "FREE", None):
                fee = size * 0.02 * (1.0 - entry)  # Approximate Polymarket fee
            pnl = payout - size - fee
            wins += 1
        else:
            pnl = -size
            losses += 1

        capital += pnl
        total_pnl += pnl
        trades.append({
            "question": m["question"][:60],
            "side": side,
            "entry": entry,
            "size": size,
            "won": won,
            "pnl": pnl,
        })

        if capital <= 0:
            break

    n = wins + losses
    return {
        "name": name,
        "trades": n,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / n if n else 0,
        "total_pnl": round(total_pnl, 2),
        "final_capital": round(capital, 2),
        "roi_pct": round((capital - initial_capital) / initial_capital * 100, 2),
        "avg_pnl": round(total_pnl / n, 2) if n else 0,
        "max_trade_pnl": round(max((t["pnl"] for t in trades), default=0), 2),
        "min_trade_pnl": round(min((t["pnl"] for t in trades), default=0), 2),
        "trade_details": trades[:5],  # Sample
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Generate experiments
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_experiments(markets):
    experiments = []

    # Classify all markets
    for m in markets:
        m["category"] = classify(m["q_lower"])
        m["keywords"] = get_keywords(m["q_lower"])

    # ── Exp 1: Category-based NO bias ──
    cat_counts = defaultdict(int)
    for m in markets:
        cat_counts[m["category"]] += 1

    for cat in cat_counts:
        if cat_counts[cat] < 10:
            continue
        for side in ["NO", "YES"]:
            for vol_min in [0, 10000, 50000, 100000]:
                for size in [10, 20, 50]:
                    experiments.append({
                        "name": f"cat_{cat}_{side}_vol{vol_min//1000}k_sz{size}",
                        "filter": lambda m, c=cat, v=vol_min: m["category"] == c and m["volume"] >= v,
                        "side": side,
                        "entry_price": lambda m, s=side: (
                            m["no_price"] if s == "NO" else m["yes_price"]
                        ) if (m["no_price"] if s == "NO" else m["yes_price"]) > 0.01 else 0.01,
                        "size": lambda m, capital, sz=size: min(sz, capital),
                    })

    # ── Exp 2: Entry price level filters ──
    for side in ["NO"]:
        for price_max in [0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]:
            for vol_min in [0, 10000, 100000]:
                experiments.append({
                    "name": f"no_price_lt{int(price_max*100)}_vol{vol_min//1000}k",
                    "filter": lambda m, pm=price_max, v=vol_min: (
                        m["no_price"] > 0.01 and m["no_price"] <= pm and m["volume"] >= v
                    ),
                    "side": "NO",
                    "entry_price": lambda m: max(m["no_price"], 0.01),
                    "size": lambda m, capital: min(20, capital),
                })

    # ── Exp 3: Keyword-based strategies ──
    for kw in KEYWORDS:
        kw_markets = [m for m in markets if kw in m["keywords"]]
        if len(kw_markets) < 15:
            continue
        for side in ["NO", "YES"]:
            experiments.append({
                "name": f"kw_{kw}_{side}",
                "filter": lambda m, k=kw: k in m["keywords"],
                "side": side,
                "entry_price": lambda m, s=side: max(m["no_price"] if s == "NO" else m["yes_price"], 0.01),
                "size": lambda m, capital: min(20, capital),
            })

    # ── Exp 4: Duration-based (short-term vs long-term) ──
    for max_days in [1, 3, 7, 14, 30, 90, 365]:
        for side in ["NO", "YES"]:
            experiments.append({
                "name": f"duration_lt{max_days}d_{side}",
                "filter": lambda m, d=max_days: m["duration_days"] is not None and m["duration_days"] <= d,
                "side": side,
                "entry_price": lambda m, s=side: max(m["no_price"] if s == "NO" else m["yes_price"], 0.01),
                "size": lambda m, capital: min(20, capital),
            })

    # ── Exp 5: Fee type strategies ──
    for fee in ["FREE", "none"]:
        for side in ["NO", "YES"]:
            experiments.append({
                "name": f"fee_{fee}_{side}",
                "filter": lambda m, f=fee: str(m["fee_type"]).upper() == f.upper() or (f == "none" and not m["fees_enabled"]),
                "side": side,
                "entry_price": lambda m, s=side: max(m["no_price"] if s == "NO" else m["yes_price"], 0.01),
                "size": lambda m, capital: min(20, capital),
            })

    # ── Exp 6: Neg risk markets ──
    for neg_risk in [True, False]:
        for side in ["NO", "YES"]:
            experiments.append({
                "name": f"negrisk_{neg_risk}_{side}",
                "filter": lambda m, nr=neg_risk: m["neg_risk"] == nr,
                "side": side,
                "entry_price": lambda m, s=side: max(m["no_price"] if s == "NO" else m["yes_price"], 0.01),
                "size": lambda m, capital: min(20, capital),
            })

    # ── Exp 7: Question length (complexity proxy) ──
    for max_len in [40, 60, 80, 100, 150]:
        for side in ["NO", "YES"]:
            experiments.append({
                "name": f"qlen_lt{max_len}_{side}",
                "filter": lambda m, l=max_len: m["q_len"] <= l,
                "side": side,
                "entry_price": lambda m, s=side: max(m["no_price"] if s == "NO" else m["yes_price"], 0.01),
                "size": lambda m, capital: min(20, capital),
            })

    # ── Exp 8: Spread-based ──
    for max_spread in [0.001, 0.005, 0.01, 0.02, 0.05]:
        for side in ["NO", "YES"]:
            experiments.append({
                "name": f"spread_lt{max_spread}_{side}",
                "filter": lambda m, s=max_spread: m["spread"] > 0 and m["spread"] <= s,
                "side": side,
                "entry_price": lambda m, s=side: max(m["no_price"] if s == "NO" else m["yes_price"], 0.01),
                "size": lambda m, capital: min(20, capital),
            })

    # ── Exp 9: Multi-keyword combos ──
    yes_kws = ["will", "by", "hit", "reach", "before"]
    no_kws = ["above", "after", "drop", "over", "below"]
    for min_yes_kw in [1, 2, 3]:
        experiments.append({
            "name": f"multi_yes_kw{min_yes_kw}_YES",
            "filter": lambda m, n=min_yes_kw: sum(1 for k in yes_kws if k in m["keywords"]) >= n,
            "side": "YES",
            "entry_price": lambda m: max(m["yes_price"], 0.01),
            "size": lambda m, capital: min(20, capital),
        })
    for min_no_kw in [1, 2, 3]:
        experiments.append({
            "name": f"multi_no_kw{min_no_kw}_NO",
            "filter": lambda m, n=min_no_kw: sum(1 for k in no_kws if k in m["keywords"]) >= n,
            "side": "NO",
            "entry_price": lambda m: max(m["no_price"], 0.01),
            "size": lambda m, capital: min(20, capital),
        })

    # ── Exp 10: Event-based (group markets) ──
    for side in ["NO", "YES"]:
        experiments.append({
            "name": f"has_event_{side}",
            "filter": lambda m: bool(m["event_slug"]),
            "side": side,
            "entry_price": lambda m, s=side: max(m["no_price"] if s == "NO" else m["yes_price"], 0.01),
            "size": lambda m, capital: min(20, capital),
        })
        experiments.append({
            "name": f"no_event_{side}",
            "filter": lambda m: not m["event_slug"],
            "side": side,
            "entry_price": lambda m, s=side: max(m["no_price"] if s == "NO" else m["yes_price"], 0.01),
            "size": lambda m, capital: min(20, capital),
        })

    # ── Exp 11: Group item markets (part of multi-outcome) ──
    for side in ["NO", "YES"]:
        experiments.append({
            "name": f"group_item_{side}",
            "filter": lambda m: bool(m["group_title"]),
            "side": side,
            "entry_price": lambda m, s=side: max(m["no_price"] if s == "NO" else m["yes_price"], 0.01),
            "size": lambda m, capital: min(20, capital),
        })

    # ── Exp 12: "Trump" markets ──
    for side in ["NO", "YES"]:
        experiments.append({
            "name": f"trump_{side}",
            "filter": lambda m: "trump" in m["q_lower"],
            "side": side,
            "entry_price": lambda m, s=side: max(m["no_price"] if s == "NO" else m["yes_price"], 0.01),
            "size": lambda m, capital: min(20, capital),
        })

    # ── Exp 13: Contrarian — buy the cheap side ──
    experiments.append({
        "name": "contrarian_buy_cheap_yes",
        "filter": lambda m: m["yes_price"] <= 0.20 and m["volume"] >= 10000,
        "side": "YES",
        "entry_price": lambda m: max(m["yes_price"], 0.01),
        "size": lambda m, capital: min(10, capital),
    })
    experiments.append({
        "name": "contrarian_buy_cheap_no",
        "filter": lambda m: m["no_price"] <= 0.20 and m["volume"] >= 10000,
        "side": "NO",
        "entry_price": lambda m: max(m["no_price"], 0.01),
        "size": lambda m, capital: min(10, capital),
    })

    # ── Exp 14: High conviction — buy near-certain outcomes ──
    experiments.append({
        "name": "high_conv_yes_gt80",
        "filter": lambda m: m["yes_price"] >= 0.80 and m["volume"] >= 10000,
        "side": "YES",
        "entry_price": lambda m: max(m["yes_price"], 0.01),
        "size": lambda m, capital: min(50, capital),
    })
    experiments.append({
        "name": "high_conv_no_gt80",
        "filter": lambda m: m["no_price"] >= 0.80 and m["volume"] >= 10000,
        "side": "NO",
        "entry_price": lambda m: max(m["no_price"], 0.01),
        "size": lambda m, capital: min(50, capital),
    })

    # ── Exp 15: Variable sizing — Kelly criterion approximation ──
    for cat in ["fdv_above", "election_longshot", "will_generic", "fed_rate"]:
        # Get historical NO rate for this category
        cat_markets = [m for m in markets if m["category"] == cat]
        if len(cat_markets) < 20:
            continue
        no_rate = sum(1 for m in cat_markets if not m["yes_won"]) / len(cat_markets)

        experiments.append({
            "name": f"kelly_{cat}_NO",
            "filter": lambda m, c=cat: m["category"] == c and m["no_price"] > 0.01,
            "side": "NO",
            "entry_price": lambda m: max(m["no_price"], 0.01),
            "size": lambda m, capital, nr=no_rate: min(
                # Kelly: f* = (p*b - q) / b where b = (1-entry)/entry, p=no_rate, q=1-no_rate
                max(0, (nr * (1 - m["no_price"]) / m["no_price"] - (1 - nr)) / ((1 - m["no_price"]) / m["no_price"])) * capital * 0.25,
                capital * 0.10  # Max 10% per trade
            ),
        })

    return experiments


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    print("Loading markets...")
    markets = load_markets()
    print(f"Loaded {len(markets)} resolved markets")

    print("Generating experiments...")
    experiments = generate_experiments(markets)
    print(f"Generated {len(experiments)} experiments")

    print(f"\nRunning experiments...")
    results = []
    for i, exp in enumerate(experiments):
        result = run_experiment(
            markets,
            name=exp["name"],
            filter_fn=exp["filter"],
            side=exp["side"],
            entry_price_fn=exp["entry_price"],
            size_fn=exp["size"],
        )
        results.append(result)
        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{len(experiments)}...")

    # Sort by ROI
    results.sort(key=lambda r: -r["roi_pct"])

    # Print top profitable
    print(f"\n{'='*120}")
    print(f"TOP 50 PROFITABLE EXPERIMENTS (of {len(experiments)} total)")
    print(f"{'='*120}")
    print(f"{'#':>3} {'ROI%':>8} {'PnL$':>8} {'Trades':>6} {'WR%':>5} {'AvgPnL':>7} {'Final$':>8}  Strategy")
    print("-" * 120)

    profitable = [r for r in results if r["total_pnl"] > 0 and r["trades"] >= 5]
    for i, r in enumerate(profitable[:50], 1):
        print(f"{i:>3} {r['roi_pct']:>+7.1f}% ${r['total_pnl']:>+7.0f} {r['trades']:>6} "
              f"{r['win_rate']*100:>4.0f}% ${r['avg_pnl']:>+6.2f} ${r['final_capital']:>7.0f}  {r['name']}")

    # Print worst
    print(f"\n{'='*120}")
    print(f"BOTTOM 20 (worst losses)")
    print(f"{'='*120}")
    worst = [r for r in results if r["trades"] >= 5]
    worst.sort(key=lambda r: r["roi_pct"])
    for i, r in enumerate(worst[:20], 1):
        print(f"{i:>3} {r['roi_pct']:>+7.1f}% ${r['total_pnl']:>+7.0f} {r['trades']:>6} "
              f"{r['win_rate']*100:>4.0f}% ${r['avg_pnl']:>+6.2f} ${r['final_capital']:>7.0f}  {r['name']}")

    # Summary statistics
    all_with_trades = [r for r in results if r["trades"] >= 5]
    n_profitable = sum(1 for r in all_with_trades if r["total_pnl"] > 0)
    print(f"\n{'='*120}")
    print(f"SUMMARY")
    print(f"  Total experiments: {len(experiments)}")
    print(f"  With 5+ trades: {len(all_with_trades)}")
    print(f"  Profitable: {n_profitable} ({n_profitable/len(all_with_trades)*100:.0f}%)" if all_with_trades else "  No results")
    print(f"  Best ROI: {results[0]['roi_pct']:+.1f}% ({results[0]['name']})")
    print(f"  Worst ROI: {results[-1]['roi_pct']:+.1f}% ({results[-1]['name']})")

    # Category breakdown
    print(f"\n  Category NO rates (from data):")
    cat_stats = defaultdict(lambda: [0, 0])
    for m in markets:
        cat_stats[m["category"]][1] += 1
        if not m["yes_won"]:
            cat_stats[m["category"]][0] += 1
    for cat, (no, total) in sorted(cat_stats.items(), key=lambda x: -x[1][1]):
        if total >= 5:
            print(f"    {cat:<22} NO={no/total:.1%} (n={total})")

    # Find best strategy per category
    print(f"\n  Best strategy per approach:")
    approaches = defaultdict(list)
    for r in all_with_trades:
        if r["total_pnl"] > 0:
            prefix = r["name"].split("_")[0]
            approaches[prefix].append(r)
    for approach, strats in sorted(approaches.items()):
        best = max(strats, key=lambda r: r["roi_pct"])
        print(f"    {approach:<15} best={best['name']:<40} ROI={best['roi_pct']:+.1f}% trades={best['trades']}")

    # Save all results
    save_results = [{k: v for k, v in r.items() if k != "trade_details"} for r in results]
    with open("bot-data/experiment_results.json", "w") as f:
        json.dump({
            "total_experiments": len(experiments),
            "profitable_count": n_profitable,
            "results": save_results,
        }, f, indent=1)
    print(f"\n  Saved to bot-data/experiment_results.json")

    # Also save profitable ones with trade details
    profitable_detail = [r for r in results if r["total_pnl"] > 0 and r["trades"] >= 5]
    profitable_detail.sort(key=lambda r: -r["roi_pct"])
    with open("bot-data/profitable_experiments.json", "w") as f:
        json.dump(profitable_detail[:100], f, indent=1)
    print(f"  Saved top 100 profitable to bot-data/profitable_experiments.json")


if __name__ == "__main__":
    main()
