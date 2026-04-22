#!/usr/bin/env python3
"""
Experiments v3: Monte Carlo simulations, entry timing, spread impact,
portfolio diversification, volatility regimes, price trajectory.
Each experiment starts $1000. Uses actual trade data from resolved markets.
"""
import json, re, random, statistics
from collections import defaultdict
import numpy as np

random.seed(42)
np.random.seed(42)

# ━━━ Load data ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_data():
    raw = json.load(open("bot-data/analytics_v2/resolved_markets_raw.json"))
    trades_data = json.load(open("bot-data/analytics_v2/trades_top500.json"))

    resolved = {}
    for m in raw:
        cid = m.get("conditionId", "")
        if not cid:
            continue
        op = m.get("outcomePrices", "")
        if isinstance(op, str):
            try: op = json.loads(op)
            except: continue
        if not op or len(op) < 2:
            continue
        try: yes_won = float(op[0]) > 0.5
        except: continue
        resolved[cid] = {
            "question": m.get("question", ""), "yes_won": yes_won,
            "volume": float(m.get("volume", 0) or 0),
            "fees_enabled": m.get("feesEnabled", False),
            "fee_type": m.get("feeType") or "none",
        }

    markets = []
    for cid, md in trades_data.items():
        if cid not in resolved:
            continue
        r = resolved[cid]
        tlist = md.get("trades", [])
        if not tlist:
            continue

        # Parse trades with timestamps
        parsed = []
        for t in tlist:
            price = float(t.get("price", 0))
            size = float(t.get("size", 0))
            ts = int(t.get("timestamp", 0))
            side = t.get("side", "")
            oi = t.get("outcomeIndex", 0)
            if price > 0 and size > 0:
                parsed.append({"price": price, "size": size, "ts": ts, "side": side, "oi": oi})
        if not parsed:
            continue

        parsed.sort(key=lambda x: x["ts"])

        # YES token prices
        yes_prices = [t["price"] for t in parsed if t["oi"] == 0]
        if not yes_prices:
            continue

        mid_prices = [p for p in yes_prices if 0.02 < p < 0.98]
        mid_yes = statistics.median(mid_prices) if mid_prices else statistics.median(yes_prices)

        # Entry at different time windows
        n = len(parsed)
        early_yes = [t["price"] for t in parsed[:n//4] if t["oi"] == 0]
        mid_window = [t["price"] for t in parsed[n//4:3*n//4] if t["oi"] == 0]
        late_yes = [t["price"] for t in parsed[3*n//4:] if t["oi"] == 0]

        # Volume flow
        buy_vol = sum(t["price"]*t["size"] for t in parsed if t["side"]=="BUY" and t["oi"]==0)
        sell_vol = sum(t["price"]*t["size"] for t in parsed if t["side"]=="SELL" and t["oi"]==0)
        net_flow = (buy_vol - sell_vol) / (buy_vol + sell_vol + 0.001)

        # Volatility (std of yes prices)
        vol_std = statistics.stdev(yes_prices) if len(yes_prices) > 1 else 0

        # Price trajectory (slope)
        if len(parsed) >= 5:
            xs = np.array([t["ts"] for t in parsed if t["oi"]==0], dtype=float)
            ys = np.array([t["price"] for t in parsed if t["oi"]==0])
            if len(xs) >= 5 and xs[-1] > xs[0]:
                xs = (xs - xs[0]) / (xs[-1] - xs[0])
                slope = float(np.polyfit(xs, ys, 1)[0])
            else:
                slope = 0
        else:
            slope = 0

        # Market age in hours
        timestamps = [t["ts"] for t in parsed if t["ts"] > 0]
        age_hours = (max(timestamps) - min(timestamps)) / 3600 if len(timestamps) > 1 else 0

        # HHI (volume concentration)
        wallet_vol = defaultdict(float)
        # We don't have wallet data in trades_top500, use trade sizes as proxy
        size_list = [t["size"] for t in parsed]
        total_size = sum(size_list)
        if total_size > 0:
            # approx HHI from trade size distribution (top trades share)
            sorted_sizes = sorted(size_list, reverse=True)
            top5_share = sum(sorted_sizes[:5]) / total_size
        else:
            top5_share = 0

        markets.append({
            **r, "condition_id": cid,
            "mid_yes": mid_yes,
            "early_yes": statistics.median(early_yes) if early_yes else mid_yes,
            "mid_window_yes": statistics.median(mid_window) if mid_window else mid_yes,
            "late_yes": statistics.median(late_yes) if late_yes else mid_yes,
            "net_flow": net_flow, "vol_std": vol_std, "slope": slope,
            "age_hours": age_hours, "top5_share": top5_share,
            "n_trades": len(parsed), "n_yes": len(yes_prices),
            "q_lower": r["question"].lower(),
        })
    return markets

# ━━━ Categories ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CATS = [
    ("sports", r"(win on 2026|match|nfl|nba|nhl|mlb|premier league|champions league|serie a|la liga|bundesliga|epl|ufc|mma|boxing|tennis|wta|atp|grand slam|fc win|fc draw)"),
    ("fdv_above", r"(fdv|fully diluted).*(above|reach|hit|over)"),
    ("fed_rate", r"(fed\b|interest rate|bps|basis point|fed chair)"),
    ("crypto_target", r"(bitcoin|btc|ethereum|eth|solana|sol|xrp).*(above|below|reach|dip|hit|greater)"),
    ("election", r"will .*(win|elected|nomination).*(president|governor|senator|prime minister)"),
    ("trump", r"\btrump\b"),
    ("geopolitics", r"(ceasefire|conflict|military|invade|regime|peace|war|missile|strike|bomb|sanction)"),
]

def classify(q):
    for name, pat in CATS:
        if re.search(pat, q): return name
    return "other"

# ━━━ Core sim ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def sim(mlist, side, entry_field="mid_yes", size=20, capital=1000.0, spread=0.0):
    """Run single experiment. Returns dict with ROI, PnL etc."""
    bal = capital
    wins = losses = 0
    pnl_total = 0
    for m in mlist:
        yes_entry = m[entry_field]
        if yes_entry <= 0.01 or yes_entry >= 0.99:
            continue
        entry = (1.0 - yes_entry) if side == "NO" else yes_entry
        entry += spread / 2  # buyer pays half spread
        if entry <= 0.01 or entry >= 0.99:
            continue
        ts = min(size, bal)
        if ts <= 0: break
        shares = ts / entry
        won = (side == "YES" and m["yes_won"]) or (side == "NO" and not m["yes_won"])
        if won:
            fee = ts * 0.02 * (1 - entry) if m["fees_enabled"] and m["fee_type"] not in ("none","FREE",None,"") else 0
            pnl = shares * 1.0 - ts - fee
        else:
            pnl = -ts
        if won: wins += 1
        else: losses += 1
        bal += pnl
        pnl_total += pnl
        if bal <= 0: break
    n = wins + losses
    return {"trades": n, "wins": wins, "wr": round(wins/n*100,1) if n else 0,
            "pnl": round(pnl_total,2), "roi": round((bal-capital)/capital*100,1),
            "final": round(bal,2)}

# ━━━ Experiments ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    print("Loading data...")
    markets = load_data()
    print(f"Markets with trade data + resolution: {len(markets)}")

    for m in markets:
        m["category"] = classify(m["q_lower"])

    cat_counts = defaultdict(int)
    for m in markets: cat_counts[m["category"]] += 1
    print(f"Categories: {dict(sorted(cat_counts.items(), key=lambda x:-x[1]))}")

    results = []

    def add(name, r):
        r["name"] = name
        results.append(r)

    # ─── 1. TEMPORAL ENTRY TIMING ────────────────────────────────────────
    print("\n[1] Temporal entry timing...")
    for side in ["YES", "NO"]:
        for ef in ["early_yes", "mid_window_yes", "late_yes", "mid_yes"]:
            label = ef.replace("_yes","").replace("_"," ")
            add(f"timing_{label}_{side}", sim(markets, side, ef))

    # Per category
    for cat in cat_counts:
        if cat_counts[cat] < 10: continue
        cm = [m for m in markets if m["category"] == cat]
        for side in ["YES", "NO"]:
            for ef in ["early_yes", "late_yes"]:
                label = ef.replace("_yes","")
                add(f"timing_{cat}_{label}_{side}", sim(cm, side, ef))

    # ─── 2. PORTFOLIO DIVERSIFICATION Monte Carlo ────────────────────────
    print("[2] Monte Carlo diversification...")
    mc_results = {}
    for n_markets in [5, 10, 20, 50, 100]:
        for side in ["YES", "NO"]:
            rois = []
            for trial in range(500):
                sample = random.sample(markets, min(n_markets, len(markets)))
                r = sim(sample, side, "mid_yes", size=20)
                if r["trades"] > 0:
                    rois.append(r["roi"])
            if rois:
                key = f"mc_{n_markets}mkt_{side}"
                mc_results[key] = {
                    "mean_roi": round(np.mean(rois), 2),
                    "median_roi": round(np.median(rois), 2),
                    "std_roi": round(np.std(rois), 2),
                    "pct_profitable": round(sum(1 for r in rois if r > 0) / len(rois) * 100, 1),
                    "p5": round(np.percentile(rois, 5), 2),
                    "p95": round(np.percentile(rois, 95), 2),
                    "min": round(min(rois), 2),
                    "max": round(max(rois), 2),
                    "n_trials": len(rois),
                }
                add(key, {"trades": n_markets, "wins": 0,
                          "wr": mc_results[key]["pct_profitable"],
                          "pnl": mc_results[key]["mean_roi"] * 10,  # approx
                          "roi": mc_results[key]["mean_roi"],
                          "final": round(1000 + mc_results[key]["mean_roi"]*10, 2)})

    # ─── 3. MARKET AGE FILTER ───────────────────────────────────────────
    print("[3] Market age filter...")
    for min_age in [0, 24, 72, 168, 336]:
        aged = [m for m in markets if m["age_hours"] >= min_age]
        for side in ["YES", "NO"]:
            add(f"age_gt{min_age}h_{side}", sim(aged, side))

    # ─── 4. VOLUME CONCENTRATION ─────────────────────────────────────────
    print("[4] Volume concentration...")
    med_t5 = statistics.median([m["top5_share"] for m in markets])
    conc_high = [m for m in markets if m["top5_share"] > med_t5]
    conc_low = [m for m in markets if m["top5_share"] <= med_t5]
    for side in ["YES", "NO"]:
        add(f"conc_high_{side}", sim(conc_high, side))
        add(f"conc_low_{side}", sim(conc_low, side))

    # ─── 5. PRICE TRAJECTORY ────────────────────────────────────────────
    print("[5] Price trajectory...")
    for slope_sign in ["up", "down"]:
        filtered = [m for m in markets if (m["slope"] > 0.05 if slope_sign == "up" else m["slope"] < -0.05)]
        for side in ["YES", "NO"]:
            add(f"slope_{slope_sign}_{side}", sim(filtered, side))

    # Momentum: trade WITH slope
    momentum = [m for m in markets if m["slope"] > 0.05]
    add("momentum_buy_yes", sim(momentum, "YES"))
    reversal = [m for m in markets if m["slope"] > 0.05]
    add("momentum_sell_no", sim(reversal, "NO"))
    # Mean reversion: trade AGAINST slope
    up_slope = [m for m in markets if m["slope"] > 0.1]
    add("mean_rev_slope_up_NO", sim(up_slope, "NO"))
    down_slope = [m for m in markets if m["slope"] < -0.1]
    add("mean_rev_slope_down_YES", sim(down_slope, "YES"))

    # ─── 6. VOLATILITY REGIME ───────────────────────────────────────────
    print("[6] Volatility regime...")
    vols = [m["vol_std"] for m in markets if m["vol_std"] > 0]
    if vols:
        v33, v66 = np.percentile(vols, 33), np.percentile(vols, 66)
        for label, filt in [
            ("low_vol", lambda m: 0 < m["vol_std"] <= v33),
            ("med_vol", lambda m: v33 < m["vol_std"] <= v66),
            ("high_vol", lambda m: m["vol_std"] > v66),
        ]:
            filtered = [m for m in markets if filt(m)]
            for side in ["YES", "NO"]:
                add(f"{label}_{side}", sim(filtered, side))

    # ─── 7. BID-ASK SPREAD IMPACT (CRITICAL) ────────────────────────────
    print("[7] Spread impact...")
    for spread_pct in [0.0, 0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.10]:
        for side in ["YES", "NO"]:
            add(f"spread_{int(spread_pct*100)}pct_{side}",
                sim(markets, side, spread=spread_pct))
        # Category-specific with spread
        for cat in ["fed_rate", "fdv_above", "election", "sports", "trump"]:
            cm = [m for m in markets if m["category"] == cat]
            if len(cm) < 5: continue
            add(f"spread_{int(spread_pct*100)}pct_{cat}_NO",
                sim(cm, "NO", spread=spread_pct))

    # ─── 8. COMBINED STRATEGIES ──────────────────────────────────────────
    print("[8] Combined strategies...")
    combos = [
        ("NO_vol50k_lowvol", lambda m: m["volume"]>=50000 and m["vol_std"]<(v33 if vols else 99), "NO"),
        ("NO_vol100k_slopeDown", lambda m: m["volume"]>=100000 and m["slope"]<-0.05, "NO"),
        ("NO_early_hightrades", lambda m: m["n_trades"]>=20 and m["age_hours"]>24, "NO"),
        ("YES_highvol_late", lambda m: m["vol_std"]>(v66 if vols else 0), "YES"),
        ("NO_fed_vol50k", lambda m: m["category"]=="fed_rate" and m["volume"]>=50000, "NO"),
        ("NO_fdv_slope_down", lambda m: m["category"]=="fdv_above" and m["slope"]<0, "NO"),
        ("NO_election_old", lambda m: m["category"]=="election" and m["age_hours"]>72, "NO"),
        ("NO_trump_conc_low", lambda m: m["category"]=="trump" and m["top5_share"]<med_t5, "NO"),
        ("NO_not_sports", lambda m: m["category"]!="sports", "NO"),
        ("NO_not_sports_vol50k", lambda m: m["category"]!="sports" and m["volume"]>=50000, "NO"),
        ("NO_cheap_entry", lambda m: (1-m["mid_yes"]) < 0.30, "NO"),
        ("NO_cheap_entry_not_sports", lambda m: (1-m["mid_yes"])<0.30 and m["category"]!="sports", "NO"),
        ("YES_cheap_entry", lambda m: m["mid_yes"] < 0.30, "YES"),
        ("YES_crypto_up_slope", lambda m: m["category"]=="crypto_target" and m["slope"]>0, "YES"),
        ("NO_geo_old", lambda m: m["category"]=="geopolitics" and m["age_hours"]>48, "NO"),
    ]
    for name, filt, side in combos:
        filtered = [m for m in markets if filt(m)]
        if len(filtered) >= 3:
            add(name, sim(filtered, side))
            # Also with spread
            for sp in [0.02, 0.05]:
                add(f"{name}_sp{int(sp*100)}pct", sim(filtered, side, spread=sp))

    # ─── 9. KELLY CRITERION Monte Carlo ──────────────────────────────────
    print("[9] Kelly criterion Monte Carlo...")
    for cat in cat_counts:
        if cat_counts[cat] < 15: continue
        cm = [m for m in markets if m["category"] == cat]
        no_rate = sum(1 for m in cm if not m["yes_won"]) / len(cm)

        for kelly_frac_name, kelly_mult in [("full", 1.0), ("half", 0.5), ("quarter", 0.25)]:
            rois = []
            for trial in range(200):
                random.shuffle(cm)
                bal = 1000.0
                for m in cm:
                    entry = 1.0 - m["mid_yes"]
                    if entry <= 0.01 or entry >= 0.99: continue
                    b = (1-entry)/entry
                    f = (no_rate * b - (1-no_rate)) / b
                    f = max(0, min(f, 0.20)) * kelly_mult
                    ts = f * bal
                    if ts < 1: continue
                    shares = ts / entry
                    if not m["yes_won"]:
                        bal += shares - ts
                    else:
                        bal -= ts
                    if bal <= 0: break
                rois.append((bal - 1000) / 1000 * 100)

            if rois:
                add(f"kelly_{kelly_frac_name}_{cat}_NO", {
                    "trades": len(cm), "wins": 0,
                    "wr": round(sum(1 for r in rois if r > 0)/len(rois)*100, 1),
                    "pnl": round(np.mean(rois)*10, 2),
                    "roi": round(np.mean(rois), 2),
                    "final": round(1000 + np.mean(rois)*10, 2),
                    "mc_std": round(np.std(rois), 2),
                    "mc_p5": round(np.percentile(rois, 5), 2),
                    "mc_p95": round(np.percentile(rois, 95), 2),
                })

    # ─── 10. ENTRY PRICE EDGE ────────────────────────────────────────────
    print("[10] Entry price edge buckets...")
    for side in ["YES", "NO"]:
        for edge_min, edge_max in [(0, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 0.50)]:
            def filt(m, emin=edge_min, emax=edge_max, s=side):
                if s == "NO":
                    entry = 1 - m["mid_yes"]
                    # edge = expected profit rate if always wins
                    edge = (1/entry - 1) if entry > 0 else 0
                else:
                    entry = m["mid_yes"]
                    edge = (1/entry - 1) if entry > 0 else 0
                return emin <= edge < emax
            filtered = [m for m in markets if filt(m)]
            if len(filtered) >= 3:
                add(f"edge_{int(edge_min*100)}_{int(edge_max*100)}pct_{side}", sim(filtered, side))

    # ─── 11. NET FLOW STRATEGIES ─────────────────────────────────────────
    print("[11] Net flow strategies...")
    for thresh in [0.2, 0.3, 0.5]:
        # Follow: buy what whales buy
        follow_buy = [m for m in markets if m["net_flow"] > thresh]
        add(f"follow_flow_gt{thresh}_YES", sim(follow_buy, "YES"))
        # Fade: sell against whales
        add(f"fade_flow_gt{thresh}_NO", sim(follow_buy, "NO"))
        # Follow sellers
        follow_sell = [m for m in markets if m["net_flow"] < -thresh]
        add(f"follow_flow_lt{-thresh}_NO", sim(follow_sell, "NO"))
        add(f"fade_flow_lt{-thresh}_YES", sim(follow_sell, "YES"))

    # ─── 12. TRADE COUNT LIQUIDITY ───────────────────────────────────────
    print("[12] Liquidity filter...")
    for min_t in [5, 10, 20, 50, 100]:
        liq = [m for m in markets if m["n_trades"] >= min_t]
        for side in ["YES", "NO"]:
            add(f"trades_gt{min_t}_{side}", sim(liq, side))

    # ─── RESULTS ─────────────────────────────────────────────────────────
    results.sort(key=lambda r: -r["roi"])
    profitable = [r for r in results if r["pnl"] > 0 and r["trades"] >= 5]
    losing = [r for r in results if r["pnl"] <= 0 and r["trades"] >= 5]
    with_trades = [r for r in results if r["trades"] >= 5]

    print(f"\n{'='*110}")
    print(f"TOP 30 PROFITABLE ({len(profitable)} of {len(with_trades)} with 5+ trades)")
    print(f"{'='*110}")
    print(f"{'#':>3} {'ROI%':>8} {'PnL$':>9} {'Trades':>6} {'WR%':>5} {'Final$':>9}  Strategy")
    print("-"*110)
    for i, r in enumerate(profitable[:30], 1):
        extra = ""
        if "mc_std" in r:
            extra = f"  [MC std={r['mc_std']:.1f}%, p5={r['mc_p5']:.1f}%, p95={r['mc_p95']:.1f}%]"
        print(f"{i:>3} {r['roi']:>+7.1f}% ${r['pnl']:>+8.0f} {r['trades']:>6} "
              f"{r['wr']:>4.0f}% ${r['final']:>8.0f}  {r['name']}{extra}")

    print(f"\n{'='*110}")
    print(f"WORST 15")
    print(f"{'='*110}")
    losing.sort(key=lambda r: r["roi"])
    for i, r in enumerate(losing[:15], 1):
        print(f"{i:>3} {r['roi']:>+7.1f}% ${r['pnl']:>+8.0f} {r['trades']:>6} "
              f"{r['wr']:>4.0f}% ${r['final']:>8.0f}  {r['name']}")

    # ─── SPREAD BREAKEVEN ────────────────────────────────────────────────
    print(f"\n{'='*110}")
    print("SPREAD IMPACT ON NO-ALL STRATEGY")
    print(f"{'='*110}")
    print(f"{'Spread':>8} {'ROI%':>8} {'PnL$':>9} {'WR%':>5}")
    for r in results:
        if r["name"].startswith("spread_") and r["name"].endswith("_NO") and "pct_" not in r["name"].split("spread_")[1].split("_NO")[0] + "X":
            # Only the "all markets" spread experiments
            parts = r["name"].replace("spread_","").replace("_NO","")
            if parts.endswith("pct") and "_" not in parts:
                print(f"{parts:>8} {r['roi']:>+7.1f}% ${r['pnl']:>+8.0f} {r['wr']:>4.0f}%")

    # ─── MONTE CARLO DIVERSIFICATION ─────────────────────────────────────
    print(f"\n{'='*110}")
    print("MONTE CARLO DIVERSIFICATION")
    print(f"{'='*110}")
    print(f"{'Strategy':<25} {'MeanROI':>8} {'MedROI':>8} {'Std':>7} {'%Prof':>6} {'P5':>8} {'P95':>8}")
    for k, v in sorted(mc_results.items()):
        print(f"{k:<25} {v['mean_roi']:>+7.1f}% {v['median_roi']:>+7.1f}% "
              f"{v['std_roi']:>6.1f}% {v['pct_profitable']:>5.1f}% "
              f"{v['p5']:>+7.1f}% {v['p95']:>+7.1f}%")

    # ─── SUMMARY ─────────────────────────────────────────────────────────
    print(f"\n{'='*110}")
    print(f"SUMMARY: {len(results)} experiments, {len(with_trades)} with 5+ trades")
    if with_trades:
        print(f"  Profitable: {len(profitable)} ({len(profitable)/len(with_trades)*100:.0f}%)")
    if profitable:
        print(f"  Best: {profitable[0]['name']} — ROI {profitable[0]['roi']:+.1f}%, {profitable[0]['trades']} trades")
    if losing:
        print(f"  Worst: {losing[0]['name']} — ROI {losing[0]['roi']:+.1f}%, {losing[0]['trades']} trades")

    # Save
    out = {"total": len(results), "profitable": len(profitable),
           "mc_diversification": mc_results, "results": results}
    with open("bot-data/experiments_v3_results.json", "w") as f:
        json.dump(out, f, indent=1)
    print(f"\nSaved to bot-data/experiments_v3_results.json")


if __name__ == "__main__":
    main()
