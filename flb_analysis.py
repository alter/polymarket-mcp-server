#!/usr/bin/env python3
"""
Favorite-Longshot Bias (FLB) on Polymarket.

For each resolved market: take last mid before close_ts, compare to actual
resolution outcome. Bin by closing price level. Compute realized win rate
per bin. Bias exists when realized != implied.

Output: bot-data/flb_results.json
Strategy: systematically bet on/against bias direction.
"""
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import sys
sys.path.insert(0, ".")
from mass_backtest import load_resolutions, load_ticks, BET_USD


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading...")
    res = load_resolutions()
    ticks = load_ticks()
    n_markets = 0
    samples = []  # (final_mid, yes_won, fees_on, market_id)

    for mid, mkt in ticks.items():
        if mid not in res:
            continue
        rr = res[mid]
        close_ts = rr["close_ts"]
        # Last tick before close
        ts_arr = mkt["ts"]
        idx = np.searchsorted(ts_arr, close_ts, side="right") - 1
        if idx < 0 or idx >= len(mkt["mid"]):
            continue
        final_mid = mkt["mid"][idx]
        samples.append((final_mid, rr["yes_won"], mkt["fees"], mid))
        n_markets += 1
    print(f"  {n_markets} markets with final mid")

    # Bin by final price
    BINS = [(0.00, 0.05), (0.05, 0.15), (0.15, 0.30), (0.30, 0.50),
            (0.50, 0.70), (0.70, 0.85), (0.85, 0.95), (0.95, 1.00)]
    bin_stats = []
    for lo, hi in BINS:
        in_bin = [(m, w, f, mid) for m, w, f, mid in samples if lo <= m < hi]
        if not in_bin:
            bin_stats.append({"range": f"[{lo:.2f},{hi:.2f})", "n": 0})
            continue
        n = len(in_bin)
        wins = sum(1 for _, w, _, _ in in_bin if w)
        actual_yes = wins / n
        avg_implied = np.mean([m for m, _, _, _ in in_bin])
        # Bet pnl: betting YES at avg_implied, win = 1, lose = 0
        # ROI per bet (no slippage): (actual_yes / avg_implied - 1)
        bet_yes_roi = (actual_yes / avg_implied - 1) * 100 if avg_implied > 0 else 0
        # Bet NO at (1 - avg_implied), shares = 1/(1-avg_implied), win iff !yes_won
        actual_no = 1 - actual_yes
        no_entry = 1 - avg_implied
        bet_no_roi = (actual_no / no_entry - 1) * 100 if no_entry > 0 else 0
        bin_stats.append({
            "range": f"[{lo:.2f},{hi:.2f})",
            "n": n,
            "avg_implied": round(avg_implied, 4),
            "actual_yes_rate": round(actual_yes, 4),
            "delta_pp": round((actual_yes - avg_implied) * 100, 2),
            "bet_yes_roi_pct": round(bet_yes_roi, 2),
            "bet_no_roi_pct": round(bet_no_roi, 2),
        })

    # Fee-aware split
    fee_split = {"free": [], "fees": []}
    for m, w, f, mid in samples:
        fee_split["fees" if f else "free"].append((m, w))

    fee_table = {}
    for tag, vals in fee_split.items():
        if not vals: continue
        fee_table[tag] = {"n": len(vals), "wr": sum(1 for _, w in vals if w) / len(vals) * 100}

    # Save
    out = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "n_markets": n_markets,
        "bins": bin_stats,
        "by_fees": fee_table,
        "overall_yes_rate": round(sum(1 for _, w, _, _ in samples if w) / len(samples) * 100, 2),
    }
    with open("bot-data/flb_results.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"\nOverall YES rate: {out['overall_yes_rate']}%")
    print(f"Free vs Fees: {fee_table}")
    print(f"\n━━━ FLB by closing price bin ━━━")
    print(f"  {'range':<14} {'n':>4} {'implied':>9} {'actual':>8} {'Δpp':>7} "
          f"{'YES ROI':>8} {'NO ROI':>8}")
    for b in bin_stats:
        if b["n"] == 0:
            print(f"  {b['range']:<14} —")
            continue
        print(f"  {b['range']:<14} {b['n']:>4} "
              f"{b['avg_implied']:>9.4f} {b['actual_yes_rate']:>8.4f} "
              f"{b['delta_pp']:>+6.1f}pp {b['bet_yes_roi_pct']:>+7.1f}% "
              f"{b['bet_no_roi_pct']:>+7.1f}%")


if __name__ == "__main__":
    main()
