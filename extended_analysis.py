#!/usr/bin/env python3
"""
Multi-dimensional analysis on 273 resolved markets:
1. End-of-life (EOL) — bin entries by hours-to-resolution
2. Resolution-day momentum reversal — late sharp moves often overshoot
3. Time-of-day (TOD) — UTC hour effects
4. Tick-density proxy for volume — high-activity markets

For each: identify if edge concentrated in specific buckets.
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
    out = {"ran_at": datetime.now(timezone.utc).isoformat()}

    # === 1. EOL bins (hours from entry to resolution) ===
    print("\n━━━ 1. End-of-life bins (entries vs hours-to-resolution) ━━━")
    EOL_BINS = [(0, 6), (6, 24), (24, 72), (72, 168), (168, 1e9)]  # h
    eol_stats = []
    for lo, hi in EOL_BINS:
        n_total = 0
        n_yes_won = 0
        prices = []
        for mid, mkt in ticks.items():
            if mid not in res:
                continue
            close_ts = res[mid]["close_ts"]
            yes_won = res[mid]["yes_won"]
            ts_arr = mkt["ts"]
            mid_arr = mkt["mid"]
            for i in range(len(ts_arr)):
                hours_to = (close_ts - ts_arr[i]) / 3600
                if lo <= hours_to < hi:
                    n_total += 1
                    if yes_won:
                        n_yes_won += 1
                    prices.append(mid_arr[i])
        if n_total > 0:
            avg_p = np.mean(prices)
            actual_y = n_yes_won / n_total
            # If you bet YES at avg_p, ROI = (actual_y/avg_p - 1)
            yes_roi = (actual_y / avg_p - 1) * 100 if avg_p > 0 else 0
            no_entry = 1 - avg_p
            no_roi = ((1-actual_y) / no_entry - 1) * 100 if no_entry > 0 else 0
            eol_stats.append({
                "bin": f"{lo}-{hi}h", "n_ticks": n_total,
                "avg_price": round(avg_p, 4),
                "actual_yes_rate": round(actual_y, 4),
                "yes_roi_pct": round(yes_roi, 2),
                "no_roi_pct": round(no_roi, 2),
            })
            print(f"  {lo}-{hi}h: n={n_total:>7,} avg_p={avg_p:.4f} actual={actual_y:.4f} "
                  f"YES_ROI={yes_roi:+5.1f}% NO_ROI={no_roi:+5.1f}%")
    out["eol"] = eol_stats

    # === 2. Resolution-day momentum reversal ===
    print("\n━━━ 2. Last-N-hour momentum (mid change in final window) ━━━")
    # For each market: take last_N_hours window before close. Compute mid change.
    # Test: did the late-window movers reverse before resolution?
    LATE_WINDOWS = [3, 6, 12, 24]  # hours
    momentum_stats = {}
    for window_h in LATE_WINDOWS:
        # Bin late-window mid changes
        deltas_won = []  # (delta, yes_won)
        for mid, mkt in ticks.items():
            if mid not in res:
                continue
            close_ts = res[mid]["close_ts"]
            yes_won = res[mid]["yes_won"]
            ts_arr = mkt["ts"]
            mid_arr = mkt["mid"]
            window_start = close_ts - window_h * 3600
            i_start = np.searchsorted(ts_arr, window_start, side="left")
            i_end = np.searchsorted(ts_arr, close_ts, side="right") - 1
            if i_start >= len(ts_arr) or i_end <= i_start:
                continue
            p_start = mid_arr[i_start]
            p_end = mid_arr[i_end]
            if p_start <= 0 or p_end <= 0:
                continue
            delta = p_end - p_start
            deltas_won.append((delta, yes_won, p_end))
        if not deltas_won:
            continue
        # Bucket by delta
        rising = [(d, w, p) for d, w, p in deltas_won if d > 0.05]
        falling = [(d, w, p) for d, w, p in deltas_won if d < -0.05]
        flat = [(d, w, p) for d, w, p in deltas_won if abs(d) <= 0.05]
        def stats(items, label):
            if not items: return None
            n = len(items)
            wins = sum(1 for _, w, _ in items if w)
            avg_p = np.mean([p for _, _, p in items])
            actual = wins / n
            return {
                "label": label, "n": n,
                "avg_end_price": round(avg_p, 4),
                "actual_yes_rate": round(actual, 4),
                "yes_roi": round((actual/avg_p - 1) * 100 if avg_p > 0 else 0, 2),
                "no_roi": round(((1-actual)/(1-avg_p) - 1) * 100 if avg_p < 1 else 0, 2),
            }
        rs = stats(rising, "rising>5%")
        fs = stats(falling, "falling<-5%")
        fl = stats(flat, "flat")
        momentum_stats[f"{window_h}h"] = {"rising": rs, "falling": fs, "flat": fl}
        print(f"  Window {window_h}h:")
        for grp_name, st in [("rising", rs), ("falling", fs), ("flat", fl)]:
            if st:
                print(f"    {grp_name:<13} n={st['n']:>4} end_p={st['avg_end_price']:.3f} "
                      f"actual={st['actual_yes_rate']:.3f} "
                      f"YES_ROI={st['yes_roi']:+5.1f}% NO_ROI={st['no_roi']:+5.1f}%")
    out["momentum"] = momentum_stats

    # === 3. Time-of-day ===
    print("\n━━━ 3. Time-of-day (UTC hour binned, all entries) ━━━")
    TOD_BINS = [(0, 8, "Asia"), (8, 14, "Europe"), (14, 22, "US"), (22, 24, "Off")]
    tod_stats = []
    for lo, hi, label in TOD_BINS:
        n_total = 0
        n_yes_won = 0
        prices = []
        for mid, mkt in ticks.items():
            if mid not in res:
                continue
            yes_won = res[mid]["yes_won"]
            close_ts = res[mid]["close_ts"]
            ts_arr = mkt["ts"]
            mid_arr = mkt["mid"]
            for i in range(len(ts_arr)):
                if ts_arr[i] > close_ts:
                    continue
                h = datetime.fromtimestamp(ts_arr[i], tz=timezone.utc).hour
                if lo <= h < hi:
                    n_total += 1
                    if yes_won:
                        n_yes_won += 1
                    prices.append(mid_arr[i])
        if n_total > 0:
            avg_p = np.mean(prices)
            actual = n_yes_won / n_total
            tod_stats.append({
                "bin": f"{label}({lo}-{hi}UTC)",
                "n_ticks": n_total,
                "avg_price": round(avg_p, 4),
                "actual_yes_rate": round(actual, 4),
                "yes_roi_pct": round((actual/avg_p - 1) * 100, 2),
                "no_roi_pct": round(((1-actual)/(1-avg_p) - 1) * 100 if avg_p < 1 else 0, 2),
            })
            print(f"  {label}({lo}-{hi}UTC): n={n_total:>7,} avg_p={avg_p:.4f} "
                  f"actual={actual:.4f} YES={tod_stats[-1]['yes_roi_pct']:+5.1f}% "
                  f"NO={tod_stats[-1]['no_roi_pct']:+5.1f}%")
    out["tod"] = tod_stats

    # === 4. Tick-density (volume proxy) ===
    print("\n━━━ 4. Tick-density volume proxy ━━━")
    densities = []
    for mid, mkt in ticks.items():
        if mid not in res:
            continue
        ts_arr = mkt["ts"]
        if len(ts_arr) < 10:
            continue
        span = ts_arr[-1] - ts_arr[0]
        if span < 60:
            continue
        density = len(ts_arr) / (span / 3600)  # ticks per hour
        densities.append((density, res[mid]["yes_won"], mid))
    densities.sort(key=lambda x: x[0])
    n = len(densities)
    print(f"  {n} markets, density range: {densities[0][0]:.1f}-{densities[-1][0]:.1f} ticks/h")
    # Quartile splits
    for q_lo, q_hi, label in [(0, 0.25, "Q1 lowest"), (0.25, 0.5, "Q2"),
                               (0.5, 0.75, "Q3"), (0.75, 1.0, "Q4 highest")]:
        i_lo = int(n * q_lo)
        i_hi = int(n * q_hi)
        sub = densities[i_lo:i_hi]
        if not sub:
            continue
        wr = sum(1 for _, w, _ in sub if w) / len(sub) * 100
        avg_d = np.mean([d for d, _, _ in sub])
        print(f"  {label}: n={len(sub)} avg_density={avg_d:.1f}/h yes_rate={wr:.1f}%")
    out["density"] = {
        "n_total": n,
        "min_density": round(densities[0][0], 2),
        "max_density": round(densities[-1][0], 2),
    }

    with open("bot-data/extended_analysis.json", "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved to bot-data/extended_analysis.json")


if __name__ == "__main__":
    main()
