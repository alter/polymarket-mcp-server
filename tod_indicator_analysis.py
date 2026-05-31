#!/usr/bin/env python3
"""
Time-of-day × indicator interaction. For each top variant, check whether
performance differs across UTC hour buckets.
"""
import json, sys
from collections import defaultdict
from datetime import datetime, timezone
import numpy as np
sys.path.insert(0, ".")
from mass_backtest import (
    BET_USD, SLIPPAGE, PRICE_FILTERS, FEES_FILTERS, SPREAD_FILTERS,
    price_mask, spread_mask, generate_primitive_signals,
    load_resolutions, load_ticks,
)


# Top variants from prior backtest
TOP = [
    "BO_p10_fade|pgt30|sany|fany",
    "BO_p100_follow|plt70|swide|fany",
    "RS_p14_t65_follow|pgt30|sany|fany",
    "RS_p14_t75_follow|pgt30|sany|fany",
    "BB_p20_sd25_fade|plt30|sany|fany",
]

TOD_BINS = [(0, 8, "Asia"), (8, 14, "Europe"), (14, 22, "US"), (22, 24, "Off")]


def main():
    res = load_resolutions()
    ticks = load_ticks()
    paired = {m: t for m, t in ticks.items() if m in res}

    print(f"\n━━━ ToD × indicator (per-bet ROI by entry hour) ━━━\n")

    for var_key in TOP:
        parts = var_key.split("|")
        prim = parts[0]
        pf = parts[1][1:]; sf = parts[2][1:]; ff = parts[3][1:]

        # accumulator per ToD bucket
        bucket = defaultdict(lambda: {"n": 0, "wins": 0, "pnl": 0.0})

        for mid, mkt in paired.items():
            yes_won = res[mid]["yes_won"]
            close_ts = res[mid]["close_ts"]
            mids = mkt["mid"]; bids = mkt["bid"]; asks = mkt["ask"]; ts_arr = mkt["ts"]
            fees_on = mkt["fees"]
            if ff == "free_only" and fees_on:
                continue
            sigs = generate_primitive_signals(mids, ts_arr, bids, asks)
            if prim not in sigs: continue
            s = sigs[prim]
            pf_m = price_mask(mids, pf); sf_m = spread_mask(mids, bids, asks, sf)
            active = (s != 0) & pf_m & sf_m & (ts_arr <= close_ts)
            if not active.any(): continue
            indices = np.where(active)[0]
            kept = []
            last_t = -1e9
            for idx in indices:
                if ts_arr[idx] - last_t >= 60:
                    kept.append(idx); last_t = ts_arr[idx]
            if not kept: continue
            for idx in kept:
                sig = s[idx]
                hr = datetime.fromtimestamp(ts_arr[idx], tz=timezone.utc).hour
                tod_label = "Off"
                for lo, hi, label in TOD_BINS:
                    if lo <= hr < hi:
                        tod_label = label; break
                if sig == 1:
                    entry = asks[idx] * (1 + SLIPPAGE)
                else:
                    entry = (1 - bids[idx]) * (1 + SLIPPAGE)
                if entry < 0.05 or entry > 0.95: continue
                won = ((sig == 1) and yes_won) or ((sig == -1) and not yes_won)
                pnl = (BET_USD * (1.0/entry - 1)) if won else -BET_USD
                bucket[tod_label]["n"] += 1
                if won:
                    bucket[tod_label]["wins"] += 1
                bucket[tod_label]["pnl"] += pnl

        print(f"  {var_key}")
        for label in ["Asia", "Europe", "US", "Off"]:
            b = bucket.get(label, {"n": 0})
            if b.get("n", 0) >= 50:
                wr = b["wins"]/b["n"]*100
                roi = b["pnl"]/(b["n"]*BET_USD)*100
                print(f"    [{label:<6}] n={b['n']:>5} WR={wr:>5.1f}% ROI={roi:>+6.1f}%")
        print()


if __name__ == "__main__":
    main()
