#!/usr/bin/env python3
"""
Theta-decay test: late-stage limit orders at extreme prices.

Hypothesis: markets at price > 0.95 in last X hours before close are essentially
resolved. Limit-buy YES at 0.97-0.99 captures spread on stragglers.

For each resolved market:
- Find ticks in last_h hours with mid > price_threshold
- Simulate limit-buy YES at limit_price (would fill if ask <= limit_price)
- If filled, hold to resolution. PnL = +1/limit_price - 1 if YES wins, else -1.
"""
import json
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
import sys
sys.path.insert(0, ".")
from mass_backtest import load_resolutions, load_ticks, BET_USD


CONFIGS = [
    # (last_h, price_thr, limit_p, side)
    (24, 0.90, 0.95, "YES"),
    (24, 0.95, 0.98, "YES"),
    (12, 0.95, 0.99, "YES"),
    (6, 0.95, 0.99, "YES"),
    (3, 0.95, 0.99, "YES"),
    # Inverse — extreme low
    (24, 0.10, 0.05, "NO"),
    (12, 0.05, 0.02, "NO"),
    (6, 0.05, 0.02, "NO"),
]


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading...")
    res = load_resolutions()
    ticks = load_ticks()
    print(f"  {len(ticks)} markets")

    print(f"\n━━━ Theta decay limit-order test ━━━")
    print(f"  {'config':<35} {'fills':>6} {'wins':>5} {'pnl':>9} {'ROI':>7}")

    out = []
    for last_h, price_thr, limit_p, side in CONFIGS:
        n_fills = 0
        n_wins = 0
        total_pnl = 0.0
        for mid, mkt in ticks.items():
            if mid not in res:
                continue
            yes_won = res[mid]["yes_won"]
            close_ts = res[mid]["close_ts"]
            ts_arr = mkt["ts"]
            mid_arr = mkt["mid"]
            bid_arr = mkt["bid"]
            ask_arr = mkt["ask"]
            window_start = close_ts - last_h * 3600
            i_start = np.searchsorted(ts_arr, window_start, side="left")
            i_end = np.searchsorted(ts_arr, close_ts, side="right") - 1
            if i_start >= len(ts_arr) or i_end <= i_start:
                continue
            # Find first tick where our limit order would fill
            for i in range(i_start, i_end + 1):
                m = mid_arr[i]
                if side == "YES":
                    if m < price_thr:
                        continue
                    # Limit-buy YES at limit_p — fills if ask <= limit_p
                    if ask_arr[i] <= limit_p:
                        # Filled at the best ask price (slightly below or = limit)
                        entry = ask_arr[i]
                        won = yes_won
                        pnl = (BET_USD * (1.0/entry - 1)) if won else -BET_USD
                        n_fills += 1
                        if won: n_wins += 1
                        total_pnl += pnl
                        break
                else:  # NO
                    if m > price_thr:
                        continue
                    # Limit-buy NO at (1-bid <= limit_p) i.e. bid >= 1-limit_p
                    if (1 - bid_arr[i]) <= limit_p:
                        entry = 1 - bid_arr[i]
                        won = not yes_won
                        pnl = (BET_USD * (1.0/entry - 1)) if won else -BET_USD
                        n_fills += 1
                        if won: n_wins += 1
                        total_pnl += pnl
                        break
        if n_fills > 0:
            wr = n_wins / n_fills * 100
            roi = total_pnl / (n_fills * BET_USD) * 100
        else:
            wr = roi = 0
        cfg = f"{last_h}h {side} p>{price_thr} limit{limit_p}"
        out.append({
            "config": cfg,
            "fills": n_fills, "wins": n_wins,
            "pnl": round(total_pnl, 4),
            "roi_pct": round(roi, 2),
            "wr": round(wr, 2),
        })
        print(f"  {cfg:<35} {n_fills:>6} {n_wins:>5} ${total_pnl:>+7.4f} {roi:>+6.1f}%")

    # Save
    with open("bot-data/theta_decay_results.json", "w") as f:
        json.dump({
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "configs": out,
        }, f, indent=2)
    print(f"\nSaved to bot-data/theta_decay_results.json")


if __name__ == "__main__":
    main()
