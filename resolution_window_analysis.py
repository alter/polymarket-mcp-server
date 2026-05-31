#!/usr/bin/env python3
"""
Resolution window analysis — what happens to prices in last X hours before
close? Test:
1. Late-stage convergence: do prices in [0.85, 0.95] WIN at >85%?
2. Late-stage overshoot: do prices that move sharply in last 1h reverse?
3. Late-stage gap: ask-bid spread before close — wide or tight?
4. Last-tick anchoring: what fraction of final mid > closing implied?
"""
import json, sys
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
sys.path.insert(0, ".")
from mass_backtest import load_resolutions, load_ticks, BET_USD


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading...")
    res = load_resolutions()
    ticks = load_ticks()
    paired = {m: t for m, t in ticks.items() if m in res}
    print(f"  {len(paired)} resolved markets with ticks\n")

    # === 1. Late-stage convergence by window ===
    print("━━━ 1. WINDOW BEFORE CLOSE — does price predict outcome? ━━━")
    for window_h in [1, 3, 6, 12, 24]:
        # For each market, get last tick in [close-window, close]
        bins = defaultdict(lambda: {"n": 0, "yes": 0, "actual_yes": 0})
        for mid, mkt in paired.items():
            close_ts = res[mid]["close_ts"]
            yes_won = res[mid]["yes_won"]
            ts_arr = mkt["ts"]
            mid_arr = mkt["mid"]
            window_start = close_ts - window_h * 3600
            mask_in_window = (ts_arr >= window_start) & (ts_arr <= close_ts)
            if not mask_in_window.any():
                continue
            # Last mid in window
            last_idx = np.where(mask_in_window)[0][-1]
            last_mid = mid_arr[last_idx]
            # Bin
            for lo, hi, label in [(0.0, 0.05, "0-5"), (0.05, 0.20, "5-20"),
                                  (0.20, 0.50, "20-50"), (0.50, 0.80, "50-80"),
                                  (0.80, 0.95, "80-95"), (0.95, 1.01, "95-100")]:
                if lo <= last_mid < hi:
                    bins[label]["n"] += 1
                    if yes_won:
                        bins[label]["actual_yes"] += 1
                    break
        print(f"  Window {window_h}h:")
        for lbl in ["0-5", "5-20", "20-50", "50-80", "80-95", "95-100"]:
            b = bins.get(lbl, {"n": 0, "actual_yes": 0})
            if b["n"] >= 3:
                rate = b["actual_yes"] / b["n"] * 100
                print(f"    [{lbl:<6}] n={b['n']:>3} actual_yes={rate:>5.1f}%")

    # === 2. Late-stage overshoot test ===
    print(f"\n━━━ 2. OVERSHOOT TEST — sharp moves in last 1h reverse? ━━━")
    # For each market: mid at -1h vs mid at close. If big move, what's outcome?
    # Strategy: bet OPPOSITE the late move (fade overshoot)
    for window_h in [0.5, 1, 2]:
        moves = []
        for mid, mkt in paired.items():
            close_ts = res[mid]["close_ts"]
            yes_won = res[mid]["yes_won"]
            ts_arr = mkt["ts"]
            mid_arr = mkt["mid"]
            t1 = close_ts - window_h * 3600
            i1 = np.searchsorted(ts_arr, t1, side="right") - 1
            if i1 < 0 or i1 >= len(ts_arr):
                continue
            i_end = np.where(ts_arr <= close_ts)[0]
            if len(i_end) == 0:
                continue
            i_end = i_end[-1]
            if i_end <= i1:
                continue
            p_start = mid_arr[i1]
            p_end = mid_arr[i_end]
            if p_start <= 0 or p_end <= 0:
                continue
            delta = p_end - p_start
            moves.append((delta, p_end, yes_won))
        # Bin by delta
        rising = [(d, p, w) for d, p, w in moves if d > 0.05]
        falling = [(d, p, w) for d, p, w in moves if d < -0.05]
        flat = [(d, p, w) for d, p, w in moves if abs(d) <= 0.05]
        for grp_name, grp in [("rising>5%", rising), ("falling<-5%", falling), ("flat", flat)]:
            if not grp:
                continue
            n = len(grp)
            yes_rate = sum(1 for d, p, w in grp if w) / n * 100
            avg_p = np.mean([p for d, p, w in grp])
            # If we bet OPPOSITE direction at p_end:
            # rising → bet NO at 1-p_end-spread, win if !yes_won
            # falling → bet YES at p_end+spread, win if yes_won
            # Approximate (no slippage):
            if grp_name.startswith("rising"):
                # bet NO; entry ≈ 1 - avg_p; win iff actual NO; pnl = (1/(1-avg_p) - 1) if win else -1
                no_actual = 1 - yes_rate / 100
                no_entry = 1 - avg_p
                if no_entry > 0:
                    no_pnl_per_bet = (no_actual / no_entry - 1) * 100
                else:
                    no_pnl_per_bet = 0
                print(f"  Window {window_h}h {grp_name:<12} n={n:>3} actual_yes={yes_rate:>5.1f}% avg_end_p={avg_p:.3f} BET_NO_ROI={no_pnl_per_bet:+5.1f}%")
            elif grp_name.startswith("falling"):
                yes_entry = avg_p
                yes_pnl_per_bet = (yes_rate/100 / yes_entry - 1) * 100 if yes_entry > 0 else 0
                print(f"  Window {window_h}h {grp_name:<12} n={n:>3} actual_yes={yes_rate:>5.1f}% avg_end_p={avg_p:.3f} BET_YES_ROI={yes_pnl_per_bet:+5.1f}%")
            else:
                print(f"  Window {window_h}h {grp_name:<12} n={n:>3} actual_yes={yes_rate:>5.1f}% avg_end_p={avg_p:.3f}")

    # === 3. Late-stage spread analysis ===
    print(f"\n━━━ 3. SPREAD WIDENING IN LAST 1H? ━━━")
    spread_data = []
    for mid, mkt in paired.items():
        close_ts = res[mid]["close_ts"]
        ts_arr = mkt["ts"]
        bid_arr = mkt["bid"]
        ask_arr = mkt["ask"]
        mid_arr = mkt["mid"]
        # Mid window
        i_mid_start = np.searchsorted(ts_arr, close_ts - 12 * 3600, side="right")
        i_mid_end = np.searchsorted(ts_arr, close_ts - 1 * 3600, side="right")
        # Late window
        i_late = np.searchsorted(ts_arr, close_ts - 1 * 3600, side="right")
        i_close = np.where(ts_arr <= close_ts)[0]
        if len(i_close) == 0: continue
        i_close = i_close[-1]
        if i_mid_end <= i_mid_start or i_close <= i_late:
            continue
        mid_spread = np.mean((ask_arr[i_mid_start:i_mid_end] - bid_arr[i_mid_start:i_mid_end]) / np.maximum(mid_arr[i_mid_start:i_mid_end], 0.01))
        late_spread = np.mean((ask_arr[i_late:i_close+1] - bid_arr[i_late:i_close+1]) / np.maximum(mid_arr[i_late:i_close+1], 0.01))
        if 0 < mid_spread < 1 and 0 < late_spread < 1:
            spread_data.append((mid_spread, late_spread))
    if spread_data:
        m = np.mean([x[0] for x in spread_data])
        l = np.mean([x[1] for x in spread_data])
        print(f"  Markets analyzed: {len(spread_data)}")
        print(f"  Mid-window spread (12h-1h before close): {m*100:.2f}%")
        print(f"  Late-window spread (last 1h before close): {l*100:.2f}%")
        print(f"  Delta: {(l-m)*100:+.2f}pp ({'widens' if l > m else 'tightens'})")


if __name__ == "__main__":
    main()
