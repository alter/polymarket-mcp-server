#!/usr/bin/env python3
"""
Win3-skip filter backtest — applies post-trade filter where if sum of last 3
closed PnLs < 0, skip the next entry signal.

Per-variant GLOBAL state (matches user spec from BTC bot):
- One deque per variant, shared across markets
- Triggered on close (exit), consumed on next entry signal
- Single skip per trigger

Output: bot-data/mass_backtest_win3.json
Compares baseline ROI vs Win3-filtered ROI for top variants.
"""
import json, time, heapq, multiprocessing as mp
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import sys
sys.path.insert(0, ".")
from mass_backtest import (
    BET_USD, SLIPPAGE,
    PRICE_FILTERS, FEES_FILTERS, SPREAD_FILTERS,
    price_mask, spread_mask,
    generate_primitive_signals,
    load_resolutions, load_ticks,
)

RESULTS_FILE = Path("bot-data/mass_backtest_win3.json")

# Test top 60 variants × 3 exit policies × {with-Win3, without}
TOP_N = 60
TP_SL_GRID = [
    (0.10, 0.20),    # rotated tp10/sl20
    (0.20, 0.50),    # original tp20/sl50
    (None, None),    # HTR
]


def simulate_one_entry(mids, ts_arr, fees_on, close_ts, yes_won,
                        eidx, side, entry, tp_pct, sl_pct):
    """Returns (exit_ts, pnl_usd)."""
    won_resolve = ((side == 1) and yes_won) or ((side == -1) and not yes_won)
    htr_pnl = (BET_USD * (1.0 / entry - 1)) if won_resolve else -BET_USD

    if tp_pct is None:
        return close_ts, htr_pnl

    end_idx = np.searchsorted(ts_arr, close_ts, side="right") - 1
    end_idx = min(end_idx, len(mids) - 1)
    if eidx + 1 > end_idx:
        return close_ts, htr_pnl

    future = mids[eidx + 1:end_idx + 1]
    p_adj = future if side == 1 else (1 - future)
    tp_p = entry * (1 + tp_pct)
    sl_p = entry * (1 - sl_pct)
    tp_hits = np.where(p_adj >= tp_p)[0]
    sl_hits = np.where(p_adj <= sl_p)[0]
    first_tp = tp_hits[0] if len(tp_hits) > 0 else 10**9
    first_sl = sl_hits[0] if len(sl_hits) > 0 else 10**9

    if first_tp == 10**9 and first_sl == 10**9:
        return close_ts, htr_pnl
    if first_tp < first_sl:
        exit_idx = eidx + 1 + first_tp
        return ts_arr[exit_idx], BET_USD * tp_pct
    exit_idx = eidx + 1 + first_sl
    return ts_arr[exit_idx], -BET_USD * sl_pct


def collect_candidates(variant_key, ticks, resolutions, tp_pct, sl_pct):
    """For one variant, collect (entry_ts, exit_ts, pnl) across all markets."""
    parts = variant_key.split("|")
    prim = parts[0]
    pf = parts[1][1:] if len(parts) > 1 else "any"
    sf = parts[2][1:] if len(parts) > 2 else "any"
    ff = parts[3][1:] if len(parts) > 3 else "any"

    candidates = []
    for mid, mkt in ticks.items():
        if mid not in resolutions:
            continue
        res = resolutions[mid]
        yes_won = res["yes_won"]
        close_ts = res["close_ts"]
        mids = mkt["mid"]
        bids = mkt["bid"]
        asks = mkt["ask"]
        ts_arr = mkt["ts"]
        fees_on = mkt["fees"]
        if ff == "free_only" and fees_on:
            continue

        signals = generate_primitive_signals(mids, ts_arr, bids, asks)
        if prim not in signals:
            continue
        sig_arr = signals[prim]
        pf_m = price_mask(mids, pf)
        sf_m = spread_mask(mids, bids, asks, sf)

        active = (sig_arr != 0) & pf_m & sf_m & (ts_arr <= close_ts)
        if not active.any():
            continue
        indices = np.where(active)[0]
        kept = []
        last_t = -1e9
        for idx in indices:
            if ts_arr[idx] - last_t >= 60:
                kept.append(idx)
                last_t = ts_arr[idx]
        if not kept:
            continue
        for eidx in kept:
            sig = sig_arr[eidx]
            entry = (asks[eidx] * (1 + SLIPPAGE) if sig == 1
                     else (1 - bids[eidx]) * (1 + SLIPPAGE))
            if entry < 0.05 or entry > 0.95:
                continue
            ets = ts_arr[eidx]
            exit_ts, pnl = simulate_one_entry(
                mids, ts_arr, fees_on, close_ts, yes_won,
                eidx, sig, entry, tp_pct, sl_pct,
            )
            candidates.append((ets, exit_ts, pnl))
    return candidates


def apply_win3(candidates):
    """Process candidates in entry_ts order with global Win3 deque.
    Returns (accepted_total_pnl, accepted_n, skipped_n).
    """
    candidates = sorted(candidates, key=lambda c: c[0])
    last_3 = []
    skip_next = False
    pending_exits = []  # min-heap by exit_ts
    accepted_pnl = 0.0
    accepted_n = 0
    skipped = 0
    for ets, ext, pnl in candidates:
        # Flush exits that completed before this entry
        while pending_exits and pending_exits[0][0] <= ets:
            x_ts, x_pnl = heapq.heappop(pending_exits)
            last_3.append(x_pnl)
            if len(last_3) > 3:
                last_3.pop(0)
            if len(last_3) == 3 and sum(last_3) < 0:
                skip_next = True
        if skip_next:
            skip_next = False
            skipped += 1
            continue
        accepted_pnl += pnl
        accepted_n += 1
        heapq.heappush(pending_exits, (ext, pnl))
    return accepted_pnl, accepted_n, skipped


def baseline_aggregate(candidates):
    """No filter — aggregate all candidates."""
    total = sum(c[2] for c in candidates)
    return total, len(candidates)


_RES = None
_TICKS = None


def _init(res, ticks):
    global _RES, _TICKS
    _RES = res
    _TICKS = ticks


def _worker(args):
    variant_key, tp_pct, sl_pct = args
    cands = collect_candidates(variant_key, _TICKS, _RES, tp_pct, sl_pct)
    if not cands:
        return None
    base_pnl, base_n = baseline_aggregate(cands)
    w3_pnl, w3_n, w3_skip = apply_win3(cands)
    return {
        "variant": variant_key,
        "tp": tp_pct, "sl": sl_pct,
        "base_n": base_n,
        "base_pnl": round(base_pnl, 4),
        "base_roi_pct": round(base_pnl / (base_n * BET_USD) * 100, 2) if base_n else 0,
        "w3_n": w3_n,
        "w3_pnl": round(w3_pnl, 4),
        "w3_roi_pct": round(w3_pnl / (w3_n * BET_USD) * 100, 2) if w3_n else 0,
        "w3_skipped": w3_skip,
    }


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading top variants...")
    base = json.load(open("bot-data/mass_backtest.json"))
    eligible = [r for r in base["results"] if r.get("n_bets", 0) >= 1000]
    eligible.sort(key=lambda r: -r.get("pnl", 0))
    top = [r["variant"] for r in eligible[:TOP_N]]
    print(f"  Top {len(top)} variants (min n_bets=1000)")

    print(f"[{datetime.now():%H:%M:%S}] Loading data...")
    res = load_resolutions()
    ticks = load_ticks()
    ticks = {m: t for m, t in ticks.items() if m in res}
    print(f"  {len(ticks)} resolved markets")

    args_list = [(v, tp, sl) for v in top for tp, sl in TP_SL_GRID]
    print(f"  Total simulations: {len(args_list)}")

    n_workers = min(max(mp.cpu_count() - 1, 1), 4)  # cap at 4 cores
    t0 = time.time()
    with mp.Pool(n_workers, initializer=_init, initargs=(res, ticks)) as pool:
        results = pool.map(_worker, args_list, chunksize=4)
    results = [r for r in results if r is not None]
    print(f"  Done in {time.time()-t0:.1f}s")

    # Compute boost
    for r in results:
        r["boost_pp"] = round(r["w3_roi_pct"] - r["base_roi_pct"], 2)

    results.sort(key=lambda r: -r["boost_pp"])

    with open(RESULTS_FILE, "w") as f:
        json.dump({
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "n_variants": len(results),
            "results": results,
        }, f, indent=1)
    print(f"\nSaved to {RESULTS_FILE}")

    print(f"\n━━━ TOP 25 by Win3 BOOST (roi_pp) ━━━")
    print(f"  {'variant':<55} {'tp/sl':<10} "
          f"{'base ROI':>9} {'w3 ROI':>9} {'boost':>7} {'skip':>5}")
    for r in results[:25]:
        ts = (f"tp{int(r['tp']*100)}/sl{int(r['sl']*100)}"
              if r["tp"] is not None else "htr")
        print(f"  {r['variant'][:55]:<55} {ts:<10} "
              f"{r['base_roi_pct']:>+8.1f}% {r['w3_roi_pct']:>+8.1f}% "
              f"{r['boost_pp']:>+6.1f}pp {r['w3_skipped']:>5}")

    print(f"\n━━━ BOTTOM 5 (Win3 hurts) ━━━")
    for r in results[-5:]:
        ts = (f"tp{int(r['tp']*100)}/sl{int(r['sl']*100)}"
              if r["tp"] is not None else "htr")
        print(f"  {r['variant'][:55]:<55} {ts:<10} "
              f"{r['base_roi_pct']:>+8.1f}% {r['w3_roi_pct']:>+8.1f}% "
              f"{r['boost_pp']:>+6.1f}pp {r['w3_skipped']:>5}")


if __name__ == "__main__":
    main()
