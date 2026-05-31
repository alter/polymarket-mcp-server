#!/usr/bin/env python3
"""
Multi-tick persistence backtest — only enter signal if it persists for K
consecutive ticks. Tests whether persistence filter improves win rate / ROI
on top variants.

Hypothesis: noisy single-tick signals are false positives. Requiring
persistence filters them.
"""
import json, time, multiprocessing as mp
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

RESULTS_FILE = Path("bot-data/mass_backtest_persistence.json")
TOP_N = 50
PERSISTENCE_VALUES = [1, 2, 3, 5]  # # of consecutive same-direction ticks


def evaluate_one(args):
    (mid, mkt, resolution, variant_keys) = args
    if mid not in resolution:
        return {}
    res = resolution[mid]
    yes_won = res["yes_won"]
    close_ts = res["close_ts"]
    mids = mkt["mid"]
    bids = mkt["bid"]
    asks = mkt["ask"]
    ts_arr = mkt["ts"]
    fees_on = mkt["fees"]

    signals = generate_primitive_signals(mids, ts_arr, bids, asks)
    pmask_cache = {pf: price_mask(mids, pf) for pf in PRICE_FILTERS}
    smask_cache = {sf: spread_mask(mids, bids, asks, sf) for sf in SPREAD_FILTERS}
    time_valid = ts_arr <= close_ts

    results = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "n_bets": 0})

    for var_key in variant_keys:
        parts = var_key.split("|")
        prim = parts[0]
        pf = parts[1][1:] if len(parts) > 1 else "any"
        sf = parts[2][1:] if len(parts) > 2 else "any"
        ff = parts[3][1:] if len(parts) > 3 else "any"
        if ff == "free_only" and fees_on:
            continue
        if prim not in signals:
            continue
        sig_arr = signals[prim]
        pf_m = pmask_cache.get(pf, np.ones(len(mids), dtype=bool))
        sf_m = smask_cache.get(sf, np.ones(len(mids), dtype=bool))

        for k in PERSISTENCE_VALUES:
            # Build "persistent signal": at index i, sig_persistent[i] = sig_arr[i]
            # iff sig_arr[i-(k-1):i+1] all equal sig_arr[i] AND non-zero
            if k == 1:
                pers = sig_arr.copy()
            else:
                pers = np.zeros_like(sig_arr)
                for i in range(k - 1, len(sig_arr)):
                    s = sig_arr[i]
                    if s == 0:
                        continue
                    if all(sig_arr[i - j] == s for j in range(k)):
                        pers[i] = s
            active = (pers != 0) & pf_m & sf_m & time_valid
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
            kept = np.array(kept)
            sigs = pers[kept]
            yes_mask = sigs == 1
            no_mask = sigs == -1
            entries = np.zeros(len(kept))
            entries[yes_mask] = asks[kept][yes_mask] * (1 + SLIPPAGE)
            entries[no_mask] = (1 - bids[kept][no_mask]) * (1 + SLIPPAGE)
            ok = (entries >= 0.05) & (entries <= 0.95)
            if not ok.any():
                continue
            kept = kept[ok]
            sigs = sigs[ok]
            entries = entries[ok]
            wins_arr = ((sigs == 1) & yes_won) | ((sigs == -1) & (not yes_won))
            shares = BET_USD / entries
            pnls = np.where(wins_arr, shares - BET_USD, -BET_USD)
            full_key = f"{var_key}|k{k}"
            results[full_key]["wins"] += int(wins_arr.sum())
            results[full_key]["losses"] += int((~wins_arr).sum())
            results[full_key]["pnl"] += float(pnls.sum())
            results[full_key]["n_bets"] += int(len(kept))
    return dict(results)


_RES = None
_TOP = None


def _init(res, top):
    global _RES, _TOP
    _RES, _TOP = res, top


def _worker(args):
    mid, mkt = args
    return evaluate_one((mid, mkt, _RES, _TOP))


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading top variants...")
    base = json.load(open("bot-data/mass_backtest.json"))
    eligible = [r for r in base["results"]
                if r.get("n_bets", 0) >= 1000 and r.get("roi_pct", 0) > 0]
    eligible.sort(key=lambda r: -r.get("pnl", 0))
    top = [r["variant"] for r in eligible[:TOP_N]]
    print(f"  Top {len(top)} variants")

    print(f"[{datetime.now():%H:%M:%S}] Loading data...")
    res = load_resolutions()
    ticks = load_ticks()
    ticks = {m: t for m, t in ticks.items() if m in res}
    print(f"  {len(ticks)} resolved markets")

    n_workers = min(max(mp.cpu_count() - 1, 1), 4)  # cap at 4 cores
    args_list = list(ticks.items())
    t0 = time.time()
    with mp.Pool(n_workers, initializer=_init, initargs=(res, top)) as pool:
        per_market = pool.map(_worker, args_list, chunksize=8)
    print(f"  Eval done in {time.time()-t0:.1f}s")

    agg = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "n_bets": 0})
    for d in per_market:
        for k, v in d.items():
            agg[k]["wins"] += v["wins"]
            agg[k]["losses"] += v["losses"]
            agg[k]["pnl"] += v["pnl"]
            agg[k]["n_bets"] += v["n_bets"]

    summary = []
    for k, v in agg.items():
        n = v["wins"] + v["losses"]
        wr = v["wins"] / n * 100 if n else 0
        roi = v["pnl"] / max(v["n_bets"] * BET_USD, 0.01) * 100
        summary.append({
            "variant": k, "n_bets": v["n_bets"], "wins": v["wins"], "losses": v["losses"],
            "wr": round(wr, 2), "pnl": round(v["pnl"], 4), "roi_pct": round(roi, 2),
        })
    summary.sort(key=lambda r: -r["pnl"])

    with open(RESULTS_FILE, "w") as f:
        json.dump({
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "n_variants": len(summary),
            "n_markets": len(ticks),
            "persistence_values": PERSISTENCE_VALUES,
            "results": summary,
        }, f, indent=1)
    print(f"\nSaved to {RESULTS_FILE}")

    # Compare per base variant: best k vs k=1
    print(f"\n━━━ Persistence boost per base variant (top 25) ━━━")
    by_base = defaultdict(dict)
    for r in summary:
        parts = r["variant"].split("|")
        base = "|".join(parts[:-1])
        k = parts[-1]
        by_base[base][k] = r

    rows = []
    for base, by_k in by_base.items():
        if "k1" not in by_k:
            continue
        k1 = by_k["k1"]
        best = max((v for kk, v in by_k.items() if kk != "k1"),
                   key=lambda v: v["roi_pct"], default=None)
        if not best:
            continue
        boost = best["roi_pct"] - k1["roi_pct"]
        rows.append((base, k1, best, boost))
    rows.sort(key=lambda x: -x[3])
    print(f"  {'variant':<55} {'k1 ROI':>9} {'best k':<5} {'best ROI':>10} {'boost':>7}")
    for base, k1, best, boost in rows[:25]:
        bk = best["variant"].split("|")[-1]
        print(f"  {base[:55]:<55} {k1['roi_pct']:>+8.1f}% {bk:<5} "
              f"{best['roi_pct']:>+9.1f}% {boost:>+6.1f}pp")


if __name__ == "__main__":
    main()
