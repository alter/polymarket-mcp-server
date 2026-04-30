#!/usr/bin/env python3
"""
Walk-forward 70/30 validation + bootstrap sign-flip null for top mass_backtest
variants. Validates which variants survive OOS (not just selection bias).

Methodology (from WF stack):
- Train 70% of resolved markets, Test 30% (chronological by close_ts)
- Validate gate: Full ROI > 0 AND TEST ROI > 0 AND TEST WR > 50% AND p-value < 0.10
- Bootstrap sign-flip null: 200 iterations, randomly flip signs of per-trade pnl
- p-value = fraction of random Sharpe ≥ actual

Output: bot-data/walkforward_results.json
"""
import json, time, multiprocessing as mp, random
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

RESULTS_FILE = Path("bot-data/walkforward_results.json")
TOP_N = 60       # variants to validate
N_BOOTSTRAP = 200
TRAIN_FRAC = 0.7


def collect_pnls(variant_key, ticks, resolutions):
    """Return list of (close_ts, entry_ts, pnl) per entry across all markets.
    Used for both train/test split AND bootstrap.
    """
    parts = variant_key.split("|")
    prim = parts[0]
    pf = parts[1][1:] if len(parts) > 1 else "any"
    sf = parts[2][1:] if len(parts) > 2 else "any"
    ff = parts[3][1:] if len(parts) > 3 else "any"

    out = []
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
            won = ((sig == 1) and yes_won) or ((sig == -1) and not yes_won)
            pnl = (BET_USD * (1.0 / entry - 1)) if won else -BET_USD
            out.append((close_ts, ts_arr[eidx], pnl))
    return out


def stats(pnls):
    """Returns dict with n, total_pnl, roi_pct, wr, sharpe."""
    if not pnls:
        return {"n": 0, "pnl": 0.0, "roi_pct": 0, "wr": 0, "sharpe": 0}
    arr = np.array([p[2] for p in pnls])
    n = len(arr)
    total = float(arr.sum())
    cost = n * BET_USD
    roi = total / cost * 100
    wins = int((arr > 0).sum())
    wr = wins / n * 100
    sharpe = float(arr.mean() / arr.std()) if arr.std() > 0 else 0
    return {"n": n, "pnl": round(total, 4), "roi_pct": round(roi, 2),
            "wr": round(wr, 2), "sharpe": round(sharpe * np.sqrt(n), 4)}


def bootstrap_pvalue(pnls, n_iter=200):
    """Sign-flip null hypothesis: randomly flip signs of pnls.
    Returns fraction of random trials with sharpe >= actual.
    Lower p = stronger evidence.
    """
    if len(pnls) < 10:
        return 1.0
    arr = np.array([p[2] for p in pnls])
    actual = arr.mean() / arr.std() if arr.std() > 0 else 0
    if actual <= 0:
        return 1.0  # no edge to validate
    extreme = 0
    rng = np.random.default_rng(42)
    for _ in range(n_iter):
        flips = rng.choice([-1, 1], size=len(arr))
        rnd = arr * flips
        rs = rnd.mean() / rnd.std() if rnd.std() > 0 else 0
        if rs >= actual:
            extreme += 1
    return extreme / n_iter


def evaluate(variant_key, ticks, resolutions):
    pnls = collect_pnls(variant_key, ticks, resolutions)
    if len(pnls) < 30:
        return {"variant": variant_key, "skip": "n<30", "full": stats(pnls)}

    # Sort by close_ts, split train/test
    pnls_sorted = sorted(pnls, key=lambda x: x[0])
    split_idx = int(len(pnls_sorted) * TRAIN_FRAC)
    train_pnls = pnls_sorted[:split_idx]
    test_pnls = pnls_sorted[split_idx:]

    full_s = stats(pnls_sorted)
    train_s = stats(train_pnls)
    test_s = stats(test_pnls)
    p_full = bootstrap_pvalue(pnls_sorted, N_BOOTSTRAP)
    p_test = bootstrap_pvalue(test_pnls, N_BOOTSTRAP)

    # Validation gate: Full ROI>0 AND TEST ROI>0 AND TEST WR>=50 AND p_full<0.10
    passed = (full_s["roi_pct"] > 0
              and test_s["roi_pct"] > 0
              and test_s["wr"] >= 50
              and p_full < 0.10)

    return {
        "variant": variant_key,
        "full": full_s, "train": train_s, "test": test_s,
        "p_full": round(p_full, 4),
        "p_test": round(p_test, 4),
        "passed_gate": passed,
    }


_RES = None
_TICKS = None


def _init(res, ticks):
    global _RES, _TICKS
    _RES, _TICKS = res, ticks


def _worker(var_key):
    return evaluate(var_key, _TICKS, _RES)


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading top variants...")
    base = json.load(open("bot-data/mass_backtest.json"))
    eligible = [r for r in base["results"]
                if r.get("n_bets", 0) >= 1000 and r.get("roi_pct", 0) > 0]
    eligible.sort(key=lambda r: -r.get("pnl", 0))
    top = [r["variant"] for r in eligible[:TOP_N]]
    print(f"  Top {len(top)} variants (n>=1000, ROI>0)")

    print(f"[{datetime.now():%H:%M:%S}] Loading data...")
    res = load_resolutions()
    ticks = load_ticks()
    ticks = {m: t for m, t in ticks.items() if m in res}
    print(f"  {len(ticks)} resolved markets")

    n_workers = min(max(mp.cpu_count() - 1, 1), 4)
    t0 = time.time()
    with mp.Pool(n_workers, initializer=_init, initargs=(res, ticks)) as pool:
        results = pool.map(_worker, top)
    print(f"  Eval done in {time.time()-t0:.1f}s")

    passed = [r for r in results if r.get("passed_gate")]
    failed = [r for r in results if not r.get("passed_gate") and "skip" not in r]
    skipped = [r for r in results if "skip" in r]

    out = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "n_top": TOP_N,
        "n_passed": len(passed),
        "n_failed": len(failed),
        "n_skipped": len(skipped),
        "results": results,
    }
    with open(RESULTS_FILE, "w") as f:
        json.dump(out, f, indent=1)

    # Print results
    print(f"\n━━━ Walk-forward results ━━━")
    print(f"  Passed gate: {len(passed)}/{len(top)}")
    print(f"  Failed: {len(failed)}, Skipped: {len(skipped)}")

    print(f"\n━━━ PASSED gate variants ━━━")
    print(f"  {'variant':<55} {'full ROI':>9} {'train':>9} {'test':>9} {'p_full':>7}")
    passed.sort(key=lambda r: -r["test"]["roi_pct"])
    for r in passed[:25]:
        print(f"  {r['variant'][:55]:<55} "
              f"{r['full']['roi_pct']:>+8.1f}% "
              f"{r['train']['roi_pct']:>+8.1f}% "
              f"{r['test']['roi_pct']:>+8.1f}% "
              f"{r['p_full']:>6.3f}")

    print(f"\n━━━ FAILED gate (TEST regression) ━━━")
    failed.sort(key=lambda r: r["test"]["roi_pct"])
    for r in failed[:10]:
        print(f"  {r['variant'][:55]:<55} "
              f"{r['full']['roi_pct']:>+8.1f}% "
              f"{r['train']['roi_pct']:>+8.1f}% "
              f"{r['test']['roi_pct']:>+8.1f}% "
              f"{r['p_full']:>6.3f}")


if __name__ == "__main__":
    main()
