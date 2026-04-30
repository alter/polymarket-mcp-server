#!/usr/bin/env python3
"""
MetaVote K-of-N ensemble backtest. Adapts WF stack's MetaVote (Sharpe 1.90 on
crypto K=3-of-8) to Polymarket primitive set.

8 voters (top forward-validated families):
  RSI follow at multiple periods + BO_p100_follow + BB follow + ME fade

For each tick: count YES votes vs NO votes across voters.
If >= K → trigger entry direction.

Output: bot-data/mass_backtest_metavote.json
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
    sig_rsi, sig_breakout, sig_bollinger, sig_mean_rev_ema, sig_zscore,
    load_resolutions, load_ticks,
)

RESULTS_FILE = Path("bot-data/mass_backtest_metavote.json")

# 8 voters from WF-validated forward winners
VOTERS = [
    ("RS7t75",  lambda m, t, b, a: sig_rsi(m, t, period=7, ob_threshold=75, direction="follow")),
    ("RS14t70", lambda m, t, b, a: sig_rsi(m, t, period=14, ob_threshold=70, direction="follow")),
    ("RS21t80", lambda m, t, b, a: sig_rsi(m, t, period=21, ob_threshold=80, direction="follow")),
    ("BO100f",  lambda m, t, b, a: sig_breakout(m, t, period=100, direction="follow")),
    ("BO50f",   lambda m, t, b, a: sig_breakout(m, t, period=50, direction="follow")),
    ("BB20f",   lambda m, t, b, a: sig_bollinger(m, t, period=20, std_mult=2.0, direction="follow")),
    ("BB10f",   lambda m, t, b, a: sig_bollinger(m, t, period=10, std_mult=1.5, direction="follow")),
    ("ZS20f",   lambda m, t, b, a: sig_zscore(m, t, period=20, threshold=2.0, direction="follow")),
]

K_VALUES = [2, 3, 4, 5]  # K-of-8 voting threshold


def metavote_signal(K, mids, ts, bids, asks):
    """Returns int8 array — +1 if K+ voters say YES, -1 if K+ NO, else 0."""
    n = len(mids)
    yes_v = np.zeros(n, dtype=np.int8)
    no_v = np.zeros(n, dtype=np.int8)
    for name, fn in VOTERS:
        s = fn(mids, ts, bids, asks)
        yes_v += (s == 1).astype(np.int8)
        no_v += (s == -1).astype(np.int8)
    out = np.zeros(n, dtype=np.int8)
    out[yes_v >= K] = 1
    out[no_v >= K] = -1
    return out


def evaluate_one(args):
    (mid, mkt, resolution, K, pf, sf, ff) = args
    if mid not in resolution:
        return None
    res = resolution[mid]
    yes_won = res["yes_won"]
    close_ts = res["close_ts"]
    mids = mkt["mid"]; bids = mkt["bid"]; asks = mkt["ask"]; ts_arr = mkt["ts"]
    fees_on = mkt["fees"]
    if ff == "free_only" and fees_on:
        return None
    sig_arr = metavote_signal(K, mids, ts_arr, bids, asks)
    pf_m = price_mask(mids, pf)
    sf_m = spread_mask(mids, bids, asks, sf)
    active = (sig_arr != 0) & pf_m & sf_m & (ts_arr <= close_ts)
    if not active.any():
        return None
    indices = np.where(active)[0]
    kept = []
    last_t = -1e9
    for idx in indices:
        if ts_arr[idx] - last_t >= 60:
            kept.append(idx)
            last_t = ts_arr[idx]
    if not kept:
        return None
    n_bets = wins = losses = 0
    pnl = 0.0
    for eidx in kept:
        sig = sig_arr[eidx]
        entry = (asks[eidx] * (1 + SLIPPAGE) if sig == 1
                 else (1 - bids[eidx]) * (1 + SLIPPAGE))
        if entry < 0.05 or entry > 0.95:
            continue
        won = ((sig == 1) and yes_won) or ((sig == -1) and not yes_won)
        if won:
            pnl += BET_USD * (1.0 / entry - 1)
            wins += 1
        else:
            pnl -= BET_USD
            losses += 1
        n_bets += 1
    return {"K": K, "pf": pf, "sf": sf, "ff": ff,
            "wins": wins, "losses": losses, "pnl": pnl, "n_bets": n_bets}


_RES = None


def _init(res):
    global _RES
    _RES = res


def _worker(args):
    market_args, K, pf, sf, ff = args
    mid, mkt = market_args
    return evaluate_one((mid, mkt, _RES, K, pf, sf, ff))


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading data...")
    res = load_resolutions()
    ticks = load_ticks()
    ticks = {m: t for m, t in ticks.items() if m in res}
    print(f"  {len(ticks)} resolved markets, {len(VOTERS)} voters")

    config_grid = []
    for K in K_VALUES:
        for pf in ["any", "gt30", "lt70"]:
            for sf in ["any", "tight", "wide"]:
                for ff in ["any", "free_only"]:
                    config_grid.append((K, pf, sf, ff))
    print(f"  Configs: {len(config_grid)}")

    n_workers = min(max(mp.cpu_count() - 1, 1), 4)
    t0 = time.time()
    all_args = [(mm, K, pf, sf, ff)
                for K, pf, sf, ff in config_grid
                for mm in ticks.items()]
    print(f"  Sims: {len(all_args):,}")
    with mp.Pool(n_workers, initializer=_init, initargs=(res,)) as pool:
        per_sim = pool.map(_worker, all_args, chunksize=200)
    print(f"  Done in {time.time()-t0:.1f}s")

    agg = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "n_bets": 0})
    for i, r in enumerate(per_sim):
        if r is None: continue
        K, pf, sf, ff = all_args[i][1:]
        key = f"K{K}|p{pf}|s{sf}|f{ff}"
        agg[key]["wins"] += r["wins"]
        agg[key]["losses"] += r["losses"]
        agg[key]["pnl"] += r["pnl"]
        agg[key]["n_bets"] += r["n_bets"]

    summary = []
    for k, v in agg.items():
        n = v["wins"] + v["losses"]
        wr = v["wins"]/n*100 if n else 0
        roi = v["pnl"]/max(v["n_bets"]*BET_USD, 0.01) * 100
        summary.append({"config": k, "n_bets": v["n_bets"], "wins": v["wins"],
                        "losses": v["losses"], "wr": round(wr,2),
                        "pnl": round(v["pnl"], 4), "roi_pct": round(roi, 2)})
    summary.sort(key=lambda r: -r["pnl"])
    with open(RESULTS_FILE, "w") as f:
        json.dump({"ran_at": datetime.now(timezone.utc).isoformat(),
                   "n_voters": len(VOTERS),
                   "K_values": K_VALUES,
                   "results": summary}, f, indent=1)
    print(f"\nSaved to {RESULTS_FILE}")

    eligible = [r for r in summary if r["n_bets"] >= 100]
    eligible.sort(key=lambda r: -r["pnl"])
    print(f"\n━━━ TOP 20 MetaVote configs (n>=100) ━━━")
    for r in eligible[:20]:
        print(f"  {r['config']:<35} ROI={r['roi_pct']:>+6.1f}% n={r['n_bets']:>5} WR={r['wr']:.0f}%")


if __name__ == "__main__":
    main()
