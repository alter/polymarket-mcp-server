#!/usr/bin/env python3
"""
Ensemble backtest — combine multiple primitives by k-of-n vote.

Tested ensembles:
- MR3: BB_p20_sd20_fade + ZS_p20_t20_fade + ME_p10_t10_fade  (mean reversion trio)
- MR5: + BO_p10_fade + WF_s10_l5_fade
- BO2: BO_p10_fade + BO_p100_fade (breakout consensus)
- LITE: BB_p20_sd25_fade + ZS_p20_t15_fade (2-only lite)

Each ensemble: enters when k-of-n primitives signal same direction at tick t.
Tests across price/spread/fees filters and exit policies.
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
    sig_whale_fade, sig_mean_rev_ema, sig_bollinger,
    sig_zscore, sig_breakout, sig_momentum,
    load_resolutions, load_ticks,
)

RESULTS_FILE = Path("bot-data/mass_backtest_ensemble.json")

ENSEMBLES = {
    "MR3":  {  # 2-of-3 mean-rev trio
        "primitives": [
            ("BB", lambda m, t, b, a: sig_bollinger(m, t, period=20, std_mult=2.0, direction="fade")),
            ("ZS", lambda m, t, b, a: sig_zscore(m, t, period=20, threshold=2.0, direction="fade")),
            ("ME", lambda m, t, b, a: sig_mean_rev_ema(m, t, period=10, dev_threshold=0.01, direction="fade")),
        ], "k_min": 2,
    },
    "MR3-strict": {
        "primitives": [
            ("BB", lambda m, t, b, a: sig_bollinger(m, t, period=20, std_mult=2.0, direction="fade")),
            ("ZS", lambda m, t, b, a: sig_zscore(m, t, period=20, threshold=2.0, direction="fade")),
            ("ME", lambda m, t, b, a: sig_mean_rev_ema(m, t, period=10, dev_threshold=0.01, direction="fade")),
        ], "k_min": 3,  # all agree
    },
    "MR5": {
        "primitives": [
            ("BB", lambda m, t, b, a: sig_bollinger(m, t, period=20, std_mult=2.0, direction="fade")),
            ("ZS", lambda m, t, b, a: sig_zscore(m, t, period=20, threshold=2.0, direction="fade")),
            ("ME", lambda m, t, b, a: sig_mean_rev_ema(m, t, period=10, dev_threshold=0.01, direction="fade")),
            ("BO", lambda m, t, b, a: sig_breakout(m, t, period=10, direction="fade")),
            ("WF", lambda m, t, b, a: sig_whale_fade(m, t, spike=0.01, lookback_min=5, direction="fade")),
        ], "k_min": 3,
    },
    "BO2": {
        "primitives": [
            ("BO10", lambda m, t, b, a: sig_breakout(m, t, period=10, direction="fade")),
            ("BO100", lambda m, t, b, a: sig_breakout(m, t, period=100, direction="fade")),
        ], "k_min": 2,
    },
    "LITE": {
        "primitives": [
            ("BB25", lambda m, t, b, a: sig_bollinger(m, t, period=20, std_mult=2.5, direction="fade")),
            ("ZS15", lambda m, t, b, a: sig_zscore(m, t, period=20, threshold=1.5, direction="fade")),
        ], "k_min": 2,
    },
    "FOLLOW3": {
        "primitives": [
            ("BB", lambda m, t, b, a: sig_bollinger(m, t, period=20, std_mult=2.0, direction="follow")),
            ("ZS", lambda m, t, b, a: sig_zscore(m, t, period=20, threshold=2.0, direction="follow")),
            ("BO", lambda m, t, b, a: sig_breakout(m, t, period=10, direction="follow")),
        ], "k_min": 2,
    },
}

TP_SL_GRID = [(0.10, 0.20), (0.20, 0.50), (None, None)]


def ensemble_sig(name, mids, ts, bids, asks):
    """Returns int8 array of -1/0/+1 — k-of-n vote."""
    cfg = ENSEMBLES[name]
    n = len(mids)
    yes_v = np.zeros(n, dtype=np.int8)
    no_v = np.zeros(n, dtype=np.int8)
    for prim_name, fn in cfg["primitives"]:
        s = fn(mids, ts, bids, asks)
        yes_v += (s == 1).astype(np.int8)
        no_v += (s == -1).astype(np.int8)
    out = np.zeros(n, dtype=np.int8)
    k = cfg["k_min"]
    out[yes_v >= k] = 1
    out[no_v >= k] = -1
    return out


def evaluate_one(args):
    (mid, mkt, resolution, ensemble_name, tp_pct, sl_pct, pf, sf, ff) = args
    if mid not in resolution:
        return None
    res = resolution[mid]
    yes_won = res["yes_won"]
    close_ts = res["close_ts"]
    mids = mkt["mid"]
    bids = mkt["bid"]
    asks = mkt["ask"]
    ts_arr = mkt["ts"]
    fees_on = mkt["fees"]
    if ff == "free_only" and fees_on:
        return None

    sig_arr = ensemble_sig(ensemble_name, mids, ts_arr, bids, asks)
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
    n_bets = 0
    pnl = 0.0
    wins = losses = 0
    for eidx in kept:
        sig = sig_arr[eidx]
        entry = (asks[eidx] * (1 + SLIPPAGE) if sig == 1
                 else (1 - bids[eidx]) * (1 + SLIPPAGE))
        if entry < 0.05 or entry > 0.95:
            continue
        won_resolve = ((sig == 1) and yes_won) or ((sig == -1) and not yes_won)
        htr_pnl = (BET_USD * (1.0 / entry - 1)) if won_resolve else -BET_USD
        if tp_pct is None:
            p = htr_pnl
            wf = won_resolve
        else:
            end_idx = np.searchsorted(ts_arr, close_ts, side="right") - 1
            end_idx = min(end_idx, len(mids) - 1)
            if eidx + 1 > end_idx:
                p, wf = htr_pnl, won_resolve
            else:
                future = mids[eidx + 1:end_idx + 1]
                p_adj = future if sig == 1 else (1 - future)
                tp_p = entry * (1 + tp_pct)
                sl_p = entry * (1 - sl_pct)
                tp_h = np.where(p_adj >= tp_p)[0]
                sl_h = np.where(p_adj <= sl_p)[0]
                ftp = tp_h[0] if len(tp_h) > 0 else 10**9
                fsl = sl_h[0] if len(sl_h) > 0 else 10**9
                if ftp == 10**9 and fsl == 10**9:
                    p, wf = htr_pnl, won_resolve
                elif ftp < fsl:
                    p, wf = BET_USD * tp_pct, True
                else:
                    p, wf = -BET_USD * sl_pct, False
        n_bets += 1
        pnl += p
        if wf:
            wins += 1
        else:
            losses += 1
    return {"wins": wins, "losses": losses, "pnl": pnl, "n_bets": n_bets}


_RES = None


def _init(res):
    global _RES
    _RES = res


def _worker(args):
    market_args, ens_name, tp, sl, pf, sf, ff = args
    mid, mkt = market_args
    return evaluate_one((mid, mkt, _RES, ens_name, tp, sl, pf, sf, ff))


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading data...")
    res = load_resolutions()
    ticks = load_ticks()
    ticks = {m: t for m, t in ticks.items() if m in res}
    print(f"  {len(ticks)} resolved markets")

    config_grid = []
    for ens_name in ENSEMBLES:
        for tp, sl in TP_SL_GRID:
            for pf in ["any", "gt30", "gt50", "lt30", "lt50", "lt70"]:
                for sf in ["any", "tight", "wide"]:
                    for ff in ["any", "free_only"]:
                        config_grid.append((ens_name, tp, sl, pf, sf, ff))
    print(f"  Configs: {len(config_grid)} ensembles × filters × policies")

    # Total: per-config, simulate across all markets
    n_workers = min(max(mp.cpu_count() - 1, 1), 4)  # cap at 4 cores
    t0 = time.time()
    all_args = []
    for cfg in config_grid:
        for mid_mkt in ticks.items():
            all_args.append((mid_mkt,) + cfg)
    print(f"  Sims: {len(all_args):,}")

    with mp.Pool(n_workers, initializer=_init, initargs=(res,)) as pool:
        per_sim = pool.map(_worker, all_args, chunksize=200)
    print(f"  Sims done in {time.time()-t0:.1f}s")

    # Aggregate per config
    agg = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "n_bets": 0})
    for i, r in enumerate(per_sim):
        if r is None:
            continue
        cfg = all_args[i][1:]
        ens, tp, sl, pf, sf, ff = cfg
        ts_label = f"tp{int(tp*100)}sl{int(sl*100)}" if tp else "htr"
        key = f"{ens}|p{pf}|s{sf}|f{ff}|{ts_label}"
        agg[key]["wins"] += r["wins"]
        agg[key]["losses"] += r["losses"]
        agg[key]["pnl"] += r["pnl"]
        agg[key]["n_bets"] += r["n_bets"]

    summary = []
    for k, v in agg.items():
        n = v["wins"] + v["losses"]
        wr = v["wins"] / n * 100 if n else 0
        cost = v["n_bets"] * BET_USD
        roi = v["pnl"] / cost * 100 if cost else 0
        summary.append({
            "config": k, "n_bets": v["n_bets"],
            "wins": v["wins"], "losses": v["losses"],
            "wr": round(wr, 2), "pnl": round(v["pnl"], 4),
            "roi_pct": round(roi, 2),
        })
    summary.sort(key=lambda r: -r["pnl"])
    with open(RESULTS_FILE, "w") as f:
        json.dump({
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "n_configs": len(summary),
            "n_markets": len(ticks),
            "ensembles": list(ENSEMBLES.keys()),
            "results": summary,
        }, f, indent=1)
    print(f"\nSaved to {RESULTS_FILE}")

    eligible = [r for r in summary if r["n_bets"] >= 100]
    eligible.sort(key=lambda r: -r["pnl"])
    print(f"\n━━━ TOP 25 (n>=100) ━━━")
    for r in eligible[:25]:
        print(f"  {r['config'][:60]:<60} ROI={r['roi_pct']:>+6.1f}% "
              f"n={r['n_bets']:>5} WR={r['wr']:.0f}%")


if __name__ == "__main__":
    main()
