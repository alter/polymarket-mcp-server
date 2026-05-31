#!/usr/bin/env python3
"""
TP/SL extension of mass_backtest — re-runs top variants with TP/SL exit policy.
Models intermediate exits before resolution (matches arena's top winners using
TP10%).

For each entry, scans forward ticks for first hit of TP or SL.
If neither hit by close, holds to resolution.
"""
import json, time, multiprocessing as mp
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import sys
sys.path.insert(0, ".")
from mass_backtest import (
    DATA, TICKS_FILE, META_FILE, CLOB_FILE,
    BET_USD, SLIPPAGE,
    PRICE_FILTERS, FEES_FILTERS, SPREAD_FILTERS,
    price_mask, spread_mask,
    generate_primitive_signals,
    load_resolutions, load_ticks,
)

RESULTS_FILE = Path("bot-data/mass_backtest_tpsl.json")

# TP/SL combinations to test
TP_SL_GRID = [
    (0.05, 0.20),
    (0.10, 0.20),
    (0.10, 0.50),
    (0.20, 0.50),
    (None, None),  # baseline: hold to resolution
]


def simulate_one_variant(sig_arr, mids, bids, asks, ts_arr, fees_on,
                          close_ts, yes_won,
                          pf_mask, sf_mask, ff,
                          tp_pct, sl_pct, cooldown=60):
    """Vectorized simulation for one (variant, market) combo with TP/SL."""
    if ff == "free_only" and fees_on:
        return 0, 0, 0.0, 0
    active = (sig_arr != 0) & pf_mask & sf_mask & (ts_arr <= close_ts)
    if not active.any():
        return 0, 0, 0.0, 0
    indices = np.where(active)[0]

    # Cooldown sequential filter
    kept = []
    last_ts = -1e9
    for idx in indices:
        if ts_arr[idx] - last_ts >= cooldown:
            kept.append(idx)
            last_ts = ts_arr[idx]
    if not kept:
        return 0, 0, 0.0, 0
    kept = np.array(kept)
    sigs = sig_arr[kept]

    # Entry prices (slippage)
    yes_mask = sigs == 1
    no_mask = sigs == -1
    entries = np.zeros(len(kept))
    entries[yes_mask] = asks[kept][yes_mask] * (1 + SLIPPAGE)
    entries[no_mask] = (1 - bids[kept][no_mask]) * (1 + SLIPPAGE)

    entry_ok = (entries >= 0.05) & (entries <= 0.95)
    if not entry_ok.any():
        return 0, 0, 0.0, 0
    kept = kept[entry_ok]
    sigs = sigs[entry_ok]
    entries = entries[entry_ok]

    # One bet per market per variant. Repeated intra-market entries all settle against
    # the SAME resolution → counting them as independent overstates n, WR and ROI
    # (per-tick aggregation trap). Take the first valid entry so each market contributes
    # exactly one win or one loss, matching how a position-at-a-time bot would trade.
    if len(kept) > 0:
        kept = kept[:1]
        sigs = sigs[:1]
        entries = entries[:1]

    n_bets = len(kept)
    wins = 0
    losses = 0
    total_pnl = 0.0

    for i in range(n_bets):
        eidx = kept[i]
        e = entries[i]
        s = sigs[i]
        won_resolve = ((s == 1) and yes_won) or ((s == -1) and not yes_won)
        htr_pnl = (BET_USD * (1.0/e - 1)) if won_resolve else -BET_USD

        if tp_pct is None:
            # Hold to resolution baseline
            pnl = htr_pnl
            won_flag = won_resolve
        else:
            # Side-adjusted future prices: YES → mid, NO → 1-mid
            end_idx = np.searchsorted(ts_arr, close_ts, side="right") - 1
            end_idx = min(end_idx, len(mids) - 1)
            if eidx + 1 > end_idx:
                pnl = htr_pnl
                won_flag = won_resolve
            else:
                future = mids[eidx + 1:end_idx + 1]
                p_adj = future if s == 1 else (1 - future)
                tp_p = e * (1 + tp_pct)
                sl_p = e * (1 - sl_pct)
                tp_hits = np.where(p_adj >= tp_p)[0]
                sl_hits = np.where(p_adj <= sl_p)[0]
                first_tp = tp_hits[0] if len(tp_hits) > 0 else 10**9
                first_sl = sl_hits[0] if len(sl_hits) > 0 else 10**9
                if first_tp == 10**9 and first_sl == 10**9:
                    pnl = htr_pnl
                    won_flag = won_resolve
                elif first_tp < first_sl:
                    pnl = BET_USD * tp_pct
                    won_flag = True
                else:
                    pnl = -BET_USD * sl_pct
                    won_flag = False

        if won_flag:
            wins += 1
        else:
            losses += 1
        total_pnl += pnl

    return wins, losses, total_pnl, n_bets


def evaluate_market_tpsl(args):
    (mid, mkt, resolution, top_variant_keys) = args
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
    pmask = {pf: price_mask(mids, pf) for pf in PRICE_FILTERS}
    smask = {sf: spread_mask(mids, bids, asks, sf) for sf in SPREAD_FILTERS}

    results = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "n_bets": 0})

    for var_key in top_variant_keys:
        # Parse variant key: "PRIM|p<pf>|s<sf>|f<ff>"
        parts = var_key.split("|")
        prim = parts[0]
        pf = parts[1][1:] if len(parts) > 1 else "any"
        sf = parts[2][1:] if len(parts) > 2 else "any"
        ff = parts[3][1:] if len(parts) > 3 else "any"

        if prim not in signals:
            continue
        sig_arr = signals[prim]
        pf_m = pmask.get(pf, np.ones(len(mids), dtype=bool))
        sf_m = smask.get(sf, np.ones(len(mids), dtype=bool))

        for tp_pct, sl_pct in TP_SL_GRID:
            tp_label = (f"tp{int(tp_pct*100)}sl{int(sl_pct*100)}"
                        if tp_pct is not None else "htr")
            full_key = f"{var_key}|{tp_label}"
            w, l, pnl, n = simulate_one_variant(
                sig_arr, mids, bids, asks, ts_arr, fees_on,
                close_ts, yes_won, pf_m, sf_m, ff, tp_pct, sl_pct,
            )
            if n > 0:
                results[full_key]["wins"] += w
                results[full_key]["losses"] += l
                results[full_key]["pnl"] += pnl
                results[full_key]["n_bets"] += n

    return dict(results)


_RES = None
_TOP = None


def _init(res, top):
    global _RES, _TOP
    _RES, _TOP = res, top


def _worker(args):
    mid, mkt = args
    return evaluate_market_tpsl((mid, mkt, _RES, _TOP))


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading top variants from baseline...")
    base = json.load(open("bot-data/mass_backtest.json"))
    eligible = [r for r in base["results"] if r.get("n_bets", 0) >= 200]
    eligible.sort(key=lambda r: -r.get("pnl", 0))
    top = [r["variant"] for r in eligible[:80]]
    print(f"  Top 80 variants from baseline (min n_bets=200)")

    print(f"[{datetime.now():%H:%M:%S}] Loading data...")
    res = load_resolutions()
    ticks = load_ticks()
    ticks = {m: t for m, t in ticks.items() if m in res}
    print(f"  {len(ticks)} markets resolved")

    n_workers = min(max(mp.cpu_count() - 1, 1), 4)  # cap at 4 cores
    print(f"  Workers: {n_workers}, TP/SL grid: {len(TP_SL_GRID)}")
    print(f"  Total variants: {len(top) * len(TP_SL_GRID)} = {len(top) * len(TP_SL_GRID):,}")

    t0 = time.time()
    args_list = list(ticks.items())
    with mp.Pool(n_workers, initializer=_init, initargs=(res, top)) as pool:
        per_market = pool.map(_worker, args_list, chunksize=10)
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
        total = v["wins"] + v["losses"]
        wr = v["wins"] / total * 100 if total else 0
        cost = v["n_bets"] * BET_USD
        roi = v["pnl"] / cost * 100 if cost else 0
        summary.append({
            "variant": k, "n_bets": v["n_bets"],
            "wins": v["wins"], "losses": v["losses"],
            "wr": round(wr, 2), "pnl": round(v["pnl"], 4),
            "roi_pct": round(roi, 2),
        })
    summary.sort(key=lambda r: -r["pnl"])
    with open(RESULTS_FILE, "w") as f:
        json.dump({
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "n_variants": len(summary),
            "n_markets": len(ticks),
            "tp_sl_grid": [(t, s) for t, s in TP_SL_GRID],
            "results": summary,
        }, f, indent=1)
    print(f"\nSaved to {RESULTS_FILE}")

    # Compare HTR vs TP/SL for each base variant
    print(f"\n━━━ HTR vs best TP/SL per base variant (top 25 by best PnL) ━━━")
    by_base = defaultdict(dict)
    for r in summary:
        base_key = "|".join(r["variant"].split("|")[:-1])
        suffix = r["variant"].split("|")[-1]
        by_base[base_key][suffix] = r

    rows = []
    for base_key, by_suffix in by_base.items():
        htr = by_suffix.get("htr")
        if not htr:
            continue
        best_tpsl = max(
            (v for k, v in by_suffix.items() if k != "htr"),
            key=lambda v: v.get("pnl", 0), default=None,
        )
        if not best_tpsl:
            continue
        rows.append((base_key, htr, best_tpsl))
    rows.sort(key=lambda x: -max(x[1]["pnl"], x[2]["pnl"]))
    print(f"  {'variant':<50} {'HTR ROI':>9} {'best TP/SL':>20} {'TP/SL ROI':>10}")
    for base_key, htr, bt in rows[:25]:
        suffix = bt["variant"].split("|")[-1]
        print(f"  {base_key[:50]:<50} {htr['roi_pct']:>+8.1f}% "
              f"{suffix:<20} {bt['roi_pct']:>+9.1f}%")


if __name__ == "__main__":
    main()
