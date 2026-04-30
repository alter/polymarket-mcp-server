#!/usr/bin/env python3
"""
Mass strategy backtest — millions of composable variants.

Architecture:
  1. PRIMITIVES: atomic signal generators (whale_fade, mean_rev, bollinger, rsi,
     wavelet, breakout, momentum, zscore, always_no_filter) — vectorized numpy.
  2. Each primitive has a finite parameter grid → produces N signal arrays per market.
  3. VARIANT = (primary_primitive, secondary_filter, direction, price_filter,
     fees_filter, sl, tp). Composed at evaluation time, not stored explicitly.
  4. VECTORIZED EVALUATION: for each market, compute all primitive signals once,
     then evaluate ALL variant combinations in vectorized loops.
  5. MULTIPROCESSING: distribute markets across CPU cores.

Target: 500K-1M variants in <30 min on 8 cores.
"""
import json, os, sys, time, itertools, gc
import multiprocessing as mp
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

DATA = Path("bot-data")
TICKS_FILE = DATA / "arena_ticks.jsonl"
META_FILE = DATA / "gamma_market_meta.json"
CLOB_FILE = DATA / "clob_cache.json"
RESULTS_FILE = DATA / "mass_backtest.json"

BET_USD = 0.01
SLIPPAGE = 0.001  # 0.1% slippage on entry+exit


# ─── Primitives: each generates a {-1, 0, +1} signal array per market ────

# Each primitive has a closed parameter grid.
# Total signals computed per market: sum over all primitives.

# Format: (name, function, param_grid)
# function takes (mid_array, ts_array) and params → int8 signal array

def sig_whale_fade(mids, ts, *, spike, lookback_min, direction):
    """Spike fade/follow: -1=NO, +1=YES."""
    n = len(mids)
    if n < 5:
        return np.zeros(n, dtype=np.int8)
    cutoff = ts - lookback_min * 60
    idx = np.searchsorted(ts, cutoff, side="right") - 1
    idx = np.clip(idx, 0, n - 1)
    valid = (idx < np.arange(n)) & (mids[idx] > 0)
    change = np.where(valid, (mids - mids[idx]) / np.maximum(mids[idx], 1e-9), 0.0)
    sig = np.zeros(n, dtype=np.int8)
    if direction == "fade":
        sig[change >= spike] = -1   # spike up → bet NO
        sig[change <= -spike] = 1   # spike down → bet YES
    else:  # follow
        sig[change >= spike] = 1
        sig[change <= -spike] = -1
    return sig


def sig_mean_rev_ema(mids, ts, *, period, dev_threshold, direction):
    """EMA mean reversion."""
    n = len(mids)
    if n < period + 2:
        return np.zeros(n, dtype=np.int8)
    alpha = 2.0 / (period + 1)
    ema = np.empty_like(mids)
    ema[0] = mids[0]
    for i in range(1, n):
        ema[i] = alpha * mids[i] + (1 - alpha) * ema[i-1]
    dev = (mids - ema) / np.maximum(np.abs(ema), 1e-9)
    sig = np.zeros(n, dtype=np.int8)
    if direction == "fade":
        sig[dev > dev_threshold] = -1
        sig[dev < -dev_threshold] = 1
    else:
        sig[dev > dev_threshold] = 1
        sig[dev < -dev_threshold] = -1
    return sig


def sig_bollinger(mids, ts, *, period, std_mult, direction):
    n = len(mids)
    if n < period:
        return np.zeros(n, dtype=np.int8)
    csum = np.cumsum(mids)
    csum2 = np.cumsum(mids ** 2)
    sma = np.full(n, np.nan)
    sma[period-1:] = (csum[period-1:] - np.concatenate(([0], csum[:-period]))) / period
    msq = np.full(n, np.nan)
    msq[period-1:] = (csum2[period-1:] - np.concatenate(([0], csum2[:-period]))) / period
    var = np.maximum(msq - sma ** 2, 0)
    sd = np.sqrt(var)
    upper = sma + std_mult * sd
    lower = sma - std_mult * sd
    sig = np.zeros(n, dtype=np.int8)
    if direction == "fade":
        sig[mids > upper] = -1  # above upper band → revert to NO
        sig[mids < lower] = 1
    else:
        sig[mids > upper] = 1
        sig[mids < lower] = -1
    return sig


def sig_zscore(mids, ts, *, period, threshold, direction):
    n = len(mids)
    if n < period:
        return np.zeros(n, dtype=np.int8)
    csum = np.cumsum(mids)
    csum2 = np.cumsum(mids ** 2)
    sma = np.full(n, np.nan)
    sma[period-1:] = (csum[period-1:] - np.concatenate(([0], csum[:-period]))) / period
    msq = np.full(n, np.nan)
    msq[period-1:] = (csum2[period-1:] - np.concatenate(([0], csum2[:-period]))) / period
    var = np.maximum(msq - sma ** 2, 0)
    sd = np.sqrt(var)
    z = np.where(sd > 0, (mids - sma) / np.maximum(sd, 1e-9), 0.0)
    z[:period-1] = 0
    sig = np.zeros(n, dtype=np.int8)
    if direction == "fade":
        sig[z > threshold] = -1
        sig[z < -threshold] = 1
    else:
        sig[z > threshold] = 1
        sig[z < -threshold] = -1
    return sig


def sig_breakout(mids, ts, *, period, direction):
    n = len(mids)
    if n < period:
        return np.zeros(n, dtype=np.int8)
    sig = np.zeros(n, dtype=np.int8)
    # Rolling max/min via simple stride loop (O(N×period), but P is small)
    for i in range(period, n):
        window = mids[i-period:i+1]
        hi, lo = window.max(), window.min()
        if mids[i] >= hi * 0.999:
            sig[i] = 1 if direction == "follow" else -1
        elif mids[i] <= lo * 1.001:
            sig[i] = -1 if direction == "follow" else 1
    return sig


def sig_momentum(mids, ts, *, period, threshold, direction):
    n = len(mids)
    if n <= period:
        return np.zeros(n, dtype=np.int8)
    out = np.zeros(n, dtype=np.float64)
    out[period:] = (mids[period:] - mids[:-period]) / np.maximum(mids[:-period], 1e-9)
    sig = np.zeros(n, dtype=np.int8)
    if direction == "follow":
        sig[out > threshold] = 1
        sig[out < -threshold] = -1
    else:
        sig[out > threshold] = -1
        sig[out < -threshold] = 1
    return sig


def sig_rsi(mids, ts, *, period, ob_threshold, direction):
    """Wilder's RSI. ob_threshold = overbought level (e.g. 70).
    Oversold = 100 - ob_threshold (symmetric).
    fade: overbought → -1 (NO), oversold → +1 (YES). follow: opposite.
    """
    n = len(mids)
    if n < period + 2:
        return np.zeros(n, dtype=np.int8)
    diffs = np.diff(mids, prepend=mids[0])
    gains = np.where(diffs > 0, diffs, 0)
    losses = np.where(diffs < 0, -diffs, 0)
    avg_gain = np.zeros(n)
    avg_loss = np.zeros(n)
    avg_gain[period] = gains[1:period+1].mean()
    avg_loss[period] = losses[1:period+1].mean()
    for i in range(period+1, n):
        avg_gain[i] = (avg_gain[i-1] * (period-1) + gains[i]) / period
        avg_loss[i] = (avg_loss[i-1] * (period-1) + losses[i]) / period
    rs = avg_gain / np.maximum(avg_loss, 1e-9)
    rsi = 100 - 100 / (1 + rs)
    sig = np.zeros(n, dtype=np.int8)
    os_threshold = 100 - ob_threshold
    if direction == "fade":
        sig[rsi >= ob_threshold] = -1   # overbought → bet NO
        sig[rsi <= os_threshold] = 1    # oversold → bet YES
    else:
        sig[rsi >= ob_threshold] = 1
        sig[rsi <= os_threshold] = -1
    sig[:period+1] = 0
    return sig


def sig_skew(mids, ts, bids, asks, *, threshold, direction):
    """L1 mid-skew proxy for orderbook imbalance.

    skew = (mid - bid) / (ask - bid).
    skew > 0.5 → mid closer to ask → buyer pressure → follow=YES, fade=NO.
    skew < 0.5 → mid closer to bid → seller pressure → follow=NO, fade=YES.
    threshold = abs deviation from 0.5 (e.g. 0.2 → skew>0.7 OR skew<0.3).
    """
    n = len(mids)
    if n < 5:
        return np.zeros(n, dtype=np.int8)
    spread = asks - bids
    valid = spread > 1e-6
    skew = np.where(valid, (mids - bids) / np.maximum(spread, 1e-9), 0.5)
    dev = skew - 0.5
    sig = np.zeros(n, dtype=np.int8)
    if direction == "follow":
        sig[dev >= threshold] = 1   # mid pushed toward ask → YES
        sig[dev <= -threshold] = -1
    else:  # fade
        sig[dev >= threshold] = -1  # mid pushed toward ask → bet NO (revert)
        sig[dev <= -threshold] = 1
    sig[~valid] = 0
    return sig


def sig_spread_regime(mids, ts, bids, asks, *, spread_pct_thr, direction):
    """Spread-regime momentum: when spread tightens, treat last mid drift as signal.

    Computes spread% = (ask-bid)/mid, finds rolling baseline (mean of prev 50),
    enters when spread compresses below threshold * baseline AND mid is moving.
    direction=follow → bet in direction of recent 5-tick mid drift.
    """
    n = len(mids)
    if n < 60:
        return np.zeros(n, dtype=np.int8)
    spr_pct = np.where(mids > 0, (asks - bids) / np.maximum(mids, 1e-9), 1.0)
    # Rolling baseline (50-tick mean)
    csum = np.cumsum(spr_pct)
    base = np.full(n, np.nan)
    base[50:] = (csum[50:] - csum[:-50]) / 50.0
    # Compression mask: current spread < spread_pct_thr × baseline
    compressed = (spr_pct < spread_pct_thr * base) & ~np.isnan(base)
    # 5-tick mid drift
    drift = np.zeros(n)
    drift[5:] = (mids[5:] - mids[:-5]) / np.maximum(mids[:-5], 1e-9)
    sig = np.zeros(n, dtype=np.int8)
    if direction == "follow":
        sig[compressed & (drift > 0.005)] = 1
        sig[compressed & (drift < -0.005)] = -1
    else:
        sig[compressed & (drift > 0.005)] = -1
        sig[compressed & (drift < -0.005)] = 1
    return sig


# ─── Variant generation ─────────────────────────────────────────────────

def generate_primitive_signals(mids, ts, bids=None, asks=None):
    """Compute all primitive signals for one market.
    Returns dict {variant_id: signal_array}.
    """
    out = {}

    # Whale fade — 7 spike × 5 lookback × 2 direction = 70 variants
    for spike in [0.005, 0.01, 0.015, 0.02, 0.03, 0.05, 0.08]:
        for lm in [1, 2, 5, 10, 30]:
            for dr in ["fade", "follow"]:
                key = f"WF_s{int(spike*1000)}_l{lm}_{dr}"
                out[key] = sig_whale_fade(mids, ts, spike=spike, lookback_min=lm, direction=dr)

    # Mean rev EMA — 6 periods × 4 thresh × 2 dir = 48
    for p in [5, 10, 20, 30, 50, 100]:
        for thr in [0.005, 0.01, 0.02, 0.04]:
            for dr in ["fade", "follow"]:
                key = f"ME_p{p}_t{int(thr*1000)}_{dr}"
                out[key] = sig_mean_rev_ema(mids, ts, period=p, dev_threshold=thr, direction=dr)

    # Bollinger — 4 periods × 3 std × 2 dir = 24
    for p in [10, 20, 30, 50]:
        for sd in [1.5, 2.0, 2.5]:
            for dr in ["fade", "follow"]:
                key = f"BB_p{p}_sd{int(sd*10)}_{dr}"
                out[key] = sig_bollinger(mids, ts, period=p, std_mult=sd, direction=dr)

    # Zscore — 4 periods × 3 thresh × 2 dir = 24
    for p in [10, 20, 50, 100]:
        for thr in [1.5, 2.0, 3.0]:
            for dr in ["fade", "follow"]:
                key = f"ZS_p{p}_t{int(thr*10)}_{dr}"
                out[key] = sig_zscore(mids, ts, period=p, threshold=thr, direction=dr)

    # Breakout — 4 periods × 2 dir = 8
    for p in [10, 20, 50, 100]:
        for dr in ["fade", "follow"]:
            key = f"BO_p{p}_{dr}"
            out[key] = sig_breakout(mids, ts, period=p, direction=dr)

    # Momentum — 4 periods × 3 thresh × 2 dir = 24
    for p in [5, 10, 30, 100]:
        for thr in [0.01, 0.02, 0.05]:
            for dr in ["fade", "follow"]:
                key = f"MO_p{p}_t{int(thr*1000)}_{dr}"
                out[key] = sig_momentum(mids, ts, period=p, threshold=thr, direction=dr)

    # RSI — 3 periods × 4 ob_thresholds × 2 dir = 24
    for p in [7, 14, 21]:
        for ob in [65, 70, 75, 80]:
            for dr in ["fade", "follow"]:
                key = f"RS_p{p}_t{ob}_{dr}"
                out[key] = sig_rsi(mids, ts, period=p, ob_threshold=ob, direction=dr)

    # Skew (mid-skew L1 imbalance proxy) — 5 thresholds × 2 dir = 10 (only if bid/ask available)
    if bids is not None and asks is not None:
        for thr in [0.10, 0.15, 0.20, 0.30, 0.40]:
            for dr in ["fade", "follow"]:
                key = f"SK_t{int(thr*100)}_{dr}"
                out[key] = sig_skew(mids, ts, bids, asks, threshold=thr, direction=dr)

        # Spread regime — 4 compression thresholds × 2 dir = 8
        for sp_thr in [0.5, 0.7, 0.9, 1.1]:
            for dr in ["fade", "follow"]:
                key = f"SR_t{int(sp_thr*100)}_{dr}"
                out[key] = sig_spread_regime(mids, ts, bids, asks,
                                             spread_pct_thr=sp_thr, direction=dr)

    return out
    # Total primitive signals: 70+48+24+24+8+24 + 10+8 = 216


# ─── Composition: variant = (primary, secondary_AND, price_filter, fees) ──

PRICE_FILTERS = ["any", "gt30", "gt50", "gt70", "lt30", "lt50", "lt70"]
FEES_FILTERS = ["free_only", "any"]
# Spread filters: applied as additional dimension (relative spread)
SPREAD_FILTERS = ["any", "tight", "wide"]


def spread_mask(mids, bids, asks, sf):
    if sf == "any":
        return np.ones(len(mids), dtype=bool)
    spr_pct = np.where(mids > 0, (asks - bids) / np.maximum(mids, 1e-9), 1.0)
    if sf == "tight":
        return spr_pct < 0.02
    if sf == "wide":
        return spr_pct > 0.05
    return np.ones(len(mids), dtype=bool)


def price_mask(mids, pf):
    if pf == "any": return np.ones(len(mids), dtype=bool)
    if pf == "gt30": return mids > 0.30
    if pf == "gt50": return mids > 0.50
    if pf == "gt70": return mids > 0.70
    if pf == "lt30": return mids < 0.30
    if pf == "lt50": return mids < 0.50
    if pf == "lt70": return mids < 0.70


# ─── Data loading (single shared) ───────────────────────────────────────

def load_resolutions():
    meta = json.load(open(META_FILE))
    clob = json.load(open(CLOB_FILE))
    out = {}
    for mid, gm in meta.items():
        if not gm.get("closed"):
            continue
        cid = gm.get("cid", "")
        if cid not in clob:
            continue
        tokens = clob[cid].get("tokens", [])
        if not tokens:
            continue
        try:
            close = datetime.fromisoformat(gm["end"].replace("Z","+00:00")).timestamp()
        except Exception:
            continue
        out[str(mid)] = {
            "yes_won": tokens[0].get("winner", False),
            "close_ts": close,
        }
    return out


def load_ticks():
    by_mkt = defaultdict(list)
    with open(TICKS_FILE) as f:
        for line in f:
            try:
                t = json.loads(line)
                ts = datetime.fromisoformat(t["ts"].replace("Z","+00:00")).timestamp()
                by_mkt[str(t["market_id"])].append((
                    ts, float(t["mid"]), float(t["bid"]),
                    float(t["ask"]), bool(t.get("fees", False)),
                ))
            except Exception:
                continue
    arrays = {}
    for mid, items in by_mkt.items():
        items.sort()
        if len(items) < 30:
            continue
        ts_arr = np.array([x[0] for x in items], dtype=np.float64)
        mid_arr = np.array([x[1] for x in items], dtype=np.float64)
        bid_arr = np.array([x[2] for x in items], dtype=np.float64)
        ask_arr = np.array([x[3] for x in items], dtype=np.float64)
        fees = items[0][4]
        arrays[mid] = {
            "ts": ts_arr, "mid": mid_arr,
            "bid": bid_arr, "ask": ask_arr, "fees": fees,
        }
    return arrays


# ─── Per-market evaluation (parallel) ───────────────────────────────────

def evaluate_market(args):
    """For one market: compute all primitive signals, then evaluate all variant
    combinations. Returns aggregated stats per variant."""
    (mid, mkt, resolution) = args

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
    n = len(mids)

    # Compute all primitive signals once (now includes microstructure)
    signals = generate_primitive_signals(mids, ts_arr, bids, asks)

    # Pre-compute price masks
    pmask = {pf: price_mask(mids, pf) for pf in PRICE_FILTERS}
    smask = {sf: spread_mask(mids, bids, asks, sf) for sf in SPREAD_FILTERS}

    # Pre-compute time-valid mask (before close)
    time_valid = ts_arr <= close_ts

    # Aggregate per variant
    results = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "n_bets": 0})

    for sig_name, sig_arr in signals.items():
      for sf, sf_mask in smask.items():
        for pf, pf_mask in pmask.items():
            for ff in FEES_FILTERS:
                if ff == "free_only" and fees_on:
                    continue
                # Combined mask: signal != 0 AND price+spread filter AND before close
                active = (sig_arr != 0) & pf_mask & sf_mask & time_valid
                if not active.any():
                    continue

                # For each active position, simulate single-shot bet (no cooldown
                # for this variant — cooldown handled by reduction below)
                # Take FIRST active signal per cooldown window (here: per variant
                # we use signal flip; simplification: process sequentially)
                indices = np.where(active)[0]
                if len(indices) == 0:
                    continue

                # Cooldown 60s minimum: skip indices closer than 60s to last
                # (simple sequential filter)
                kept = []
                last_ts = -1e9
                for idx in indices:
                    if ts_arr[idx] - last_ts >= 60:
                        kept.append(idx)
                        last_ts = ts_arr[idx]
                if not kept:
                    continue

                kept = np.array(kept)
                sigs = sig_arr[kept]
                # side: +1 → YES, -1 → NO
                # Entry: ask×(1+slip) for YES, (1-bid)×(1+slip) for NO
                yes_mask = sigs == 1
                no_mask = sigs == -1
                entries = np.zeros(len(kept))
                entries[yes_mask] = asks[kept][yes_mask] * (1 + SLIPPAGE)
                entries[no_mask] = (1 - bids[kept][no_mask]) * (1 + SLIPPAGE)

                # Filter extreme entries
                entry_ok = (entries >= 0.05) & (entries <= 0.95)
                if not entry_ok.any():
                    continue
                kept = kept[entry_ok]
                sigs = sigs[entry_ok]
                entries = entries[entry_ok]

                # Settle: each bet wins if (side YES and yes_won) or (side NO and not yes_won)
                wins_arr = ((sigs == 1) & yes_won) | ((sigs == -1) & (not yes_won))
                shares = BET_USD / entries
                pnls = np.where(wins_arr, shares - BET_USD, -BET_USD)

                key = f"{sig_name}|p{pf}|s{sf}|f{ff}"
                results[key]["wins"] += int(wins_arr.sum())
                results[key]["losses"] += int((~wins_arr).sum())
                results[key]["pnl"] += float(pnls.sum())
                results[key]["n_bets"] += int(len(kept))

    return dict(results)


# ─── Main ───────────────────────────────────────────────────────────────

_RES = None


def _init_worker(res):
    global _RES
    _RES = res


def _worker(args):
    mid, mkt = args
    return evaluate_market((mid, mkt, _RES))


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading resolutions...")
    t0 = time.time()
    resolutions = load_resolutions()
    print(f"  {len(resolutions)} resolved markets in {time.time()-t0:.1f}s")

    print(f"[{datetime.now():%H:%M:%S}] Loading ticks...")
    t0 = time.time()
    ticks = load_ticks()
    n_total = sum(len(m["mid"]) for m in ticks.values())
    print(f"  {len(ticks)} markets, {n_total:,} ticks in {time.time()-t0:.1f}s")

    # Filter to markets with resolutions
    ticks = {mid: m for mid, m in ticks.items() if mid in resolutions}
    print(f"  {len(ticks)} markets with resolutions")

    # Estimate variants
    primitives = 198 + 18  # base + microstructure (skew + spread regime)
    variants = primitives * len(PRICE_FILTERS) * len(SPREAD_FILTERS) * len(FEES_FILTERS)
    print(f"\nVariants: {variants:,}  ({primitives} primitives × {len(PRICE_FILTERS)} price × "
          f"{len(SPREAD_FILTERS)} spread × {len(FEES_FILTERS)} fees)")

    n_workers = min(max(mp.cpu_count() - 1, 1), 4)  # cap at 4 cores
    print(f"Workers: {n_workers}")
    print(f"\n[{datetime.now():%H:%M:%S}] Running...")
    t0 = time.time()

    args = list(ticks.items())
    with mp.Pool(n_workers, initializer=_init_worker,
                 initargs=(resolutions,)) as pool:
        per_market = pool.map(_worker, args, chunksize=10)

    print(f"  Per-market eval done in {time.time()-t0:.1f}s")

    # Aggregate across markets
    print(f"[{datetime.now():%H:%M:%S}] Aggregating...")
    t0 = time.time()
    agg = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "n_bets": 0})
    for d in per_market:
        for k, v in d.items():
            agg[k]["wins"] += v["wins"]
            agg[k]["losses"] += v["losses"]
            agg[k]["pnl"] += v["pnl"]
            agg[k]["n_bets"] += v["n_bets"]
    print(f"  Aggregated {len(agg)} variants in {time.time()-t0:.1f}s")

    # Build summary
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

    # Save
    summary.sort(key=lambda s: -s["pnl"])
    with open(RESULTS_FILE, "w") as f:
        json.dump({
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "n_variants": len(summary),
            "n_markets": len(ticks),
            "slippage_pct": SLIPPAGE,
            "results": summary,
        }, f, indent=1)
    print(f"\nSaved to {RESULTS_FILE}")

    # Display top
    eligible = [s for s in summary if s["n_bets"] >= 30]
    eligible.sort(key=lambda s: -s["pnl"])
    print(f"\nVariants with ≥30 bets: {len(eligible)}")

    print(f"\n━━━ TOP 25 by PnL ━━━")
    print(f"  {'Variant':<55} {'N':>6} {'WR':>5} {'PnL':>10} {'ROI':>7}")
    for s in eligible[:25]:
        print(f"  {s['variant'][:55]:<55} {s['n_bets']:>6} "
              f"{s['wr']:>4.0f}% ${s['pnl']:>+8.2f} {s['roi_pct']:>+6.1f}%")

    print(f"\n━━━ BOTTOM 5 ━━━")
    for s in eligible[-5:]:
        print(f"  {s['variant'][:55]:<55} {s['n_bets']:>6} "
              f"{s['wr']:>4.0f}% ${s['pnl']:>+8.2f} {s['roi_pct']:>+6.1f}%")

    # Per-primitive aggregation
    print(f"\n━━━ Avg ROI by primitive prefix ━━━")
    by_prim = defaultdict(list)
    for s in eligible:
        prim = s["variant"].split("_")[0]
        by_prim[prim].append(s["roi_pct"])
    for prim, rois in sorted(by_prim.items()):
        print(f"  {prim:<6} avg {np.mean(rois):+5.1f}%, best {max(rois):+5.1f}%, "
              f"worst {min(rois):+5.1f}%, n={len(rois)}")


if __name__ == "__main__":
    main()
