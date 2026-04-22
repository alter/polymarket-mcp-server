#!/usr/bin/env python3
"""
1M-experiment backtester for Polymarket tick data.
Run: python3 -u backtest_1m.py
"""

import json
import math
import random
import time
import itertools
from collections import defaultdict
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE = Path(__file__).parent / "bot-data"
TICK_FILE = BASE / "smart_prices.jsonl"
META_FILE = BASE / "market_metadata.json"
OUT_FILE  = BASE / "backtest_1m_results.json"

# ---------------------------------------------------------------------------
# 1. Load raw tick data
# ---------------------------------------------------------------------------
print("Loading tick data...")
raw: dict[str, list[dict]] = defaultdict(list)
with open(TICK_FILE) as f:
    for line in f:
        d = json.loads(line)
        raw[d["market"]].append(d)

# Sort each market by timestamp
for mkt in raw:
    raw[mkt].sort(key=lambda x: x["ts"])

print(f"  Markets: {len(raw)}, total ticks: {sum(len(v) for v in raw.values())}")

# ---------------------------------------------------------------------------
# 2. Load market metadata
# ---------------------------------------------------------------------------
with open(META_FILE) as f:
    meta: dict[str, dict] = json.load(f)

# ---------------------------------------------------------------------------
# 3. Precompute per-market numpy arrays
# ---------------------------------------------------------------------------
print("Precomputing arrays...")

EMA_PERIODS = [3, 5, 7, 10, 15, 20, 30, 50]

def compute_ema(arr: np.ndarray, period: int) -> np.ndarray:
    """EMA with SMA seed for first `period` bars; NaN during warm-up."""
    n = len(arr)
    out = np.full(n, np.nan)
    if n < period:
        return out
    # seed
    seed = float(np.mean(arr[:period]))
    out[period - 1] = seed
    alpha = 2.0 / (period + 1)
    for i in range(period, n):
        out[i] = arr[i] * alpha + out[i - 1] * (1.0 - alpha)
    return out

def compute_rolling_std(arr: np.ndarray, period: int) -> np.ndarray:
    """Rolling std, NaN for first period-1 bars."""
    n = len(arr)
    out = np.full(n, np.nan)
    for i in range(period - 1, n):
        out[i] = float(np.std(arr[i - period + 1 : i + 1], ddof=0))
    return out

# Market data container
market_ids = sorted(raw.keys())

# per_market[mkt] = dict with arrays and scalars
per_market: dict[str, dict] = {}

for mkt in market_ids:
    ticks = raw[mkt]
    n = len(ticks)
    mids   = np.array([t["mid"] for t in ticks], dtype=np.float64)
    bids   = np.array([t["bid"] for t in ticks], dtype=np.float64)
    asks   = np.array([t["ask"] for t in ticks], dtype=np.float64)
    spreads = asks - bids

    avg_spread = float(np.mean(spreads))

    # volatility: std of mid prices (absolute), comparable to spread
    avg_vol = float(np.std(mids, ddof=0)) if n > 1 else 0.0

    vol_gt_spread = avg_vol > avg_spread  # can profit from mean reversion

    m = meta.get(mkt, {})
    fee_type     = m.get("fee_type", None)
    fees_enabled = m.get("fees_enabled", False)
    question     = m.get("question", mkt)

    # Check if "vs" appears in question (for no_vs filter)
    is_vs = " vs" in question.lower() or " vs." in question.lower()

    # EMA arrays: keyed by period. Convert to Python lists for fast access.
    ema_arrays: dict[int, list] = {}
    std_arrays: dict[int, list] = {}
    ema_ready_idx: dict[int, int] = {}  # first valid index per period
    for p in EMA_PERIODS:
        ea = compute_ema(mids, p)
        sa = compute_rolling_std(mids, p)
        ema_arrays[p] = ea.tolist()
        std_arrays[p] = sa.tolist()
        # Find first non-NaN index
        ready = n  # default: never ready
        for ri in range(n):
            if not math.isnan(ea[ri]):
                ready = ri
                break
        ema_ready_idx[p] = ready

    per_market[mkt] = {
        "mids":   mids.tolist(),
        "bids":   bids.tolist(),
        "asks":   asks.tolist(),
        "spreads": spreads.tolist(),
        "n":      n,
        "avg_spread": avg_spread,
        "avg_vol":    avg_vol,
        "vol_gt_spread": vol_gt_spread,
        "fee_type":   fee_type,
        "fees_enabled": fees_enabled,
        "is_vs":      is_vs,
        "ema":        ema_arrays,
        "std":        std_arrays,
        "ema_ready":  ema_ready_idx,
    }

print(f"  Precomputed arrays for {len(per_market)} markets")
print(f"  Markets with vol>spread: {sum(1 for m in per_market.values() if m['vol_gt_spread'])}")

# ---------------------------------------------------------------------------
# 4. Fee calculation
# ---------------------------------------------------------------------------
FEE_RATES = {
    "sports_fees_v2":       0.03,
    "culture_fees":         0.05,
    "finance_fees":         0.04,
    "finance_prices_fees":  0.04,
    "crypto_fees":          0.072,
}

def calc_fee(size_usd: float, price: float, fee_type) -> float:
    if not fee_type:
        return 0.0
    rate = FEE_RATES.get(fee_type, 0.05)
    return size_usd * rate * (1.0 - price)

# ---------------------------------------------------------------------------
# 5. Market filter sets (precomputed)
# ---------------------------------------------------------------------------
def build_filter_sets() -> dict[str, list[str]]:
    sets = {}
    all_mkts = market_ids

    sets["all"] = all_mkts
    sets["fee_free"] = [m for m in all_mkts if not per_market[m]["fees_enabled"]]
    sets["no_vs"]    = [m for m in all_mkts if not per_market[m]["is_vs"]]
    sets["fee_free_no_vs"] = [
        m for m in all_mkts
        if not per_market[m]["fees_enabled"] and not per_market[m]["is_vs"]
    ]
    sets["vol_gt_spread"] = [m for m in all_mkts if per_market[m]["vol_gt_spread"]]
    sets["vol_gt_spread_free"] = [
        m for m in all_mkts
        if per_market[m]["vol_gt_spread"] and not per_market[m]["fees_enabled"]
    ]
    return sets

filter_sets = build_filter_sets()
for k, v in filter_sets.items():
    print(f"  Filter '{k}': {len(v)} markets")

# ---------------------------------------------------------------------------
# 6. Backtest engine
# ---------------------------------------------------------------------------
MAX_POSITIONS = 20

def run_experiment(params: dict) -> dict:
    strategy    = params["strategy"]
    mkt_filter  = params["market_filter"]
    position_usd = params["position_usd"]
    sl          = params["sl"]   # None or negative float e.g. -0.05
    tp          = params["tp"]   # None or positive float e.g. 0.05

    markets = filter_sets.get(mkt_filter, [])
    if not markets:
        return {"pnl": 0.0, "trades": 0, "wins": 0, "losses": 0, "fees": 0.0,
                "max_drawdown": 0.0, "win_rate": 0.0, "profit_factor": 0.0,
                "params": params}

    # Strategy-specific params
    ema_period  = params.get("ema_period", 10)
    entry_dev   = params.get("entry_dev", 0.01)
    exit_dev    = params.get("exit_dev", 0.005)
    k_std       = params.get("k_std", 1.5)
    ema_fast    = params.get("ema_fast", 5)
    ema_slow    = params.get("ema_slow", 20)
    spread_thr  = params.get("spread_thr", 0.005)

    total_pnl   = 0.0
    total_fees  = 0.0
    trades      = 0
    wins        = 0
    losses      = 0
    gross_profit = 0.0
    gross_loss   = 0.0

    peak_equity = 0.0
    equity      = 0.0
    max_dd      = 0.0

    total_positions = 0  # across all markets simultaneously (simplified: count open at any time)

    for mkt in markets:
        pm = per_market[mkt]
        n       = pm["n"]
        mids    = pm["mids"]
        bids    = pm["bids"]
        asks    = pm["asks"]
        spreads = pm["spreads"]
        fee_type = pm["fee_type"]

        if strategy in ("mean_rev", "momentum", "bollinger", "spread_aware"):
            ema_arr = pm["ema"][ema_period]
            std_arr = pm["std"][ema_period]
            start_idx = pm["ema_ready"].get(ema_period, n)
        elif strategy == "dual_ema":
            ema_fast_arr = pm["ema"].get(ema_fast)
            ema_slow_arr = pm["ema"].get(ema_slow)
            if ema_fast_arr is None or ema_slow_arr is None:
                continue
            start_idx = max(pm["ema_ready"].get(ema_fast, n),
                           pm["ema_ready"].get(ema_slow, n), 1)

        # State per market
        pos_entry = 0.0
        pos_fee = 0.0
        has_pos = False
        pending_buy  = False
        pending_sell = False

        for i in range(n):
            ask_i = asks[i]
            bid_i = bids[i]

            # Step 1: Execute pending order
            if pending_buy and not has_pos:
                if total_positions < MAX_POSITIONS:
                    fee = calc_fee(position_usd, ask_i, fee_type)
                    pos_entry = ask_i
                    pos_fee = fee
                    has_pos = True
                    total_positions += 1
                pending_buy = False

            if pending_sell and has_pos:
                shares = position_usd / pos_entry
                gross = shares * bid_i
                fee_exit = calc_fee(gross, bid_i, fee_type)
                pnl = gross - fee_exit - position_usd - pos_fee
                total_pnl  += pnl
                total_fees += pos_fee + fee_exit
                trades += 1
                if pnl > 0:
                    wins += 1
                    gross_profit += pnl
                else:
                    losses += 1
                    gross_loss -= pnl
                equity += pnl
                if equity > peak_equity:
                    peak_equity = equity
                dd = peak_equity - equity
                if dd > max_dd:
                    max_dd = dd
                has_pos = False
                total_positions -= 1
                pending_sell = False

            # Step 2: Skip if before warmup
            if i < start_idx:
                continue

            mid_i = mids[i]

            # Step 3: Check exit conditions for open position
            if has_pos:
                unrealized_pct = (bid_i - pos_entry) / pos_entry

                do_exit = False
                if sl is not None and unrealized_pct <= sl:
                    do_exit = True
                if not do_exit and tp is not None and unrealized_pct >= tp:
                    do_exit = True

                if not do_exit:
                    if strategy == "mean_rev" or strategy == "spread_aware":
                        if mid_i > ema_arr[i] * (1.0 + exit_dev):
                            do_exit = True
                    elif strategy == "momentum":
                        if mid_i < ema_arr[i] * (1.0 - exit_dev):
                            do_exit = True
                    elif strategy == "bollinger":
                        if mid_i > ema_arr[i] + k_std * std_arr[i]:
                            do_exit = True
                    elif strategy == "dual_ema":
                        if ema_fast_arr[i - 1] >= ema_slow_arr[i - 1] and ema_fast_arr[i] < ema_slow_arr[i]:
                            do_exit = True

                if do_exit:
                    pending_sell = True
                    continue

            # Step 4: Check entry conditions
            if not has_pos and not pending_buy:
                if strategy == "mean_rev":
                    if mid_i < ema_arr[i] * (1.0 - entry_dev):
                        pending_buy = True
                elif strategy == "momentum":
                    if mid_i > ema_arr[i] * (1.0 + entry_dev):
                        pending_buy = True
                elif strategy == "bollinger":
                    sv = std_arr[i]
                    if sv > 0 and mid_i < ema_arr[i] - k_std * sv:
                        pending_buy = True
                elif strategy == "spread_aware":
                    if spreads[i] < spread_thr and mid_i < ema_arr[i] * (1.0 - entry_dev):
                        pending_buy = True
                elif strategy == "dual_ema":
                    if ema_fast_arr[i - 1] <= ema_slow_arr[i - 1] and ema_fast_arr[i] > ema_slow_arr[i]:
                        pending_buy = True

        # Close remaining position at end using last bid
        if has_pos:
            exit_price = bids[n - 1]
            shares = position_usd / pos_entry
            gross = shares * exit_price
            fee_exit = calc_fee(gross, exit_price, fee_type)
            pnl = gross - fee_exit - position_usd - pos_fee
            total_pnl  += pnl
            total_fees += pos_fee + fee_exit
            trades += 1
            if pnl > 0:
                wins += 1
                gross_profit += pnl
            else:
                losses += 1
                gross_loss += abs(pnl)
            equity += pnl
            dd = max(0.0, peak_equity - equity)
            if dd > max_dd:
                max_dd = dd
            total_positions = max(0, total_positions - 1)

    win_rate = wins / trades if trades > 0 else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (math.inf if gross_profit > 0 else 0.0)

    return {
        "pnl":           round(total_pnl, 6),
        "trades":        trades,
        "wins":          wins,
        "losses":        losses,
        "fees":          round(total_fees, 6),
        "max_drawdown":  round(max_dd, 6),
        "win_rate":      round(win_rate, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor != math.inf else 999.0,
        "params":        params,
    }

# ---------------------------------------------------------------------------
# 7. Parameter grid
# ---------------------------------------------------------------------------
print("\nBuilding parameter grid...")

SL_VALUES  = [None, -0.03, -0.05, -0.08, -0.10, -0.15, -0.20, -0.30]
TP_VALUES  = [None, 0.02, 0.03, 0.05, 0.07, 0.10, 0.15, 0.25]
FILTERS    = ["all", "fee_free", "no_vs", "fee_free_no_vs", "vol_gt_spread", "vol_gt_spread_free"]
POSITIONS  = [25, 50, 100]
EMA_PERIODS_GRID = [3, 5, 7, 10, 15, 20, 30, 50]

def gen_experiments():
    experiments = []

    # --- mean_rev ---
    entry_devs = [0.005, 0.01, 0.015, 0.02, 0.03, 0.05]
    exit_devs  = [0.002, 0.005, 0.01, 0.02]
    for ep, ed, xd, sl, tp, mf, pos in itertools.product(
        EMA_PERIODS_GRID, entry_devs, exit_devs, SL_VALUES, TP_VALUES, FILTERS, POSITIONS
    ):
        experiments.append({
            "strategy":     "mean_rev",
            "ema_period":   ep,
            "entry_dev":    ed,
            "exit_dev":     xd,
            "sl":           sl,
            "tp":           tp,
            "market_filter": mf,
            "position_usd": pos,
        })

    # --- momentum ---
    for ep, ed, xd, sl, tp, mf, pos in itertools.product(
        EMA_PERIODS_GRID, entry_devs, exit_devs, SL_VALUES, TP_VALUES, FILTERS, POSITIONS
    ):
        experiments.append({
            "strategy":     "momentum",
            "ema_period":   ep,
            "entry_dev":    ed,
            "exit_dev":     xd,
            "sl":           sl,
            "tp":           tp,
            "market_filter": mf,
            "position_usd": pos,
        })

    # --- bollinger ---
    k_vals = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]
    for ep, k, sl, tp, mf, pos in itertools.product(
        EMA_PERIODS_GRID, k_vals, SL_VALUES, TP_VALUES, FILTERS, POSITIONS
    ):
        experiments.append({
            "strategy":     "bollinger",
            "ema_period":   ep,
            "k_std":        k,
            "sl":           sl,
            "tp":           tp,
            "market_filter": mf,
            "position_usd": pos,
        })

    # --- spread_aware ---
    spread_thrs = [0.003, 0.005, 0.008, 0.01, 0.015]
    for ep, ed, xd, st, sl, tp, mf, pos in itertools.product(
        EMA_PERIODS_GRID, entry_devs, exit_devs, spread_thrs, SL_VALUES, TP_VALUES, FILTERS, POSITIONS
    ):
        experiments.append({
            "strategy":     "spread_aware",
            "ema_period":   ep,
            "entry_dev":    ed,
            "exit_dev":     xd,
            "spread_thr":   st,
            "sl":           sl,
            "tp":           tp,
            "market_filter": mf,
            "position_usd": pos,
        })

    # --- dual_ema ---
    fast_periods = [3, 5, 7, 10]
    slow_periods = [15, 20, 30, 50]
    for ef, es, sl, tp, mf, pos in itertools.product(
        fast_periods, slow_periods, SL_VALUES, TP_VALUES, FILTERS, POSITIONS
    ):
        experiments.append({
            "strategy":     "dual_ema",
            "ema_fast":     ef,
            "ema_slow":     es,
            "sl":           sl,
            "tp":           tp,
            "market_filter": mf,
            "position_usd": pos,
        })

    return experiments

experiments = gen_experiments()
total = len(experiments)
print(f"  Generated {total:,} experiments")

TARGET = 1_000_000
MAX    = 1_500_000
MIN    = 800_000

if total > MAX:
    print(f"  Sampling down to {TARGET:,}...")
    random.shuffle(experiments)
    experiments = experiments[:TARGET]
elif total < MIN:
    # Expand by repeating with slight param jitter - just duplicate for now
    print(f"  Under {MIN:,}, duplicating to reach target...")
    extra = TARGET - total
    experiments = experiments + random.choices(experiments, k=extra)
    random.shuffle(experiments)

print(f"  Running {len(experiments):,} experiments")

# ---------------------------------------------------------------------------
# 8. Run experiments (single-threaded, numpy precomputed)
# ---------------------------------------------------------------------------
print("\nRunning backtest...")
results = []
start_time = time.time()
n_exp = len(experiments)

for idx, params in enumerate(experiments):
    r = run_experiment(params)
    results.append(r)

    if (idx + 1) % 10_000 == 0:
        elapsed = time.time() - start_time
        rate = (idx + 1) / elapsed
        remaining = (n_exp - idx - 1) / rate
        print(f"  [{idx+1:>8,}/{n_exp:,}] rate={rate:.0f}/s  eta={remaining/60:.1f}m  "
              f"best_pnl={max(r2['pnl'] for r2 in results[-10000:]):.4f}")

elapsed = time.time() - start_time
print(f"\nDone in {elapsed:.1f}s  ({len(results)/elapsed:.0f} exp/s)")

# ---------------------------------------------------------------------------
# 9. Analysis & output
# ---------------------------------------------------------------------------
results.sort(key=lambda x: x["pnl"], reverse=True)

print("\n" + "="*70)
print("TOP 50 EXPERIMENTS")
print("="*70)
for i, r in enumerate(results[:50]):
    p = r["params"]
    strat = p["strategy"]
    mf    = p["market_filter"]
    ep    = p.get("ema_period", f"{p.get('ema_fast')}/{p.get('ema_slow')}")
    sl    = p.get("sl")
    tp    = p.get("tp")
    ed    = p.get("entry_dev", p.get("k_std", "-"))
    pos   = p.get("position_usd")
    print(f"  #{i+1:3d}  pnl={r['pnl']:+.4f}  trades={r['trades']:3d}  "
          f"wr={r['win_rate']:.2f}  pf={r['profit_factor']:.2f}  "
          f"strat={strat:<14} filter={mf:<18} ema={ep}  "
          f"ed={ed}  sl={sl}  tp={tp}  pos=${pos}")

print("\n" + "="*70)
print("BOTTOM 20 EXPERIMENTS")
print("="*70)
for i, r in enumerate(results[-20:]):
    p = r["params"]
    strat = p["strategy"]
    mf    = p["market_filter"]
    ep    = p.get("ema_period", f"{p.get('ema_fast')}/{p.get('ema_slow')}")
    print(f"  #{i+1:3d}  pnl={r['pnl']:+.4f}  trades={r['trades']:3d}  "
          f"strat={strat:<14} filter={mf:<18} ema={ep}")

# ---------------------------------------------------------------------------
# Summary by dimension
# ---------------------------------------------------------------------------
def summarize(results, key_fn, label):
    groups = defaultdict(list)
    for r in results:
        groups[key_fn(r)].append(r["pnl"])
    rows = []
    for k, pnls in groups.items():
        avg = sum(pnls) / len(pnls)
        mx  = max(pnls)
        positive = sum(1 for p in pnls if p > 0)
        rows.append((k, avg, mx, positive, len(pnls)))
    rows.sort(key=lambda x: x[1], reverse=True)
    print(f"\n{'='*70}")
    print(f"SUMMARY BY {label}")
    print(f"{'='*70}")
    print(f"  {'Key':<28} {'Avg PnL':>10} {'Max PnL':>10} {'Win%':>6} {'Count':>8}")
    for k, avg, mx, pos, cnt in rows:
        print(f"  {str(k):<28} {avg:>+10.4f} {mx:>+10.4f} {pos/cnt*100:>5.1f}% {cnt:>8,}")

summarize(results, lambda r: r["params"]["strategy"],      "STRATEGY")
summarize(results, lambda r: r["params"]["market_filter"],  "MARKET FILTER")
summarize(results, lambda r: r["params"].get("ema_period", f"f{r['params'].get('ema_fast')}/s{r['params'].get('ema_slow')}"), "EMA PERIOD")
summarize(results, lambda r: r["params"].get("entry_dev", r["params"].get("k_std", "-")), "ENTRY DEV / K_STD")
summarize(results, lambda r: r["params"]["sl"],             "STOP LOSS")
summarize(results, lambda r: r["params"]["tp"],             "TAKE PROFIT")
summarize(results, lambda r: r["params"]["position_usd"],   "POSITION USD")

# ---------------------------------------------------------------------------
# Heatmap: strategy x market_filter (avg pnl)
# ---------------------------------------------------------------------------
print(f"\n{'='*70}")
print("HEATMAP: Strategy x Market Filter (avg PnL)")
print(f"{'='*70}")
strategies = ["mean_rev", "momentum", "bollinger", "spread_aware", "dual_ema"]
filters_short = ["all", "fee_free", "no_vs", "fee_free_no_vs", "vol_gt_spread", "vol_gt_spread_free"]

heat = defaultdict(lambda: defaultdict(list))
for r in results:
    s = r["params"]["strategy"]
    f = r["params"]["market_filter"]
    heat[s][f].append(r["pnl"])

header = f"{'Strategy':<16}" + "".join(f"{f[:8]:>12}" for f in filters_short)
print(header)
for s in strategies:
    row = f"{s:<16}"
    for f in filters_short:
        pnls = heat[s][f]
        avg = sum(pnls) / len(pnls) if pnls else 0.0
        row += f"{avg:>+12.4f}"
    print(row)

# ---------------------------------------------------------------------------
# Save results
# ---------------------------------------------------------------------------
print(f"\nSaving top 1000 results to {OUT_FILE}...")
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

save_data = {
    "meta": {
        "total_experiments": len(results),
        "elapsed_seconds":   round(elapsed, 1),
        "rate_per_second":   round(len(results) / elapsed, 0),
        "markets":           len(per_market),
        "ticks":             sum(pm["n"] for pm in per_market.values()),
    },
    "top_1000":    results[:1000],
    "bottom_100":  results[-100:],
}

with open(OUT_FILE, "w") as f:
    json.dump(save_data, f, indent=2, default=str)

print(f"Saved. Best PnL: {results[0]['pnl']:+.4f}, Worst: {results[-1]['pnl']:+.4f}")
