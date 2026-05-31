#!/usr/bin/env python3
"""
Whale Fade backtest — vectorized + multiprocessing.

Backtests 4900 variants on 300K+ ticks of historical data.
Optimizations:
  - numpy vectorized spike detection (35 mask arrays per market)
  - precomputed lookback prices (5 windows × N ticks)
  - multiprocessing.Pool across variants
  - market metadata + CLOB outcomes pre-loaded once

Expected runtime: ~5-10 min for 4900 variants × 300K ticks vs hours single-threaded.
"""
import json, os, sys, time, itertools
import multiprocessing as mp
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))

DATA = Path("bot-data")
TICKS_FILE = DATA / "arena_ticks.jsonl"
META_FILE = DATA / "gamma_market_meta.json"
CLOB_FILE = DATA / "clob_cache.json"
RESULTS_FILE = DATA / "whale_fade_backtest.json"

BET_USD = 0.01
SLIPPAGE_PCT = 0.001  # 0.1% slippage on entry+exit (round-trip 0.2%)

SPIKE_THRESHOLDS = np.array([0.01, 0.015, 0.02, 0.03, 0.05, 0.08, 0.10])  # 7
LOOKBACK_MIN = [1, 2, 5, 10, 30]                                          # 5
COOLDOWN_SEC = [0, 60, 300, 900, 1800]                                     # 5
PRICE_FILTERS = ["any", "gt30", "gt50", "gt70", "lt30", "lt50", "lt70"]   # 7
DIRECTIONS = ["fade", "follow"]                                            # 2
FEES_FILTERS = ["free_only", "any"]                                        # 2
# Total: 7 × 5 × 5 × 7 × 2 × 2 = 4900 variants


def passes_price_filter(prices: np.ndarray, pf: str) -> np.ndarray:
    if pf == "any": return np.ones(len(prices), dtype=bool)
    if pf == "gt30": return prices > 0.30
    if pf == "gt50": return prices > 0.50
    if pf == "gt70": return prices > 0.70
    if pf == "lt30": return prices < 0.30
    if pf == "lt50": return prices < 0.50
    if pf == "lt70": return prices < 0.70
    return np.ones(len(prices), dtype=bool)


# ─── Data loading ─────────────────────────────────────────────────────────

def load_resolutions():
    """Build map: market_id → (yes_won, close_ts)."""
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
        yes_won = tokens[0].get("winner", False)
        try:
            close = datetime.fromisoformat(gm["end"].replace("Z","+00:00")).timestamp()
        except Exception:
            continue
        out[str(mid)] = {"yes_won": yes_won, "close_ts": close}
    return out


def load_ticks_grouped():
    """Group ticks by market_id, sorted by timestamp."""
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
        fees = items[0][4]  # constant per market
        arrays[mid] = {
            "ts": ts_arr, "mid": mid_arr,
            "bid": bid_arr, "ask": ask_arr,
            "fees": fees,
        }
    return arrays


# ─── Vectorized spike detection ──────────────────────────────────────────

def precompute_market(mkt):
    """For one market, precompute lookback prices and change arrays.
    Returns: lookback_change[lm] = (N,) array of % change vs price at (now - lm minutes).
    """
    ts = mkt["ts"]
    mid = mkt["mid"]
    n = len(ts)
    out = {}
    for lm in LOOKBACK_MIN:
        cutoff = ts - lm * 60
        # For each i, find largest j < i where ts[j] <= cutoff[i]
        # Then change[i] = (mid[i] - mid[j]) / mid[j]
        # Vectorized via searchsorted
        idx = np.searchsorted(ts, cutoff, side="right") - 1
        idx = np.clip(idx, 0, n-1)
        old_price = mid[idx]
        # Mask out cases where idx == i (no past data) or old_price is current
        valid = (idx < np.arange(n)) & (old_price > 0)
        change = np.where(valid, (mid - old_price) / np.maximum(old_price, 1e-9), 0.0)
        out[lm] = change
    return out


# ─── Per-variant simulation (parallelizable) ─────────────────────────────

def simulate_variant(args):
    """Simulate one variant on all markets. Returns (variant_idx, wins, losses, pnl, n_bets)."""
    (variant_idx, params, markets_data, resolutions) = args
    spike_thresh = params["spike_threshold"]
    lookback_min = params["lookback_min"]
    cooldown_sec = params["cooldown_sec"]
    price_filter = params["price_filter"]
    direction = params["direction"]
    fees_filter = params["fees_filter"]

    wins = 0
    losses = 0
    realized_pnl = 0.0
    n_bets = 0

    for mid, data in markets_data.items():
        if mid not in resolutions:
            continue
        if fees_filter == "free_only" and data["fees"]:
            continue

        ts = data["ts"]
        mids = data["mid"]
        asks = data["ask"]
        bids = data["bid"]
        n = len(ts)
        if n < 30:
            continue

        # Get precomputed change for this lookback
        change = data["lookback_change"][lookback_min]

        # Spike mask
        spike_mask = np.abs(change) >= spike_thresh
        # Price filter
        pf_mask = passes_price_filter(mids, price_filter)
        # Combined
        mask = spike_mask & pf_mask
        spike_indices = np.where(mask)[0]
        if len(spike_indices) == 0:
            continue

        # Walk spike indices respecting cooldown
        last_signal_ts = -1e9
        resolution = resolutions[mid]
        yes_won = resolution["yes_won"]
        close_ts = resolution["close_ts"]

        for i in spike_indices:
            t = ts[i]
            if t - last_signal_ts < cooldown_sec:
                continue
            if t > close_ts:
                continue
            last_signal_ts = t

            # Direction
            ch = change[i]
            if direction == "fade":
                side = "NO" if ch > 0 else "YES"
            else:
                side = "YES" if ch > 0 else "NO"

            # Entry: ask for YES, 1-bid for NO + slippage
            if side == "YES":
                entry = asks[i] * (1 + SLIPPAGE_PCT)
            else:
                entry = (1 - bids[i]) * (1 + SLIPPAGE_PCT)

            if entry < 0.05 or entry > 0.95:
                continue

            shares = BET_USD / entry
            won = (side == "YES" and yes_won) or (side == "NO" and not yes_won)
            pnl = (shares - BET_USD) if won else -BET_USD
            if won:
                wins += 1
            else:
                losses += 1
            realized_pnl += pnl
            n_bets += 1

    return (variant_idx, wins, losses, realized_pnl, n_bets,
            params["name"])


# ─── Main ────────────────────────────────────────────────────────────────

def make_variants():
    out = []
    idx = 0
    for st, lm, cs, pf, dr, ff in itertools.product(
            SPIKE_THRESHOLDS.tolist(), LOOKBACK_MIN, COOLDOWN_SEC,
            PRICE_FILTERS, DIRECTIONS, FEES_FILTERS):
        out.append({
            "id": idx,
            "name": f"V{idx:04d}_s{int(st*1000)}_l{lm}m_c{cs}_p{pf}_d{dr[0]}_f{ff[0]}",
            "spike_threshold": st,
            "lookback_min": lm,
            "cooldown_sec": cs,
            "price_filter": pf,
            "direction": dr,
            "fees_filter": ff,
        })
        idx += 1
    return out


_WORKER_DATA = None
_WORKER_RES = None


def _init_worker(data, res):
    global _WORKER_DATA, _WORKER_RES
    _WORKER_DATA = data
    _WORKER_RES = res


def _worker(args):
    variant_idx, params = args
    return simulate_variant((variant_idx, params, _WORKER_DATA, _WORKER_RES))


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading resolutions...")
    t0 = time.time()
    resolutions = load_resolutions()
    print(f"  {len(resolutions)} resolved markets in {time.time()-t0:.1f}s")

    print(f"[{datetime.now():%H:%M:%S}] Loading ticks...")
    t0 = time.time()
    markets_data = load_ticks_grouped()
    print(f"  {len(markets_data)} markets, "
          f"{sum(len(m['mid']) for m in markets_data.values()):,} ticks loaded in {time.time()-t0:.1f}s")

    # Filter to markets with resolution
    markets_data = {mid: m for mid, m in markets_data.items() if mid in resolutions}
    print(f"  {len(markets_data)} markets with resolutions")

    print(f"[{datetime.now():%H:%M:%S}] Precomputing lookback changes...")
    t0 = time.time()
    for mid, m in markets_data.items():
        m["lookback_change"] = precompute_market(m)
    print(f"  Done in {time.time()-t0:.1f}s")

    variants = make_variants()
    print(f"\nVariants: {len(variants)}")
    print(f"Workers: {min(max(mp.cpu_count() - 1, 1), 4)} (capped at 4)")

    print(f"\n[{datetime.now():%H:%M:%S}] Running backtest...")
    t0 = time.time()
    n_workers = min(max(mp.cpu_count() - 1, 1), 4)  # cap at 4 cores
    args = [(v["id"], v) for v in variants]
    with mp.Pool(n_workers, initializer=_init_worker,
                 initargs=(markets_data, resolutions)) as pool:
        results = pool.map(_worker, args, chunksize=50)
    print(f"  Done in {time.time()-t0:.1f}s")

    # Aggregate
    summary = []
    for r in results:
        idx, wins, losses, pnl, n, name = r
        v = variants[idx]
        total = wins + losses
        summary.append({
            "id": idx, "name": name,
            "spike_threshold": v["spike_threshold"],
            "lookback_min": v["lookback_min"],
            "cooldown_sec": v["cooldown_sec"],
            "price_filter": v["price_filter"],
            "direction": v["direction"],
            "fees_filter": v["fees_filter"],
            "wins": wins, "losses": losses,
            "n_bets": n,
            "wr": round(wins / max(total, 1) * 100, 2),
            "pnl": round(pnl, 4),
            "roi_pct": round(pnl / max(n * BET_USD, 0.01) * 100, 2) if n else 0,
        })

    with open(RESULTS_FILE, "w") as f:
        json.dump({
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "n_variants": len(variants),
            "n_markets": len(markets_data),
            "n_resolutions": len(resolutions),
            "slippage_pct": SLIPPAGE_PCT,
            "bet_usd": BET_USD,
            "results": summary,
        }, f, indent=1)
    print(f"\nSaved to {RESULTS_FILE}")

    # Display top
    summary_eligible = [s for s in summary if s["n_bets"] >= 30]
    summary_eligible.sort(key=lambda s: -s["pnl"])
    print(f"\nVariants with ≥30 bets: {len(summary_eligible)}")

    print(f"\n━━━ TOP 20 by PnL ━━━")
    print(f"  {'Variant':<55} {'N':>5} {'WR':>5} {'PnL':>10} {'ROI':>7}")
    for s in summary_eligible[:20]:
        print(f"  {s['name'][:55]:<55} {s['n_bets']:>5} "
              f"{s['wr']:>4.0f}% ${s['pnl']:>+8.4f} {s['roi_pct']:>+5.1f}%")

    print(f"\n━━━ BOTTOM 5 ━━━")
    for s in summary_eligible[-5:]:
        print(f"  {s['name'][:55]:<55} {s['n_bets']:>5} "
              f"{s['wr']:>4.0f}% ${s['pnl']:>+8.4f} {s['roi_pct']:>+5.1f}%")

    # Aggregate by parameter
    print(f"\n━━━ Average ROI by spike_threshold ━━━")
    by_st = defaultdict(list)
    for s in summary_eligible:
        by_st[s["spike_threshold"]].append(s["roi_pct"])
    for st in sorted(by_st):
        rois = by_st[st]
        print(f"  {st:.3f}: avg ROI {np.mean(rois):+.1f}% (best {max(rois):+.1f}%, "
              f"worst {min(rois):+.1f}%) n={len(rois)}")

    print(f"\n━━━ Average ROI by direction ━━━")
    by_dr = defaultdict(list)
    for s in summary_eligible:
        by_dr[s["direction"]].append(s["roi_pct"])
    for dr, rois in by_dr.items():
        print(f"  {dr}: avg ROI {np.mean(rois):+.1f}% (best {max(rois):+.1f}%) n={len(rois)}")


if __name__ == "__main__":
    main()
