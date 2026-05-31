#!/usr/bin/env python3
"""
Fast backtest: numpy + multiprocessing.

Approach:
1. Load ticks → group by market → numpy arrays per market (mid/bid/ask, ts)
2. Precompute ALL indicator series ONCE per market (vectorized numpy)
3. Per strategy: walk through ticks, look up precomputed values O(1)
4. Distribute strategies across CPU cores via multiprocessing.Pool

Skips: forest_*, ensemble_* (their compute_signal calls Python sub-fns 25-45 times
per tick — too dynamic to precompute. Keep them in slow backtest if needed).
Focuses on atomic indicator strategies (mean_rev_ema, wavelet_mr, zscore, bollinger,
rsi, mean_rev_sma, hybrid_wbz_*, momentum, breakout, macd) — ~700 strategies.
"""
import json, os, sys, time
import multiprocessing as mp
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))

TICKS_FILE = Path("bot-data/arena_ticks.jsonl")
RESULTS_FILE = Path("bot-data/backtest_fast_all.json")

SLIPPAGE_PCT = 0.001
RESOLUTION_LOW = 0.05
RESOLUTION_HIGH = 0.95
TRAIN_FRACTION = 0.70


def load_ticks_to_arrays():
    """Group ticks by market into numpy arrays. Filtered for non-resolution."""
    by_mkt = defaultdict(list)
    with open(TICKS_FILE) as f:
        for line in f:
            try:
                t = json.loads(line)
                ts_str = t.get("ts", "")
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp() \
                    if isinstance(ts_str, str) else float(ts_str)
                m = t["mid"]
                if m <= RESOLUTION_LOW or m >= RESOLUTION_HIGH:
                    continue
                by_mkt[t["market_id"]].append((
                    ts, float(m), float(t["bid"]), float(t["ask"]),
                    bool(t.get("fees", False)),
                ))
            except Exception:
                continue

    arrays = {}
    for mid, items in by_mkt.items():
        items.sort(key=lambda x: x[0])
        if len(items) < 10:
            continue
        ts_arr = np.array([x[0] for x in items], dtype=np.float64)
        mid_arr = np.array([x[1] for x in items], dtype=np.float64)
        bid_arr = np.array([x[2] for x in items], dtype=np.float64)
        ask_arr = np.array([x[3] for x in items], dtype=np.float64)
        fees = items[0][4]
        arrays[mid] = {
            "ts": ts_arr, "mid": mid_arr, "bid": bid_arr, "ask": ask_arr,
            "fees": fees,
        }
    return arrays


print(f"[{datetime.now():%H:%M:%S}] Loading ticks...")
t0 = time.time()
MARKETS = load_ticks_to_arrays()
print(f"  {len(MARKETS)} markets loaded in {time.time()-t0:.1f}s "
      f"({sum(len(m['mid']) for m in MARKETS.values()):,} ticks)")


# ─── Vectorized indicators (numpy, O(N) once per market) ────────────────────

def vec_ema(prices: np.ndarray, period: int) -> np.ndarray:
    """Full EMA series. ema[0]=prices[0], ema[t] = alpha*prices[t] + (1-alpha)*ema[t-1]."""
    alpha = 2.0 / (period + 1)
    out = np.empty_like(prices)
    out[0] = prices[0]
    for i in range(1, len(prices)):
        out[i] = alpha * prices[i] + (1 - alpha) * out[i-1]
    return out


def vec_sma(prices: np.ndarray, period: int) -> np.ndarray:
    """Rolling SMA. NaN for first period-1 elements."""
    if len(prices) < period:
        return np.full_like(prices, np.nan)
    csum = np.cumsum(prices)
    out = np.full_like(prices, np.nan)
    out[period-1:] = (csum[period-1:] - np.concatenate(([0], csum[:-period]))) / period
    # Forward fill initial nan with first value
    out[:period-1] = prices[:period-1]
    return out


def vec_rsi(prices: np.ndarray, period: int = 14) -> np.ndarray:
    """Wilder-style RSI."""
    deltas = np.diff(prices, prepend=prices[0])
    gains = np.maximum(deltas, 0)
    losses = np.maximum(-deltas, 0)
    # Simple rolling mean approximation
    avg_g = np.convolve(gains, np.ones(period)/period, mode='same')
    avg_l = np.convolve(losses, np.ones(period)/period, mode='same')
    rs = avg_g / np.maximum(avg_l, 1e-9)
    rsi = 100 - (100 / (1 + rs))
    rsi[:period] = 50.0
    return rsi


def vec_zscore(prices: np.ndarray, period: int) -> np.ndarray:
    """Rolling z-score: (price - rolling mean) / rolling std."""
    if len(prices) < period:
        return np.zeros_like(prices)
    sma_arr = vec_sma(prices, period)
    # Rolling std via sum of squares
    csum_sq = np.cumsum(prices ** 2)
    rolling_mean_sq = np.full_like(prices, np.nan)
    rolling_mean_sq[period-1:] = (csum_sq[period-1:] -
        np.concatenate(([0], csum_sq[:-period]))) / period
    var = rolling_mean_sq - sma_arr ** 2
    var = np.maximum(var, 0)
    std = np.sqrt(var)
    z = np.where(std > 0, (prices - sma_arr) / std, 0.0)
    z[:period-1] = 0.0
    return z


def vec_bollinger_signal(prices: np.ndarray, period: int, std_mult: float) -> np.ndarray:
    """+1 if price < lower band, -1 if above upper, 0 otherwise."""
    if len(prices) < period:
        return np.zeros_like(prices, dtype=np.int8)
    sma_arr = vec_sma(prices, period)
    csum_sq = np.cumsum(prices ** 2)
    rolling_mean_sq = np.full_like(prices, np.nan)
    rolling_mean_sq[period-1:] = (csum_sq[period-1:] -
        np.concatenate(([0], csum_sq[:-period]))) / period
    var = np.maximum(rolling_mean_sq - sma_arr ** 2, 0)
    sd = np.sqrt(var)
    lower = sma_arr - std_mult * sd
    upper = sma_arr + std_mult * sd
    sig = np.zeros(len(prices), dtype=np.int8)
    sig[prices < lower] = 1   # buy
    sig[prices > upper] = -1  # sell
    sig[:period-1] = 0
    return sig


def vec_breakout(prices: np.ndarray, period: int) -> np.ndarray:
    """+1 at N-period high, -1 at N-period low."""
    if len(prices) < period:
        return np.zeros_like(prices, dtype=np.int8)
    out = np.zeros(len(prices), dtype=np.int8)
    for i in range(period, len(prices)):
        window = prices[i-period:i+1]
        hi, lo = window.max(), window.min()
        cur = prices[i]
        if cur >= hi * 0.999:
            out[i] = 1
        elif cur <= lo * 1.001:
            out[i] = -1
    return out


def vec_haar_divergence(prices: np.ndarray, level: int) -> np.ndarray:
    """Haar wavelet divergence at level. (price - smoothed_price) / smoothed.
    Smoothed = average over 2^level points."""
    window = 2 ** level
    if len(prices) < window + 2:
        return np.zeros_like(prices)
    sma_arr = vec_sma(prices, window)
    div = np.where(sma_arr > 0, (prices - sma_arr) / np.maximum(sma_arr, 1e-9), 0.0)
    div[:window+1] = 0.0
    return div


def vec_momentum(prices: np.ndarray, period: int) -> np.ndarray:
    if len(prices) <= period:
        return np.zeros_like(prices)
    out = np.zeros_like(prices)
    out[period:] = (prices[period:] - prices[:-period]) / np.maximum(prices[:-period], 1e-9)
    return out


# ─── Signal computation per strategy (using precomputed arrays) ─────────────

def compute_signals(strat_params, prices: np.ndarray) -> np.ndarray:
    """Returns int8 array: +1 buy, -1 sell, 0 nothing."""
    ind = strat_params.indicator
    p = strat_params.period
    e = strat_params.entry_param

    if ind == "mean_rev_ema":
        ema_arr = vec_ema(prices, p)
        dev = (prices - ema_arr) / np.maximum(np.abs(ema_arr), 1e-9)
        sig = np.zeros_like(prices, dtype=np.int8)
        sig[dev < -e] = 1
        sig[dev > e] = -1
        return sig

    elif ind == "mean_rev_sma":
        sma_arr = vec_sma(prices, p)
        dev = (prices - sma_arr) / np.maximum(np.abs(sma_arr), 1e-9)
        sig = np.zeros_like(prices, dtype=np.int8)
        sig[dev < -e] = 1
        sig[dev > e] = -1
        return sig

    elif ind == "rsi":
        rsi_arr = vec_rsi(prices, p)
        # entry_param = lo threshold, exit_param = hi threshold
        sig = np.zeros_like(prices, dtype=np.int8)
        sig[rsi_arr < e] = 1
        sig[rsi_arr > strat_params.exit_param] = -1
        return sig

    elif ind == "bollinger":
        return vec_bollinger_signal(prices, p, e)

    elif ind == "zscore":
        z = vec_zscore(prices, p)
        sig = np.zeros_like(prices, dtype=np.int8)
        sig[z < -e] = 1
        sig[z > e] = -1
        return sig

    elif ind == "breakout":
        return vec_breakout(prices, p)

    elif ind == "wavelet_mr":
        div = vec_haar_divergence(prices, p)
        sig = np.zeros_like(prices, dtype=np.int8)
        sig[div < -e] = 1
        sig[div > e] = -1
        return sig

    elif ind == "wavelet_ms":
        # Multi-scale: all levels 1..p must agree
        sigs = []
        for lv in range(1, p + 1):
            div = vec_haar_divergence(prices, lv)
            s = np.zeros_like(prices, dtype=np.int8)
            s[div < -e] = 1
            s[div > e] = -1
            sigs.append(s)
        if not sigs:
            return np.zeros_like(prices, dtype=np.int8)
        stack = np.stack(sigs)
        all_buy = np.all(stack == 1, axis=0)
        all_sell = np.all(stack == -1, axis=0)
        out = np.zeros_like(prices, dtype=np.int8)
        out[all_buy] = 1
        out[all_sell] = -1
        return out

    elif ind == "hybrid_wbz_all" or ind == "hybrid_wbz_2of3":
        # 3-vote ensemble: wavelet (level=p), BB(20,2), zscore(20,2)
        wv = vec_haar_divergence(prices, p)
        wv_sig = np.zeros_like(prices, dtype=np.int8)
        wv_sig[wv < -e] = 1
        wv_sig[wv > e] = -1
        bb_sig = vec_bollinger_signal(prices, 20, 2.0)
        z = vec_zscore(prices, 20)
        z_sig = np.zeros_like(prices, dtype=np.int8)
        z_sig[z < -2.0] = 1
        z_sig[z > 2.0] = -1
        votes_buy = (wv_sig == 1).astype(np.int8) + (bb_sig == 1).astype(np.int8) + (z_sig == 1).astype(np.int8)
        votes_sell = (wv_sig == -1).astype(np.int8) + (bb_sig == -1).astype(np.int8) + (z_sig == -1).astype(np.int8)
        out = np.zeros_like(prices, dtype=np.int8)
        if ind == "hybrid_wbz_all":
            out[votes_buy == 3] = 1
            out[votes_sell == 3] = -1
        else:  # 2of3
            out[(votes_buy >= 2) & (votes_buy > votes_sell)] = 1
            out[(votes_sell >= 2) & (votes_sell > votes_buy)] = -1
        return out

    elif ind == "momentum":
        m = vec_momentum(prices, p)
        sig = np.zeros_like(prices, dtype=np.int8)
        sig[m > e] = 1
        sig[m < -e] = -1
        return sig

    elif ind == "macd":
        # MACD histogram via EMA(fast) - EMA(slow); signal line via EMA(hist, signal_period)
        # period encodes slow; fast=12, signal=9
        slow = p
        fast = 12
        if len(prices) < slow:
            return np.zeros_like(prices, dtype=np.int8)
        ef = vec_ema(prices, fast)
        es = vec_ema(prices, slow)
        macd_line = ef - es
        sig_line = vec_ema(macd_line, 9)
        hist = macd_line - sig_line
        sig = np.zeros_like(prices, dtype=np.int8)
        sig[hist > e] = 1
        sig[hist < -e] = -1
        return sig

    return np.zeros_like(prices, dtype=np.int8)


# ─── Simulation per strategy (fast, uses precomputed signals) ───────────────

def simulate_strategy(strat_params, markets_data):
    """Simulate one strategy across all markets. Returns trade results."""
    # Per-market: precompute signals
    sl = strat_params.stop_loss
    tp = strat_params.take_profit
    fee_free = strat_params.fee_free_only

    trades = []
    open_positions = {}  # mid -> (entry_price, side, opened_idx)

    # Sort markets by timestamp (we need to iterate ticks in time order across markets).
    # Simpler approach: iterate each market independently. Since strategies don't have
    # cross-market state, this is correct for backtest purposes.

    for mid, mkt in markets_data.items():
        if fee_free and mkt["fees"]:
            continue

        prices = mkt["mid"]
        bids = mkt["bid"]
        asks = mkt["ask"]
        n = len(prices)
        if n < 30:
            continue

        # Compute signal array once
        try:
            signals = compute_signals(strat_params, prices)
        except Exception:
            continue

        in_pos = False
        side = 0  # 1=YES, -1=NO
        entry_price = 0.0
        opened_idx = 0

        # Walk ticks
        for i in range(n):
            if in_pos:
                # Exit logic: SL/TP only (skip indicator-based reversal exit
                # for speed; tests show TP/SL dominates)
                if side == 1:
                    cur = bids[i] * (1 - SLIPPAGE_PCT)
                else:
                    cur = (1.0 - asks[i]) * (1 - SLIPPAGE_PCT)
                if entry_price > 0:
                    pnl_pct = (cur - entry_price) / entry_price
                    exit_now = False
                    reason = ""
                    if sl > -0.90 and pnl_pct <= sl:
                        exit_now = True; reason = "stop_loss"
                    elif pnl_pct >= tp:
                        exit_now = True; reason = "take_profit"
                    # Signal flip exit
                    elif (side == 1 and signals[i] == -1) or (side == -1 and signals[i] == 1):
                        exit_now = True; reason = "signal_flip"
                    if exit_now:
                        # Compute PnL at $50 position
                        position_usd = strat_params.position_usd
                        shares = position_usd / max(entry_price, 1e-9)
                        pnl = shares * (cur - entry_price)
                        trades.append((float(mkt["ts"][i]), pnl, reason, side))
                        in_pos = False
            else:
                s = signals[i]
                if s == 0:
                    continue
                # Spread filter
                if prices[i] > 0 and (asks[i] - bids[i]) / prices[i] > 0.10:
                    continue
                # Entry: ask for buy, 1-bid for sell, + slippage
                if s == 1:
                    entry_price = asks[i] * (1 + SLIPPAGE_PCT)
                    if entry_price < 0.05 or entry_price > 0.95:
                        continue
                    side = 1
                else:
                    entry_price = (1.0 - bids[i]) * (1 + SLIPPAGE_PCT)
                    if entry_price < 0.05 or entry_price > 0.95:
                        continue
                    side = -1
                in_pos = True
                opened_idx = i

        # MTM close at end of data
        if in_pos and n > 0:
            if side == 1:
                cur = bids[n-1] * (1 - SLIPPAGE_PCT)
            else:
                cur = (1.0 - asks[n-1]) * (1 - SLIPPAGE_PCT)
            position_usd = strat_params.position_usd
            shares = position_usd / max(entry_price, 1e-9)
            pnl = shares * (cur - entry_price)
            trades.append((float(mkt["ts"][n-1]), pnl, "mtm_end", side))

    # Aggregate
    if not trades:
        return None
    pnls = np.array([t[1] for t in trades])
    wins = int((pnls > 0).sum())
    losses = int(((pnls <= 0) & (pnls != 0)).sum())
    total_pnl = float(pnls.sum())
    gross_w = float(pnls[pnls > 0].sum())
    gross_l = float(abs(pnls[pnls <= 0].sum()))
    pf = gross_w / max(gross_l, 0.01)

    # Max DD
    eq = 1000.0
    peak = 1000.0
    max_dd = 0.0
    sorted_trades = sorted(trades, key=lambda x: x[0])
    for _, pnl, _, _ in sorted_trades:
        eq += pnl
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    return {
        "id": strat_params.id, "name": strat_params.name,
        "indicator": strat_params.indicator,
        "final_equity": round(1000 + total_pnl, 2),
        "trades": len(trades),
        "wins": wins, "losses": losses,
        "wr": round(wins / len(trades) * 100, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / len(trades), 3),
        "pf": round(pf, 2),
        "max_dd_pct": round(max_dd * 100, 2),
        "sl": sl, "tp": tp, "period": strat_params.period,
        "entry_param": strat_params.entry_param,
    }


# ─── Multiprocessing worker ─────────────────────────────────────────────────

_WORKER_MARKETS = None


def _init_worker(markets):
    global _WORKER_MARKETS
    _WORKER_MARKETS = markets


def _worker(strat_params):
    return simulate_strategy(strat_params, _WORKER_MARKETS)


def split_markets_by_time(markets, train_frac=TRAIN_FRACTION):
    """Split each market's arrays into train and holdout slices."""
    # Find global split timestamp based on all ticks
    all_ts = []
    for m in markets.values():
        all_ts.extend(m["ts"].tolist())
    all_ts.sort()
    split_ts = all_ts[int(len(all_ts) * train_frac)]
    train_m = {}
    hold_m = {}
    for mid, m in markets.items():
        ts = m["ts"]
        train_mask = ts < split_ts
        hold_mask = ~train_mask
        if train_mask.sum() >= 30:
            train_m[mid] = {k: m[k][train_mask] if isinstance(m[k], np.ndarray) else m[k]
                            for k in m}
        if hold_mask.sum() >= 30:
            hold_m[mid] = {k: m[k][hold_mask] if isinstance(m[k], np.ndarray) else m[k]
                           for k in m}
    return train_m, hold_m, split_ts


def cluster_key(r):
    ind = r["indicator"]
    if ind.startswith("hybrid"):
        return "hybrid_safe" if r["sl"] == -0.20 else "hybrid"
    if ind == "wavelet_mr" and r["sl"] == -0.20: return "wavelet_safe"
    if ind == "zscore" and r["sl"] == -0.20: return "zscore_safe"
    if ind == "bollinger" and r["sl"] == -0.20: return "bollinger_safe"
    if ind == "mean_rev_ema" and r["sl"] == -0.20: return "mean_rev_ema_safe"
    if ind.startswith("wavelet"): return ind
    return ind


def print_summary(results, label, n_ticks):
    by_cluster = defaultdict(list)
    for r in results:
        by_cluster[cluster_key(r)].append(r)
    print(f"\n{'='*95}")
    print(f"{label}  ({n_ticks:,} ticks, {len(results)} strategies traded)")
    print(f"{'='*95}")
    print(f"{'Cluster':<25} {'N':>4} {'Prof%':>6} {'AvgEq':>8} {'BestEq':>9} "
          f"{'Trades':>8} {'Avg$':>8} {'PF':>6} {'MaxDD':>7}")
    print("─"*95)
    for c in sorted(by_cluster, key=lambda k: -max(r["final_equity"] for r in by_cluster[k])):
        g = by_cluster[c]
        n = len(g)
        prof = sum(1 for r in g if r["final_equity"] > 1000)
        avg_eq = sum(r["final_equity"] for r in g) / n
        best = max(g, key=lambda r: r["final_equity"])
        total_tr = sum(r["trades"] for r in g)
        avg_trade = sum(r["avg_pnl"] for r in g) / n
        avg_pf = sum(r["pf"] for r in g) / n
        max_dd = max(r["max_dd_pct"] for r in g)
        print(f"{c:<25} {n:>4} {prof*100//n:>5}% ${avg_eq:>6.0f} ${best['final_equity']:>7.0f} "
              f"{total_tr:>8} ${avg_trade:>+6.2f} {avg_pf:>6.2f} {max_dd:>6.1f}%")

    print(f"\nTop 15 by equity:")
    results.sort(key=lambda r: -r["final_equity"])
    for r in results[:15]:
        print(f"  {r['name'][:50]:<50}  ${r['final_equity']:>7.2f} "
              f"({r['trades']:>5} tr, WR={r['wr']:>4.1f}%, PF={r['pf']:.2f}, DD={r['max_dd_pct']:.1f}%)")


def main():
    import multi_strategy as ms
    # Filter to atomic-indicator strategies (skip forest/ensemble — complex)
    SKIP_INDICATORS = {"forest_15", "forest_25", "forest_25_super", "forest_35", "forest_45",
                       "forest_15_meta", "forest_25_meta", "forest_35_meta",
                       "ensemble_strat", "ensemble_uniform", "ensemble_weighted",
                       "ensemble_super", "mean_rev_sma"}
    strats = [s for s in ms.ALL_STRATEGIES if s.indicator not in SKIP_INDICATORS]
    # mean_rev_sma actually supported, re-add
    strats = [s for s in ms.ALL_STRATEGIES
              if s.indicator in ("mean_rev_ema", "mean_rev_sma", "rsi", "bollinger",
                                  "zscore", "breakout", "wavelet_mr", "wavelet_ms",
                                  "hybrid_wbz_all", "hybrid_wbz_2of3", "momentum", "macd")]
    print(f"\nStrategies to backtest: {len(strats)}")

    train_m, hold_m, split_ts = split_markets_by_time(MARKETS)
    train_n = sum(len(m["mid"]) for m in train_m.values())
    hold_n = sum(len(m["mid"]) for m in hold_m.values())
    split_dt = datetime.fromtimestamp(split_ts, timezone.utc)
    print(f"Split at {split_dt:%Y-%m-%d %H:%M UTC}: train={train_n:,} ticks, "
          f"holdout={hold_n:,} ticks")

    n_workers = min(max(mp.cpu_count() - 1, 1), 4)  # cap at 4 cores
    print(f"Workers: {n_workers}")

    # TRAIN
    print(f"\n[{datetime.now():%H:%M:%S}] Running TRAIN...")
    t0 = time.time()
    with mp.Pool(n_workers, initializer=_init_worker, initargs=(train_m,)) as pool:
        train_raw = pool.map(_worker, strats, chunksize=10)
    train_results = [r for r in train_raw if r is not None]
    print(f"  Done in {time.time()-t0:.1f}s")
    print_summary(train_results, "TRAIN (conservative)", train_n)

    # HOLDOUT
    print(f"\n[{datetime.now():%H:%M:%S}] Running HOLDOUT...")
    t0 = time.time()
    with mp.Pool(n_workers, initializer=_init_worker, initargs=(hold_m,)) as pool:
        hold_raw = pool.map(_worker, strats, chunksize=10)
    hold_results = [r for r in hold_raw if r is not None]
    print(f"  Done in {time.time()-t0:.1f}s")
    print_summary(hold_results, "HOLDOUT (true validation)", hold_n)

    with open(RESULTS_FILE, "w") as f:
        json.dump({
            "slippage_pct": SLIPPAGE_PCT,
            "resolution_skipped_range": [RESOLUTION_LOW, RESOLUTION_HIGH],
            "train": {"ticks": train_n, "results": train_results},
            "holdout": {"ticks": hold_n, "results": hold_results},
            "split_ts": split_ts,
        }, f, indent=2)
    print(f"\nSaved to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
