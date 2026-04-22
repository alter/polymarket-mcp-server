#!/usr/bin/env python3
"""
100K experiment backtester for Polymarket.
Vectorized: precompute all EMA arrays, then sweep parameters.
No look-ahead bias: signal on bar T, execute on bar T+1.
Buy at ask[T+1], sell at bid[T+1].
"""

import itertools
import json
import os
import subprocess
import sys
import time
from typing import Dict, List, Optional

import numpy as np

# ── Constants ──
FEE_RATES = {
    "sports_fees_v2": 0.03, "culture_fees": 0.05, "finance_fees": 0.04,
    "finance_prices_fees": 0.04, "politics_fees": 0.04, "economics_fees": 0.05,
    "crypto_fees": 0.072, "mentions_fees": 0.04, "tech_fees": 0.04,
}
STARTING_BALANCE = 1000.0
MAX_POSITIONS = 20


def load_data():
    """Load tick data + metadata. Returns list of market dicts."""
    meta = {}
    meta_path = os.path.join("bot-data", "market_metadata.json")
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)

    raw = {}
    with open(os.path.join("bot-data", "smart_prices.jsonl")) as f:
        for line in f:
            r = json.loads(line.strip())
            mid = r.get("market", "")
            if not mid or r.get("bid", 0) <= 0 or r.get("ask", 0) <= 0:
                continue
            raw.setdefault(mid, []).append(r)

    markets = []
    for mid, ticks in raw.items():
        ticks.sort(key=lambda t: t["ts"])
        if len(ticks) < 10:
            continue
        m = meta.get(mid, {})
        fee_type = m.get("fee_type")
        question = m.get("question", "")
        has_vs = " vs." in question or " vs " in question

        mids = np.array([t.get("mid", (t["bid"]+t["ask"])/2) for t in ticks])
        bids = np.array([t["bid"] for t in ticks])
        asks = np.array([t["ask"] for t in ticks])

        markets.append({
            "id": mid, "question": question,
            "fee_type": fee_type, "has_vs": has_vs,
            "mids": mids, "bids": bids, "asks": asks,
            "n": len(ticks),
        })
    return markets


def precompute_emas(markets, periods):
    """Precompute EMA arrays for all markets and periods.
    Returns dict: (market_idx, period) -> numpy array of EMA values.
    EMA[i] is computed using bars 0..i only (no look-ahead).
    EMA values for bars < period are set to NaN (warm-up).
    """
    ema_cache = {}
    for mi, mkt in enumerate(markets):
        mids = mkt["mids"]
        n = mkt["n"]
        for p in periods:
            ema = np.full(n, np.nan)
            if n < p:
                ema_cache[(mi, p)] = ema
                continue
            # Seed with SMA of first p bars
            sma = np.mean(mids[:p])
            ema[p - 1] = sma
            alpha = 2.0 / (p + 1)
            for i in range(p, n):
                ema[i] = alpha * mids[i] + (1 - alpha) * ema[i - 1]
            ema_cache[(mi, p)] = ema
    return ema_cache


def calc_fee(size_usd, price, fee_type):
    if not fee_type:
        return 0.0
    rate = FEE_RATES.get(fee_type, 0.05)
    return size_usd * rate * (1.0 - price)


def run_experiment(config, markets, ema_cache):
    """
    Run one backtest experiment.

    Bar-by-bar for each market independently (then merge via timeline).
    Actually: process each market's bars sequentially since positions are per-market.

    Flow per market bar T:
      1) Execute pending order from T-1 at bar T prices (buy@ask, sell@bid)
      2) EMA already precomputed — just read ema_cache
      3) Check exit signals at bar T → create pending sell for T+1
      4) Check entry signals at bar T → create pending buy for T+1
    """
    strat = config["strategy_type"]
    ema_period = config["ema_period"]
    entry_dev = config["entry_dev"]
    exit_dev = config["exit_dev"]
    sl_pct = config.get("stop_loss_pct")
    sl_usd = config.get("stop_loss_usd")
    tp_pct = config.get("take_profit_pct")
    tp_usd = config.get("take_profit_usd")
    pos_usd = config.get("position_usd", 50.0)
    mfilter = config.get("market_filter", "all")

    balance = STARTING_BALANCE
    total_positions = 0  # count across all markets
    total_fees = 0.0
    trade_pnls = []
    peak_eq = STARTING_BALANCE
    max_dd = 0.0

    # Track per-market positions (only one at a time)
    # Process each market independently (bar by bar)
    for mi, mkt in enumerate(markets):
        # Apply filter
        ft = mkt["fee_type"]
        if mfilter == "fee_free" and ft is not None:
            continue
        if mfilter == "no_vs" and mkt["has_vs"]:
            continue
        if mfilter == "fee_free_no_vs" and (ft is not None or mkt["has_vs"]):
            continue

        ema_arr = ema_cache.get((mi, ema_period))
        if ema_arr is None:
            continue

        mids = mkt["mids"]
        bids = mkt["bids"]
        asks = mkt["asks"]
        n = mkt["n"]

        # Per-market state
        pending = None   # "buy" or "sell"
        in_position = False
        entry_price = 0.0
        shares = 0.0
        cost = 0.0

        for t in range(n):
            bar_mid = mids[t]
            bar_bid = bids[t]
            bar_ask = asks[t]
            ema_val = ema_arr[t]

            # STEP 1: Execute pending order from bar T-1
            if pending == "buy" and not in_position:
                # Check global position count limit
                if total_positions < MAX_POSITIONS:
                    exec_price = bar_ask
                    if 0 < exec_price < 1.0:
                        fee = calc_fee(pos_usd, exec_price, ft)
                        total_cost = pos_usd + fee
                        if total_cost <= balance:
                            shares = pos_usd / exec_price
                            entry_price = exec_price
                            cost = total_cost
                            balance -= total_cost
                            total_fees += fee
                            in_position = True
                            total_positions += 1
                pending = None

            elif pending == "sell" and in_position:
                exec_price = bar_bid
                if exec_price > 0:
                    gross = shares * exec_price
                    fee = calc_fee(gross, exec_price, ft)
                    net = gross - fee
                    pnl = net - cost
                    balance += net
                    total_fees += fee
                    trade_pnls.append(pnl)
                    in_position = False
                    total_positions -= 1
                pending = None

            # Skip if EMA not ready (warmup)
            if np.isnan(ema_val):
                continue

            # STEP 2: Check exits
            if in_position and pending is None:
                pnl_pct = (bar_bid - entry_price) / entry_price if entry_price > 0 else 0
                pnl_usd_now = shares * bar_bid - cost

                should_exit = False

                # Stop loss
                if sl_pct is not None and pnl_pct <= sl_pct:
                    should_exit = True
                elif sl_usd is not None and pnl_usd_now <= -abs(sl_usd):
                    should_exit = True

                # Take profit
                if not should_exit:
                    if tp_pct is not None and pnl_pct >= tp_pct:
                        should_exit = True
                    elif tp_usd is not None and pnl_usd_now >= tp_usd:
                        should_exit = True

                # EMA-based exit
                if not should_exit and ema_val > 0:
                    dev = (bar_mid - ema_val) / ema_val
                    if strat == "mean_rev" and dev >= exit_dev:
                        should_exit = True
                    elif strat == "momentum" and dev <= -exit_dev:
                        should_exit = True

                if should_exit:
                    pending = "sell"

            # STEP 3: Check entries
            if not in_position and pending is None and total_positions < MAX_POSITIONS:
                if ema_val > 0 and bar_mid > 0:
                    dev = (bar_mid - ema_val) / ema_val
                    enter = False
                    if strat == "mean_rev" and dev <= -entry_dev:
                        enter = True
                    elif strat == "momentum" and dev >= entry_dev:
                        enter = True
                    if enter:
                        pending = "buy"

            # Drawdown tracking (approximate: only this market's position value)
            eq = balance
            if in_position:
                eq += shares * bar_bid
            if eq > peak_eq:
                peak_eq = eq
            dd = (peak_eq - eq) / peak_eq if peak_eq > 0 else 0
            if dd > max_dd:
                max_dd = dd

        # End of market data: close any open position at last bid
        if in_position:
            last_bid = bids[-1]
            gross = shares * last_bid
            fee = calc_fee(gross, last_bid, ft)
            net = gross - fee
            pnl = net - cost
            balance += net
            total_fees += fee
            trade_pnls.append(pnl)
            in_position = False
            total_positions -= 1

    # Compute metrics
    total_trades = len(trade_pnls)
    wins = sum(1 for p in trade_pnls if p > 0)
    losses = sum(1 for p in trade_pnls if p <= 0)
    gross_win = sum(p for p in trade_pnls if p > 0)
    gross_loss = abs(sum(p for p in trade_pnls if p < 0))

    return {
        "equity": round(balance, 2),
        "pnl": round(balance - STARTING_BALANCE, 2),
        "trades": total_trades,
        "wins": wins,
        "losses": losses,
        "fees": round(total_fees, 2),
        "max_drawdown": round(max_dd, 4),
        "win_rate": round(wins / total_trades, 4) if total_trades else 0,
        "avg_pnl": round(sum(trade_pnls) / total_trades, 4) if total_trades else 0,
        "best_trade": round(max(trade_pnls), 4) if trade_pnls else 0,
        "worst_trade": round(min(trade_pnls), 4) if trade_pnls else 0,
        "profit_factor": round(gross_win / gross_loss, 4) if gross_loss > 0 else (999 if gross_win > 0 else 0),
    }


def generate_experiments():
    """Generate ~100K experiment configs."""
    experiments = []

    # BLOCK 1: Main grid (mean_rev + momentum)
    # 2 × 8 × 8 × 3 × 8 × 8 × 4 = 98,304
    for strat, ema, entry, exit_r, sl, tp, filt in itertools.product(
        ["mean_rev", "momentum"],
        [3, 5, 7, 10, 15, 20, 30, 50],
        [0.003, 0.005, 0.007, 0.01, 0.015, 0.02, 0.03, 0.05],
        [0.0, 0.5, 1.0],
        [None, -0.03, -0.05, -0.08, -0.10, -0.15, -0.20, -0.30],
        [None, 0.02, 0.03, 0.05, 0.07, 0.10, 0.15, 0.25],
        ["all", "fee_free", "no_vs", "fee_free_no_vs"],
    ):
        experiments.append({
            "strategy_type": strat, "ema_period": ema,
            "entry_dev": entry, "exit_dev": entry * exit_r,
            "stop_loss_pct": sl, "take_profit_pct": tp,
            "position_usd": 50.0, "market_filter": filt,
        })

    # BLOCK 2: Fixed $ SL/TP
    for ema, entry, sl_usd, tp_usd, filt in itertools.product(
        [5, 10, 20, 30],
        [0.01, 0.02, 0.03],
        [1, 2, 3, 5, 8, 10],
        [1, 2, 3, 5, 8, 10],
        ["all", "fee_free"],
    ):
        experiments.append({
            "strategy_type": "mean_rev", "ema_period": ema,
            "entry_dev": entry, "exit_dev": entry * 0.5,
            "stop_loss_usd": sl_usd, "take_profit_usd": tp_usd,
            "position_usd": 50.0, "market_filter": filt,
        })

    # BLOCK 3: Position sizing
    for ema, entry, sl, tp, pos, filt in itertools.product(
        [5, 10, 20],
        [0.01, 0.02, 0.03],
        [None, -0.10, -0.25],
        [None, 0.05, 0.10],
        [10, 25, 50, 75, 100, 150, 200],
        ["all", "fee_free"],
    ):
        experiments.append({
            "strategy_type": "mean_rev", "ema_period": ema,
            "entry_dev": entry, "exit_dev": entry * 0.5,
            "stop_loss_pct": sl, "take_profit_pct": tp,
            "position_usd": pos, "market_filter": filt,
        })

    return experiments


def run_worker_chunk(chunk_file, out_file):
    """Run as subprocess: load data, load configs from chunk_file, run, save to out_file."""
    markets = load_data()
    periods = set()
    with open(chunk_file) as f:
        configs = json.load(f)
    for c in configs:
        periods.add(c["ema_period"])
    ema_cache = precompute_emas(markets, periods)

    results = []
    for c in configs:
        r = run_experiment(c, markets, ema_cache)
        r["config"] = c
        results.append(r)

    with open(out_file, "w") as f:
        json.dump(results, f)


def main():
    print("Loading tick data...")
    markets = load_data()
    print(f"Loaded {len(markets)} markets")
    for m in markets[:5]:
        print(f"  {m['id']}: {m['n']} ticks, fee={m['fee_type'] or 'FREE'}, {m['question'][:50]}")

    print("\nGenerating experiments...")
    experiments = generate_experiments()
    total = len(experiments)
    print(f"Total experiments: {total:,}")

    # Collect all needed EMA periods
    all_periods = set(c["ema_period"] for c in experiments)
    print(f"Precomputing EMAs for periods {sorted(all_periods)}...")
    t0 = time.time()
    ema_cache = precompute_emas(markets, all_periods)
    print(f"  EMA precompute: {time.time()-t0:.1f}s, {len(ema_cache)} arrays")

    # Check if we should use subprocesses
    ncpu = os.cpu_count() or 4

    if "--worker" in sys.argv:
        # Subprocess mode
        idx = sys.argv.index("--worker")
        chunk_f = sys.argv[idx + 1]
        out_f = sys.argv[idx + 2]
        run_worker_chunk(chunk_f, out_f)
        return

    if "--single" in sys.argv or ncpu <= 1:
        # Single-threaded
        print(f"Running single-threaded...")
        t0 = time.time()
        results = []
        for i, config in enumerate(experiments):
            r = run_experiment(config, markets, ema_cache)
            r["config"] = config
            results.append(r)
            if (i + 1) % 5000 == 0:
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed
                eta = (total - i - 1) / rate
                print(f"  {i+1:,}/{total:,} ({(i+1)*100//total}%) "
                      f"{elapsed:.0f}s {rate:.0f}/s ETA {eta:.0f}s")
    else:
        # Parallel via subprocesses (avoids macOS multiprocessing issues)
        print(f"Running parallel via {ncpu} subprocesses...")
        t0 = time.time()
        chunk_size = (total + ncpu - 1) // ncpu
        tmp_dir = "/tmp/backtest_100k_chunks"
        os.makedirs(tmp_dir, exist_ok=True)

        # Write chunks
        chunk_files = []
        out_files = []
        for i in range(ncpu):
            start = i * chunk_size
            end = min(start + chunk_size, total)
            if start >= total:
                break
            chunk = experiments[start:end]
            cf = os.path.join(tmp_dir, f"chunk_{i}.json")
            of = os.path.join(tmp_dir, f"result_{i}.json")
            with open(cf, "w") as f:
                json.dump(chunk, f)
            chunk_files.append(cf)
            out_files.append(of)
            print(f"  Chunk {i}: {len(chunk):,} experiments")

        # Launch subprocesses
        procs = []
        script_path = os.path.abspath(__file__)
        for cf, of in zip(chunk_files, out_files):
            p = subprocess.Popen(
                [sys.executable, script_path, "--worker", cf, of],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            procs.append(p)

        # Wait for all
        print(f"  Waiting for {len(procs)} workers...")
        for i, p in enumerate(procs):
            ret = p.wait()
            elapsed = time.time() - t0
            if ret != 0:
                stderr = p.stderr.read().decode()
                print(f"  Worker {i} FAILED (rc={ret}): {stderr[:200]}")
            else:
                print(f"  Worker {i} done ({elapsed:.0f}s)")

        # Collect results
        results = []
        for of in out_files:
            if os.path.exists(of):
                with open(of) as f:
                    results.extend(json.load(f))
                os.remove(of)

        # Cleanup
        for cf in chunk_files:
            if os.path.exists(cf):
                os.remove(cf)

    elapsed = time.time() - t0
    print(f"\nDone: {len(results):,} experiments in {elapsed:.1f}s ({len(results)/elapsed:.0f}/s)")

    # Sort by PnL descending
    results.sort(key=lambda r: -r["pnl"])

    # Save
    out_path = "bot-data/backtest_100k_results.json"
    with open(out_path, "w") as f:
        json.dump({
            "total_experiments": len(results),
            "elapsed_seconds": round(elapsed, 1),
            "results": results,
        }, f)
    fsize = os.path.getsize(out_path) / 1e6
    print(f"Saved to {out_path} ({fsize:.1f} MB)")

    # Print top/bottom
    print("\n" + "=" * 100)
    hdr = f"{'#':>5} {'PnL':>8} {'Trades':>6} {'WR':>5} {'Fees':>7} {'DD':>6} {'PF':>6} | {'Type':>8} {'EMA':>4} {'Dev':>5} {'SL':>7} {'TP':>7} {'Pos$':>5} {'Filter':>14}"
    print("TOP 30:")
    print(hdr)
    for i, r in enumerate(results[:30]):
        c = r["config"]
        sl = f"{c['stop_loss_pct']}" if c.get('stop_loss_pct') is not None else (
            f"${c['stop_loss_usd']}" if c.get('stop_loss_usd') else "off")
        tp = f"{c['take_profit_pct']}" if c.get('take_profit_pct') is not None else (
            f"${c['take_profit_usd']}" if c.get('take_profit_usd') else "off")
        print(f"{i+1:>5} {r['pnl']:>+8.1f} {r['trades']:>6} {r['win_rate']:>4.0%} "
              f"{r['fees']:>7.1f} {r['max_drawdown']:>5.1%} {r['profit_factor']:>6.2f} | "
              f"{c['strategy_type']:>8} {c['ema_period']:>4} {c['entry_dev']:>5.3f} "
              f"{sl:>7} {tp:>7} {c.get('position_usd',50):>5.0f} {c.get('market_filter','all'):>14}")

    print(f"\nBOTTOM 10:")
    print(hdr)
    for i, r in enumerate(results[-10:]):
        c = r["config"]
        sl = f"{c['stop_loss_pct']}" if c.get('stop_loss_pct') is not None else (
            f"${c['stop_loss_usd']}" if c.get('stop_loss_usd') else "off")
        tp = f"{c['take_profit_pct']}" if c.get('take_profit_pct') is not None else (
            f"${c['take_profit_usd']}" if c.get('take_profit_usd') else "off")
        print(f"{len(results)-9+i:>5} {r['pnl']:>+8.1f} {r['trades']:>6} {r['win_rate']:>4.0%} "
              f"{r['fees']:>7.1f} {r['max_drawdown']:>5.1%} {r['profit_factor']:>6.2f} | "
              f"{c['strategy_type']:>8} {c['ema_period']:>4} {c['entry_dev']:>5.3f} "
              f"{sl:>7} {tp:>7} {c.get('position_usd',50):>5.0f} {c.get('market_filter','all'):>14}")

    # Summary
    pnls = [r["pnl"] for r in results]
    profitable = sum(1 for p in pnls if p > 0)
    print(f"\n{'='*100}")
    print(f"Profitable: {profitable:,}/{len(results):,} ({profitable*100//len(results)}%)")
    print(f"Avg PnL: ${sum(pnls)/len(pnls):.2f}, Median: ${sorted(pnls)[len(pnls)//2]:.2f}")
    print(f"Best: ${max(pnls):.2f}, Worst: ${min(pnls):.2f}")

    for st in ["mean_rev", "momentum"]:
        sub = [r["pnl"] for r in results if r["config"]["strategy_type"] == st]
        if sub:
            prof = sum(1 for p in sub if p > 0)
            print(f"  {st}: {len(sub):,}, profitable {prof:,} ({prof*100//len(sub)}%), avg ${sum(sub)/len(sub):.2f}")

    for filt in ["all", "fee_free", "no_vs", "fee_free_no_vs"]:
        sub = [r["pnl"] for r in results if r["config"].get("market_filter") == filt]
        if sub:
            prof = sum(1 for p in sub if p > 0)
            print(f"  {filt}: {len(sub):,}, profitable {prof:,} ({prof*100//len(sub)}%), avg ${sum(sub)/len(sub):.2f}")


if __name__ == "__main__":
    main()
