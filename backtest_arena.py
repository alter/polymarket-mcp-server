#!/usr/bin/env python3
"""
Backtest arena strategies on historical tick data.

Loads bot-data/arena_ticks.jsonl and replays through arena logic.
Uses same signal/exit code as live multi_strategy.py for fair comparison.

Usage:
    python backtest_arena.py               # all strategies
    python backtest_arena.py --new         # only hybrid + autotuned_safe (fastest)
    python backtest_arena.py --limit 100   # first 100 strategies only
"""
import json, sys, time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import multi_strategy as ms
from multi_strategy import (PriceTick, TrackedMarket, StrategyState,
                             compute_signal, should_exit, calc_taker_fee)

TICKS_FILE = Path("bot-data/arena_ticks.jsonl")
RESULTS_FILE = Path("bot-data/backtest_results.json")


def load_ticks():
    """Load all ticks chronologically."""
    ticks = []
    with open(TICKS_FILE) as f:
        for line in f:
            try:
                t = json.loads(line)
                # Convert ts string to epoch
                ts_str = t.get("ts", "")
                if isinstance(ts_str, str):
                    try:
                        ts = datetime.fromisoformat(
                            ts_str.replace("Z", "+00:00")).timestamp()
                    except Exception:
                        continue
                else:
                    ts = float(ts_str)
                ticks.append({
                    "ts": ts,
                    "market_id": t["market_id"],
                    "mid": float(t["mid"]),
                    "bid": float(t["bid"]),
                    "ask": float(t["ask"]),
                    "fees": t.get("fees", False),
                })
            except Exception:
                continue
    ticks.sort(key=lambda x: x["ts"])
    return ticks


def select_strategies(mode="new"):
    """Pick strategies to backtest."""
    strats = ms.ALL_STRATEGIES
    if mode == "new":
        # Only hybrid + autotuned_safe (SL=-0.20) — the newly deployed groups
        return [s for s in strats
                if s.indicator.startswith("hybrid") or s.stop_loss == -0.20]
    if mode == "champions":
        # Top-performing families from autotune
        return [s for s in strats
                if s.indicator in ("wavelet_mr", "wavelet_ms", "zscore",
                                   "bollinger", "mean_rev_sma")
                and s.fee_free_only
                and s.stop_loss <= -0.20]
    if mode.startswith("limit:"):
        n = int(mode.split(":")[1])
        return strats[:n]
    return strats


def run_backtest(strategies, ticks, verbose=True):
    """Replay ticks through strategies. Returns per-strategy results."""
    states = [StrategyState(s.params if hasattr(s, 'params') else s)
              for s in strategies]

    # Build market registry from first occurrence
    markets: dict = {}
    market_fee_info: dict = {}  # cache of is_fee

    # Track equity curves: strategy_id -> [(ts, equity)]
    equity_curves: dict = defaultdict(list)
    trade_logs: dict = defaultdict(list)

    t_start = ticks[0]["ts"]
    t_end = ticks[-1]["ts"]
    duration_h = (t_end - t_start) / 3600

    if verbose:
        print(f"Replaying {len(ticks):,} ticks over {duration_h:.1f}h "
              f"across {len(set(t['market_id'] for t in ticks))} markets")
        print(f"  on {len(states)} strategies")

    MAX_CONCURRENT = 20
    last_progress = 0

    for idx, tick in enumerate(ticks):
        mid = tick["market_id"]

        if mid not in markets:
            markets[mid] = TrackedMarket(
                market_id=mid, question=f"mkt_{mid}",
                token_yes=f"{mid}_y", token_no=f"{mid}_n",
                end_date="", volume_24h=0.0,
                fees_enabled=tick.get("fees", False),
                fee_type="free" if not tick.get("fees", False) else "sports_fees_v2",
            )

        mkt = markets[mid]
        mkt.ticks.append(PriceTick(
            ts=tick["ts"], mid=tick["mid"],
            bid=tick["bid"], ask=tick["ask"],
        ))

        if len(mkt.ticks) < 5:
            continue

        last = mkt.ticks[-1]
        prices = [t.mid for t in mkt.ticks]
        fee_type = mkt.fee_type if mkt.fees_enabled else None

        for strat in states:
            # Fee-free filter
            if strat.params.fee_free_only and mkt.fees_enabled:
                if mid not in strat.positions:
                    continue

            # Exit check
            if mid in strat.positions:
                pos = strat.positions[mid]
                entry = pos["entry_price"]
                current = last.mid if pos["side"] == "YES" else (1.0 - last.mid)
                if entry > 0:
                    pnl_pct = (current - entry) / entry
                    if strat.params.stop_loss > -0.90 and pnl_pct <= strat.params.stop_loss:
                        strat.close_position(mid, current, "stop_loss")
                        continue
                    if pnl_pct >= strat.params.take_profit:
                        strat.close_position(mid, current, "take_profit")
                        continue
                    er = should_exit(strat.params, pos, prices)
                    if er:
                        strat.close_position(mid, current, er)
                        continue
                continue

            # Entry check
            signal = compute_signal(strat.params, prices)
            if not signal:
                continue

            if strat.params.side_bias == "long" and signal != "buy":
                continue
            if strat.params.side_bias == "short" and signal != "sell":
                continue

            spread_pct = (last.ask - last.bid) / last.mid if last.mid > 0 else 1
            if spread_pct > 0.10:
                continue
            rt_fee = calc_taker_fee(1.0, last.mid, fee_type) * 2
            if rt_fee > 0.03:
                continue

            if len(strat.positions) >= MAX_CONCURRENT:
                continue

            if signal == "buy":
                strat.open_position(mid, f"mkt_{mid}", "YES", f"{mid}_y",
                    last.mid, fee_type, f"{strat.params.indicator}_buy")
            else:
                strat.open_position(mid, f"mkt_{mid}", "NO", f"{mid}_n",
                    1.0 - last.mid, fee_type, f"{strat.params.indicator}_sell")

        # Progress
        if verbose and idx - last_progress > 25000:
            last_progress = idx
            print(f"  [{idx}/{len(ticks)}] {idx*100//len(ticks)}%")

    # Close remaining positions at last observed price (mark-to-market)
    for strat in states:
        for mid in list(strat.positions.keys()):
            pos = strat.positions[mid]
            if mid in markets and markets[mid].ticks:
                last = markets[mid].ticks[-1]
                current = last.mid if pos["side"] == "YES" else (1.0 - last.mid)
                strat.close_position(mid, current, "backtest_end")

    # Aggregate results
    results = []
    for strat in states:
        trades = strat.history
        if not trades:
            continue
        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] <= 0 and t["pnl"] != 0)
        total_pnl = sum(t["pnl"] for t in trades)
        gross_w = sum(t["pnl"] for t in trades if t["pnl"] > 0)
        gross_l = abs(sum(t["pnl"] for t in trades if t["pnl"] <= 0))
        pf = gross_w / max(gross_l, 0.01)

        # Max DD from equity curve
        eq = 1000.0
        peak = 1000.0
        max_dd = 0.0
        for t in sorted(trades, key=lambda x: x.get("closed_at", 0)):
            eq += t["pnl"]
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

        results.append({
            "id": strat.params.id, "name": strat.params.name,
            "indicator": strat.params.indicator,
            "final_equity": round(strat.balance, 2),
            "trades": len(trades),
            "wins": wins, "losses": losses,
            "wr": round(wins / len(trades) * 100, 1),
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(total_pnl / len(trades), 3),
            "pf": round(pf, 2),
            "max_dd_pct": round(max_dd * 100, 2),
            "fee_free": strat.params.fee_free_only,
            "sl": strat.params.stop_loss,
            "tp": strat.params.take_profit,
            "period": strat.params.period,
            "entry_param": strat.params.entry_param,
        })

    return results, duration_h


def print_summary(results, duration_h):
    """Pretty-print results by cluster."""
    from collections import defaultdict
    by_ind = defaultdict(list)
    for r in results:
        ind = r["indicator"]
        if ind.startswith("forest") and "_meta" in ind: ind = "forest_meta"
        elif ind.startswith("forest"): ind = "forest"
        elif ind.startswith("hybrid"):
            ind = "hybrid_safe" if r["sl"] == -0.20 else "hybrid"
        elif ind == "wavelet_mr" and r["sl"] == -0.20:
            ind = "wavelet_safe"
        elif ind == "zscore" and r["sl"] == -0.20:
            ind = "zscore_safe"
        elif ind == "bollinger" and r["sl"] == -0.20:
            ind = "bollinger_safe"
        elif ind == "mean_rev_ema" and r["sl"] == -0.20:
            ind = "mean_rev_ema_safe"
        by_ind[ind].append(r)

    print(f"\n{'='*80}")
    print(f"BACKTEST RESULTS — {duration_h:.1f}h ({duration_h/24:.2f}d) of ticks")
    print(f"{'='*80}")
    print(f"Start:        $1,000 per strategy")
    print(f"Position:     $50/trade (as live)")
    print(f"Max concur:   20 positions/strategy")
    print()
    print(f"{'Cluster':<25} {'N':>4} {'Prof%':>6} {'AvgEq':>8} {'BestEq':>9} "
          f"{'Trades':>8} {'Avg$':>8} {'PF':>6} {'MaxDD':>7}")
    print("─"*100)

    for ind in sorted(by_ind.keys(), key=lambda k: -max(r["final_equity"] for r in by_ind[k])):
        g = by_ind[ind]
        n = len(g)
        prof = sum(1 for r in g if r["final_equity"] > 1000)
        avg_eq = sum(r["final_equity"] for r in g) / n
        best = max(g, key=lambda r: r["final_equity"])
        total_tr = sum(r["trades"] for r in g)
        avg_trade_pnl = sum(r["avg_pnl"] for r in g) / n
        avg_pf = sum(r["pf"] for r in g) / n
        max_dd = max(r["max_dd_pct"] for r in g)
        print(f"{ind:<25} {n:>4} {prof*100//n:>5}% ${avg_eq:>6.0f} ${best['final_equity']:>7.0f} "
              f"{total_tr:>8} ${avg_trade_pnl:>+6.2f} {avg_pf:>6.2f} {max_dd:>6.1f}%")

    # Top 15 by final equity
    print(f"\n🏆 TOP 15 strategies by final equity:")
    results.sort(key=lambda r: -r["final_equity"])
    for r in results[:15]:
        print(f"  {r['name'][:50]:<50}  ${r['final_equity']:>7.2f} ({r['trades']:>4} trades, "
              f"WR={r['wr']:>4.1f}%, PF={r['pf']:.2f}, maxDD={r['max_dd_pct']:.1f}%)")

    # Worst 5
    print(f"\n💀 WORST 5 strategies:")
    for r in results[-5:]:
        print(f"  {r['name'][:50]:<50}  ${r['final_equity']:>7.2f} ({r['trades']:>4} trades, "
              f"WR={r['wr']:>4.1f}%, PF={r['pf']:.2f}, maxDD={r['max_dd_pct']:.1f}%)")


def main():
    mode = "new"
    if "--all" in sys.argv:
        mode = "all"
    elif "--champions" in sys.argv:
        mode = "champions"
    for a in sys.argv:
        if a.startswith("--limit"):
            mode = f"limit:{a.split('=')[1] if '=' in a else sys.argv[sys.argv.index(a)+1]}"

    print(f"Mode: {mode}")
    print("Loading ticks...")
    t0 = time.time()
    ticks = load_ticks()
    print(f"  Loaded {len(ticks):,} ticks in {time.time()-t0:.1f}s")

    strategies = select_strategies(mode)
    print(f"  Selected {len(strategies)} strategies")

    t0 = time.time()
    results, duration_h = run_backtest(strategies, ticks, verbose=True)
    print(f"\nBacktest complete in {time.time()-t0:.1f}s")

    print_summary(results, duration_h)

    # Save
    with open(RESULTS_FILE, "w") as f:
        json.dump({
            "mode": mode,
            "duration_hours": duration_h,
            "n_ticks": len(ticks),
            "n_strategies": len(strategies),
            "results": results,
        }, f, indent=2)
    print(f"\nSaved to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
