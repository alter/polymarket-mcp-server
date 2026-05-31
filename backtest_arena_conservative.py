#!/usr/bin/env python3
"""
Conservative backtest with bias mitigations:

1. Entry at ASK (for YES buy) / 1-BID (for NO buy) — crosses spread realistically
2. Exit at BID (for YES close) / 1-ASK (for NO close) — crosses spread back
3. Slippage: +0.1% on each execution (both entry and exit)
4. Latency: 1-tick delay between decision and fill (uses next tick's price)
5. Resolution filter: skip ticks where mid ≤ 0.05 or ≥ 0.95 (no near-resolution data)
6. Time-split validation: 70% train / 30% holdout — metrics reported separately

Compare with backtest_arena.py (optimistic) to see robustness of strategies.
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
RESULTS_FILE = Path("bot-data/backtest_conservative.json")
RESULTS_FILE_ALL = Path("bot-data/backtest_conservative_all.json")

SLIPPAGE_PCT = 0.001          # 0.1% slippage each side
LATENCY_TICKS = 1             # 1-tick fill delay
RESOLUTION_LOW = 0.05         # skip ticks with mid ≤ 0.05
RESOLUTION_HIGH = 0.95        # skip ticks with mid ≥ 0.95
TRAIN_FRACTION = 0.70         # 70% train, 30% holdout


def load_ticks():
    ticks = []
    with open(TICKS_FILE) as f:
        for line in f:
            try:
                t = json.loads(line)
                ts_str = t.get("ts", "")
                if isinstance(ts_str, str):
                    ts = datetime.fromisoformat(
                        ts_str.replace("Z", "+00:00")).timestamp()
                else:
                    ts = float(ts_str)
                ticks.append({
                    "ts": ts, "market_id": t["market_id"],
                    "mid": float(t["mid"]), "bid": float(t["bid"]),
                    "ask": float(t["ask"]), "fees": t.get("fees", False),
                })
            except Exception:
                continue
    ticks.sort(key=lambda x: x["ts"])
    return ticks


def select_strategies(mode="new"):
    strats = ms.ALL_STRATEGIES
    if mode == "new":
        return [s for s in strats
                if s.indicator.startswith("hybrid") or s.stop_loss == -0.20]
    return strats


def run_backtest(strategies, ticks, phase_label="", apply_filters=True):
    """Replay ticks with conservative assumptions."""
    states = [StrategyState(s if hasattr(s, 'params') else s) for s in strategies]

    markets = {}
    pending_fills = defaultdict(list)  # strat_id -> [(market, side, decide_tick_idx, ...)]

    MAX_CONCURRENT = 20
    n_resolution_skipped = 0
    n_slippage_applied = 0

    for idx, tick in enumerate(ticks):
        mid = tick["market_id"]

        # Resolution filter — skip extreme prices
        if apply_filters and (tick["mid"] <= RESOLUTION_LOW or tick["mid"] >= RESOLUTION_HIGH):
            n_resolution_skipped += 1
            continue

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
            if strat.params.fee_free_only and mkt.fees_enabled:
                if mid not in strat.positions:
                    continue

            # ── EXIT: use BID for YES close, ASK for NO close + slippage ──
            if mid in strat.positions:
                pos = strat.positions[mid]
                entry = pos["entry_price"]
                # Conservative exit: cross spread
                if pos["side"] == "YES":
                    current = last.bid  # hit bid to sell YES
                else:
                    current = 1.0 - last.ask  # hit ask to sell NO back
                # Slippage
                current *= (1 - SLIPPAGE_PCT) if apply_filters else 1.0

                if entry > 0:
                    pnl_pct = (current - entry) / entry
                    if strat.params.stop_loss > -0.90 and pnl_pct <= strat.params.stop_loss:
                        strat.close_position(mid, current, "stop_loss")
                        n_slippage_applied += 1
                        continue
                    if pnl_pct >= strat.params.take_profit:
                        strat.close_position(mid, current, "take_profit")
                        n_slippage_applied += 1
                        continue
                    er = should_exit(strat.params, pos, prices)
                    if er:
                        strat.close_position(mid, current, er)
                        n_slippage_applied += 1
                        continue
                continue

            # ── ENTRY: apply latency (fill at next tick) ──
            # For simplicity: we decide at tick N, fill at tick N+1 of same market
            # Implementation: queue signal, execute when next tick arrives

            signal = compute_signal(strat.params, prices)
            if not signal:
                continue
            if strat.params.side_bias == "long" and signal != "buy": continue
            if strat.params.side_bias == "short" and signal != "sell": continue
            if len(strat.positions) >= MAX_CONCURRENT: continue

            spread_pct = (last.ask - last.bid) / last.mid if last.mid > 0 else 1
            if spread_pct > 0.10: continue
            rt_fee = calc_taker_fee(1.0, last.mid, fee_type) * 2
            if rt_fee > 0.03: continue

            # Queue for next-tick fill
            pending_fills[strat.params.id].append({
                "market_id": mid, "signal": signal,
                "decide_ts": tick["ts"], "fee_type": fee_type,
            })

        # ── Process pending fills: execute on THIS tick if decided on previous tick ──
        for sid in list(pending_fills.keys()):
            remaining = []
            for fill in pending_fills[sid]:
                if fill["market_id"] != mid:
                    remaining.append(fill)
                    continue
                # Latency check: decide_ts < this tick's ts ⇒ execute now
                if tick["ts"] <= fill["decide_ts"]:
                    remaining.append(fill)
                    continue
                # Execute
                strat = next((s for s in states if s.params.id == sid), None)
                if not strat or fill["market_id"] in strat.positions:
                    continue
                if len(strat.positions) >= MAX_CONCURRENT:
                    continue
                # Conservative fill: use ASK for YES buy, 1-BID for NO buy + slippage
                if fill["signal"] == "buy":
                    fill_price = last.ask
                    fill_price *= (1 + SLIPPAGE_PCT) if apply_filters else 1.0
                    strat.open_position(mid, f"mkt_{mid}", "YES", f"{mid}_y",
                        fill_price, fill["fee_type"], f"{strat.params.indicator}_buy")
                else:
                    fill_price = 1.0 - last.bid
                    fill_price *= (1 + SLIPPAGE_PCT) if apply_filters else 1.0
                    strat.open_position(mid, f"mkt_{mid}", "NO", f"{mid}_n",
                        fill_price, fill["fee_type"], f"{strat.params.indicator}_sell")
                n_slippage_applied += 1
            pending_fills[sid] = remaining

    # Mark-to-market remaining positions
    for strat in states:
        for mkt_id in list(strat.positions.keys()):
            pos = strat.positions[mkt_id]
            if mkt_id in markets and markets[mkt_id].ticks:
                last = markets[mkt_id].ticks[-1]
                if pos["side"] == "YES":
                    current = last.bid * (1 - SLIPPAGE_PCT)
                else:
                    current = (1.0 - last.ask) * (1 - SLIPPAGE_PCT)
                strat.close_position(mkt_id, current, "backtest_end")

    # Aggregate
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

        eq = 1000.0
        peak = 1000.0
        max_dd = 0.0
        for t in sorted(trades, key=lambda x: x.get("closed_at", 0)):
            eq += t["pnl"]
            if eq > peak: peak = eq
            dd = (peak - eq) / peak if peak > 0 else 0
            if dd > max_dd: max_dd = dd

        results.append({
            "id": strat.params.id, "name": strat.params.name,
            "indicator": strat.params.indicator,
            "final_equity": round(strat.balance, 2),
            "trades": len(trades), "wins": wins, "losses": losses,
            "wr": round(wins / len(trades) * 100, 1),
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(total_pnl / len(trades), 3),
            "pf": round(pf, 2),
            "max_dd_pct": round(max_dd * 100, 2),
            "sl": strat.params.stop_loss,
            "tp": strat.params.take_profit,
            "period": strat.params.period,
        })
    return results, n_resolution_skipped, n_slippage_applied


def print_summary(results, label, duration_h, skip_info=""):
    from collections import defaultdict
    by_ind = defaultdict(list)
    for r in results:
        ind = r["indicator"]
        if ind.startswith("hybrid"):
            ind = "hybrid_safe" if r["sl"] == -0.20 else "hybrid"
        elif ind == "wavelet_mr" and r["sl"] == -0.20: ind = "wavelet_safe"
        elif ind == "zscore" and r["sl"] == -0.20: ind = "zscore_safe"
        elif ind == "bollinger" and r["sl"] == -0.20: ind = "bollinger_safe"
        elif ind == "mean_rev_ema" and r["sl"] == -0.20: ind = "mean_rev_ema_safe"
        by_ind[ind].append(r)

    print(f"\n{'='*90}")
    print(f"{label} — {duration_h:.1f}h ({duration_h/24:.2f}d){skip_info}")
    print(f"{'='*90}")
    print(f"{'Cluster':<25} {'N':>4} {'Prof%':>6} {'AvgEq':>8} {'BestEq':>9} "
          f"{'Trades':>8} {'Avg$':>8} {'PF':>6} {'MaxDD':>7}")
    print("─"*95)
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


def main():
    mode = "new"
    if "--all" in sys.argv:
        mode = "all"
    elif "--new" in sys.argv:
        mode = "new"

    print(f"Mode: {mode}")
    print("Loading ticks...")
    t0 = time.time()
    ticks = load_ticks()
    print(f"  {len(ticks):,} ticks loaded in {time.time()-t0:.1f}s")

    strategies = select_strategies(mode)
    print(f"  {len(strategies)} strategies")

    # Train / holdout split
    split_idx = int(len(ticks) * TRAIN_FRACTION)
    train_ticks = ticks[:split_idx]
    holdout_ticks = ticks[split_idx:]
    train_start = datetime.fromtimestamp(train_ticks[0]['ts'], timezone.utc)
    train_end = datetime.fromtimestamp(train_ticks[-1]['ts'], timezone.utc)
    hold_start = datetime.fromtimestamp(holdout_ticks[0]['ts'], timezone.utc)
    hold_end = datetime.fromtimestamp(holdout_ticks[-1]['ts'], timezone.utc)
    train_h = (train_ticks[-1]['ts'] - train_ticks[0]['ts']) / 3600
    hold_h = (holdout_ticks[-1]['ts'] - holdout_ticks[0]['ts']) / 3600
    print(f"\nTrain: {train_start:%m-%d %H:%M} → {train_end:%m-%d %H:%M}  ({train_h:.1f}h, {len(train_ticks):,} ticks)")
    print(f"Hold:  {hold_start:%m-%d %H:%M} → {hold_end:%m-%d %H:%M}  ({hold_h:.1f}h, {len(holdout_ticks):,} ticks)")

    # Run TRAIN phase (for reference — shows if params generalize)
    print(f"\n━━ TRAIN PHASE ({train_h:.1f}h) ━━")
    t0 = time.time()
    train_results, train_skipped, train_fills = run_backtest(
        strategies, train_ticks, "TRAIN", apply_filters=True)
    print(f"  Done in {time.time()-t0:.1f}s. Skipped {train_skipped:,} resolution ticks.")
    print_summary(train_results, "TRAIN (conservative)", train_h,
                  f"  [resolution-skipped {train_skipped}]")

    # Run HOLDOUT phase — critical validation
    print(f"\n━━ HOLDOUT PHASE ({hold_h:.1f}h) — TRUE OUT-OF-SAMPLE TEST ━━")
    t0 = time.time()
    hold_results, hold_skipped, hold_fills = run_backtest(
        strategies, holdout_ticks, "HOLDOUT", apply_filters=True)
    print(f"  Done in {time.time()-t0:.1f}s. Skipped {hold_skipped:,} resolution ticks.")
    print_summary(hold_results, "HOLDOUT (true validation)", hold_h,
                  f"  [resolution-skipped {hold_skipped}]")

    # Save both
    output_file = RESULTS_FILE_ALL if mode == "all" else RESULTS_FILE
    with open(output_file, "w") as f:
        json.dump({
            "slippage_pct": SLIPPAGE_PCT,
            "latency_ticks": LATENCY_TICKS,
            "resolution_range_skipped": [RESOLUTION_LOW, RESOLUTION_HIGH],
            "train": {"duration_h": train_h, "ticks": len(train_ticks),
                      "resolution_skipped": train_skipped, "results": train_results},
            "holdout": {"duration_h": hold_h, "ticks": len(holdout_ticks),
                        "resolution_skipped": hold_skipped, "results": hold_results},
        }, f, indent=2)
    print(f"\nSaved to {output_file}")


if __name__ == "__main__":
    main()
