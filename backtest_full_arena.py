#!/usr/bin/env python3
"""
Full arena backtest — replays bot-data/arena_ticks.jsonl through every
strategy in multi_strategy.ALL_STRATEGIES (current 1535) and reports
per-strategy ROI / WR / max DD.

Use this BEFORE deploying new strategies to find which ones survive
historical conditions and which are pure live-arena luck.

Output: bot-data/arena_backtest_full.json with per-strategy metrics.
"""
import json, os, sys, time
from collections import defaultdict, deque
from datetime import datetime, timezone

# Import strategy library + signal logic from multi_strategy
sys.path.insert(0, ".")
import multi_strategy as ms

TICKS = "bot-data/arena_ticks.jsonl"
OUT = "bot-data/arena_backtest_full.json"

STARTING = ms.STARTING_BALANCE
HISTORY_MAXLEN = 500
# Subsample: only evaluate strategies every N-th tick per market.
# Real arena calls tick_all periodically (~once per minute), not per-tick.
# Tick density ~3-10/min/market, so EVAL_EVERY=10 ≈ 1 eval/min/market.
EVAL_EVERY = 20


class FastTick:
    __slots__ = ("ts", "mid", "bid", "ask")
    def __init__(self, ts, mid, bid, ask):
        self.ts, self.mid, self.bid, self.ask = ts, mid, bid, ask


class FastMarket:
    __slots__ = ("market_id", "ticks", "fees_enabled", "fee_type", "tick_count")
    def __init__(self, market_id, fees_on):
        self.market_id = market_id
        self.ticks = deque(maxlen=HISTORY_MAXLEN)
        self.fees_enabled = fees_on
        self.fee_type = "sports_fees_v2" if fees_on else None
        self.tick_count = 0


def _strategy_state():
    """Per-strategy mutable state."""
    return {
        "balance": STARTING,
        "positions": {},   # mid -> {entry_price, side, cost_usd, fee_type}
        "wins": 0, "losses": 0, "trades": 0,
        "realized": 0.0, "fees": 0.0,
        "peak_eq": STARTING, "min_eq": STARTING,
    }


def equity(state):
    return state["balance"] + sum(p["cost_usd"] for p in state["positions"].values())


MAX_POSITIONS = 9   # match multi_strategy production


def open_pos(state, mid, side, price, fee_type):
    fee = ms.calc_taker_fee(50.0, price, fee_type)
    cost = 50.0 + fee
    if cost > state["balance"] or len(state["positions"]) >= MAX_POSITIONS:
        return False
    state["balance"] -= cost
    state["fees"] += fee
    state["positions"][mid] = {
        "entry_price": price, "side": side, "cost_usd": cost,
        "fee_type": fee_type, "shares": 50.0 / max(price, 0.01),
        "opened_tick": state.get("_tick_count", 0),
    }
    return True


def close_pos(state, mid, exit_price, _reason):
    pos = state["positions"].pop(mid)
    gross = pos["shares"] * exit_price
    fee = ms.calc_taker_fee(gross, exit_price, pos["fee_type"])
    net = gross - fee
    pnl = net - pos["cost_usd"]
    state["balance"] += net
    state["fees"] += fee
    state["trades"] += 1
    state["realized"] += pnl
    if pnl > 0:
        state["wins"] += 1
    else:
        state["losses"] += 1
    eq = equity(state)
    if eq > state["peak_eq"]:
        state["peak_eq"] = eq
    if eq < state["min_eq"]:
        state["min_eq"] = eq


def main():
    # Focus on UNVALIDATED additions: forest_rand (500) + paid-market clones.
    # The 1023 baseline strategies are already validated via mass_backtest +
    # walkforward — re-running them is wasted compute. To force full backtest
    # set BACKTEST_ALL=1 in env.
    full = bool(os.environ.get("BACKTEST_ALL"))
    only_det = bool(os.environ.get("BACKTEST_DETERMINISTIC"))
    if full:
        strategies = [s for s in ms.ALL_STRATEGIES if not s.indicator.endswith("_meta")]
        scope = f"all non-meta ({len(strategies)})"
    elif only_det:
        # Deterministic indicators — skip forest/ensemble (random subset variance)
        DET = {"mean_rev_ema", "mean_rev_sma", "momentum", "rsi",
               "bollinger", "macd", "breakout", "zscore", "wavelet_mr",
               "wavelet_ms", "hybrid_wbz_all", "hybrid_wbz_2of3"}
        strategies = [s for s in ms.ALL_STRATEGIES if s.indicator in DET]
        scope = f"deterministic primitives ({len(strategies)})"
    else:
        strategies = [s for s in ms.ALL_STRATEGIES
                      if s.indicator == "forest_rand"
                      or (not s.fee_free_only and s.indicator in (
                          "mean_rev_ema", "wavelet_mr", "rsi", "bollinger", "breakout"))]
        scope = f"new 512 (forest_rand + paid clones, {len(strategies)})"
    print(f"Loading {len(strategies)} strategies ({scope})...")
    states = {s.id: _strategy_state() for s in strategies}

    # Streaming read
    if not os.path.exists(TICKS):
        print(f"ERROR: {TICKS} not found")
        return
    file_size = os.path.getsize(TICKS) / 1e6
    print(f"Streaming {file_size:.0f}MB of ticks from {TICKS}")

    markets = {}
    n_ticks = 0
    n_evaluations = 0
    t0 = time.time()
    last_print = t0

    with open(TICKS) as f:
        for line in f:
            try:
                d = json.loads(line)
            except Exception:
                continue
            mid_id = d.get("market_id")
            if not mid_id:
                continue
            if mid_id not in markets:
                markets[mid_id] = FastMarket(mid_id, d.get("fees", False))
            mkt = markets[mid_id]
            mkt.ticks.append(FastTick(
                d.get("ts", ""), d.get("mid", 0.0),
                d.get("bid", 0.0), d.get("ask", 0.0),
            ))
            mkt.tick_count += 1
            n_ticks += 1

            # Process strategies on THIS market only (locality)
            if len(mkt.ticks) < 5:
                continue
            # Subsample — only process every EVAL_EVERY-th tick per market
            if mkt.tick_count % EVAL_EVERY != 0:
                continue

            last = mkt.ticks[-1]
            prices = [t.mid for t in mkt.ticks]
            fee_type = mkt.fee_type if mkt.fees_enabled else None

            for strat in strategies:
                state = states[strat.id]
                # fee filter
                if strat.fee_free_only and mkt.fees_enabled:
                    if mid_id not in state["positions"]:
                        continue

                # Exit
                if mid_id in state["positions"]:
                    pos = state["positions"][mid_id]
                    current = last.mid if pos["side"] == "YES" else (1.0 - last.mid)
                    if pos["entry_price"] > 0:
                        pnl_pct = (current - pos["entry_price"]) / pos["entry_price"]
                        if strat.stop_loss > -0.90 and pnl_pct <= strat.stop_loss:
                            close_pos(state, mid_id, current, "sl")
                            continue
                        if pnl_pct >= strat.take_profit:
                            close_pos(state, mid_id, current, "tp")
                            continue
                    # should_exit() — production also checks pattern/time-based exits
                    try:
                        exit_reason = ms.should_exit(strat, pos, prices)
                    except Exception:
                        exit_reason = None
                    if exit_reason:
                        close_pos(state, mid_id, current, exit_reason)
                    continue

                # Entry
                try:
                    signal = ms.compute_signal(strat, prices)
                except Exception:
                    signal = None
                if not signal:
                    continue
                if strat.side_bias == "long" and signal != "buy":
                    continue
                if strat.side_bias == "short" and signal != "sell":
                    continue
                spread_pct = (last.ask - last.bid) / last.mid if last.mid > 0 else 1
                if spread_pct > 0.10:
                    continue
                rt_fee = ms.calc_taker_fee(1.0, last.mid, fee_type) * 2
                if rt_fee > 0.03:
                    continue
                # Skip *_meta strategies — they need ML model + meta_predict.py
                # which isn't available in pure backtest. Production code has
                # extra meta-labeling gate that we can't replicate here.
                if strat.indicator.endswith("_meta"):
                    continue
                # Open — production uses last.mid (NOT ask/bid). Match exactly.
                side = "YES" if signal == "buy" else "NO"
                price = last.mid if side == "YES" else (1 - last.mid)
                if 0.05 <= price <= 0.95:
                    open_pos(state, mid_id, side, price, fee_type)
                n_evaluations += 1

            # Progress
            if time.time() - last_print > 30:
                last_print = time.time()
                rate = n_ticks / max(time.time() - t0, 0.01)
                print(f"  {n_ticks:,} ticks processed ({rate:.0f}/s, "
                      f"{n_evaluations:,} signal evals)")

    # Final close: any open positions at last seen mid
    for mid_id, mkt in markets.items():
        if not mkt.ticks:
            continue
        last_mid = mkt.ticks[-1].mid
        for strat in strategies:
            state = states[strat.id]
            if mid_id in state["positions"]:
                pos = state["positions"][mid_id]
                current = last_mid if pos["side"] == "YES" else (1.0 - last_mid)
                close_pos(state, mid_id, current, "eos")

    # Aggregate
    rows = []
    for strat in strategies:
        st = states[strat.id]
        eq = equity(st)
        nw = st["wins"]; nl = st["losses"]; nt = st["trades"]
        wr = nw / max(nw + nl, 1) * 100
        roi = (eq - STARTING) / STARTING * 100
        peak = st["peak_eq"]
        max_dd_pct = (peak - st["min_eq"]) / peak * 100 if peak else 0
        rows.append({
            "id": strat.id, "name": strat.name,
            "indicator": strat.indicator,
            "trades": nt, "wins": nw, "losses": nl, "wr": round(wr, 1),
            "equity": round(eq, 2), "roi_pct": round(roi, 2),
            "realized": round(st["realized"], 2),
            "fees": round(st["fees"], 4),
            "peak_eq": round(peak, 2),
            "max_dd_pct": round(max_dd_pct, 2),
        })

    rows.sort(key=lambda r: -r["roi_pct"])
    elapsed = time.time() - t0
    out = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "n_ticks": n_ticks, "n_strategies": len(strategies),
        "n_evals": n_evaluations,
        "elapsed_sec": round(elapsed, 1),
        "results": rows,
    }
    with open(OUT, "w") as f:
        json.dump(out, f, indent=1)
    print(f"\nDone in {elapsed:.0f}s ({n_ticks/elapsed:.0f} ticks/s)")
    print(f"Saved to {OUT}\n")

    # Summary
    profitable = [r for r in rows if r["roi_pct"] > 0]
    losers = [r for r in rows if r["roi_pct"] < -10]
    print(f"Profitable (ROI>0): {len(profitable)} / {len(rows)} ({len(profitable)/len(rows)*100:.0f}%)")
    print(f"Losers (ROI<-10): {len(losers)}")

    print(f"\nTOP 15 by ROI:")
    print(f"{'name':<55} {'ROI':>7} {'WR':>4} {'trades':>6} {'maxDD':>5}")
    for r in rows[:15]:
        print(f"{r['name'][:55]:<55} {r['roi_pct']:>+6.1f}% {r['wr']:>3.0f}% "
              f"{r['trades']:>6} {r['max_dd_pct']:>4.0f}%")

    print(f"\nBOTTOM 10 by ROI:")
    for r in rows[-10:]:
        print(f"{r['name'][:55]:<55} {r['roi_pct']:>+6.1f}% {r['wr']:>3.0f}% "
              f"{r['trades']:>6} {r['max_dd_pct']:>4.0f}%")

    # By indicator family
    by_ind = defaultdict(list)
    for r in rows:
        by_ind[r["indicator"]].append(r["roi_pct"])
    print(f"\nBy indicator family (mean ROI):")
    fam_rows = []
    for ind, rois in by_ind.items():
        fam_rows.append((ind, sum(rois)/len(rois), len(rois),
                         sum(1 for x in rois if x > 0)))
    fam_rows.sort(key=lambda r: -r[1])
    for ind, mean_roi, n, n_prof in fam_rows:
        print(f"  {ind:<25} mean={mean_roi:>+6.1f}%  n={n:>4}  profitable={n_prof}/{n}")


if __name__ == "__main__":
    main()
