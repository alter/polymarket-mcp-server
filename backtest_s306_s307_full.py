#!/usr/bin/env python3
"""
Full backtest of S306 and S307 over Apr 17 – May 22 2026,
combining original arena_ticks.jsonl + recovered arena_ticks_recovered.jsonl.

Exact multi_strategy.py logic reproduced here.
"""

import json, os, math, sys
from collections import deque
from datetime import datetime, timezone

# ── Constants (from multi_strategy.py) ─────────────────────────────────────
STARTING_BALANCE = 1000.0
POSITION_USD     = 50.0
MAX_POSITIONS    = 20
MIN_MID          = 0.10
MAX_MID          = 0.90
MIN_SPREAD       = 0.001
MAX_SPREAD       = 0.05
SPREAD_LIMIT     = 0.10   # tick_all_strategies check
TICK_MAXLEN      = 500
MIN_TICKS        = 5

# ── EMA / RSI (exact copy from multi_strategy.py) ──────────────────────────
def ema(prices, period):
    if not prices:
        return 0.0
    alpha = 2.0 / (period + 1)
    e = prices[0]
    for p in prices[1:]:
        e = alpha * p + (1 - alpha) * e
    return e

def rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        d = prices[i] - prices[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    return 100 - (100 / (1 + avg_g / avg_l))

# ── Strategy specs ─────────────────────────────────────────────────────────
STRATEGIES = {
    "S306": dict(
        indicator="mean_rev_ema", period=10,
        entry_param=0.02, exit_param=0.015,    # exit_param = entry*0.75
        sl=-0.25, tp=0.10,
    ),
    "S307": dict(
        indicator="rsi", period=14,
        entry_param=30.0, exit_param=0.5,
        sl=-0.25, tp=0.10,
    ),
}

# ── Tick data loader ───────────────────────────────────────────────────────
def load_ticks(*paths):
    """Load and merge-sort ticks from multiple files."""
    all_ticks = []
    for path in paths:
        if not os.path.exists(path):
            print(f"  [warn] not found: {path}", file=sys.stderr)
            continue
        n = 0
        with open(path) as f:
            for line in f:
                try:
                    t = json.loads(line)
                    all_ticks.append(t)
                    n += 1
                except Exception:
                    pass
        print(f"  loaded {n:,} ticks from {os.path.basename(path)}")
    all_ticks.sort(key=lambda t: t["ts"])
    return all_ticks

# ── Backtest engine ────────────────────────────────────────────────────────
class StrategyState:
    def __init__(self, name, spec):
        self.name = name
        self.spec = spec
        self.balance  = STARTING_BALANCE
        self.positions = {}   # market_id -> {entry, side, shares, cost}
        self.equity_curve = []  # (ts_iso, equity)
        self.trades   = []
        self.peak_equity = STARTING_BALANCE

    @property
    def equity(self):
        return self.balance + sum(p["cost"] for p in self.positions.values())

    def try_enter(self, market_id, mid, bid, ask, ts_iso):
        if market_id in self.positions:
            return
        if len(self.positions) >= MAX_POSITIONS:
            return
        if not (MIN_MID <= mid <= MAX_MID):
            return
        spread_pct = (ask - bid) / mid if mid > 0 else 1
        if spread_pct > SPREAD_LIMIT:
            return

        spec = self.spec
        # Build price history from tick buffer (not stored here; passed in)
        # Called only after prices check passes outside
        pass  # logic moved to run()

    def open_no(self, market_id, mid, ts_iso):
        if len(self.positions) >= MAX_POSITIONS:
            return False
        no_price = 1.0 - mid
        cost = POSITION_USD
        if cost > self.balance:
            return False
        shares = POSITION_USD / no_price
        self.balance -= cost
        self.positions[market_id] = {
            "entry": no_price, "shares": shares, "cost": cost, "opened_at": ts_iso
        }
        return True

    def close(self, market_id, mid, reason, ts_iso, exit_no_price=None):
        pos = self.positions.pop(market_id)
        # For TP/SL: exit at the threshold price, not the (potentially gap-skipped) tick price.
        # This prevents sparse-tick slippage from inflating TP gains or softening SL losses.
        if exit_no_price is None:
            exit_no_price = 1.0 - mid
        gross = pos["shares"] * exit_no_price
        pnl = gross - pos["cost"]
        self.balance += gross
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity
        self.trades.append({
            "pnl": pnl, "reason": reason,
            "opened": pos["opened_at"], "closed": ts_iso,
        })


def run_backtest(ticks):
    states = {name: StrategyState(name, spec) for name, spec in STRATEGIES.items()}

    # Per-market tick buffer
    market_ticks = {}   # market_id -> deque(maxlen=500)

    last_equity_ts = ""
    equity_interval = 3600  # record equity once per hour (by ts string comparison)

    for tick in ticks:
        ts  = tick["ts"]
        mid = tick["mid"]
        mid_f = float(mid)
        bid   = float(tick.get("bid", mid_f))
        ask   = float(tick.get("ask", mid_f))
        mkt   = str(tick["market_id"])
        fees  = tick.get("fees", False)

        # Update tick buffer
        if mkt not in market_ticks:
            market_ticks[mkt] = deque(maxlen=TICK_MAXLEN)
        market_ticks[mkt].append(mid_f)
        prices = list(market_ticks[mkt])

        if len(prices) < MIN_TICKS:
            continue

        spread_pct = (ask - bid) / mid_f if mid_f > 0 else 1

        for name, st in states.items():
            spec = st.spec
            ind  = spec["indicator"]

            # ── Manage existing position ─────────────────────────────────
            if mkt in st.positions:
                pos = st.positions[mkt]
                current = 1.0 - mid_f   # NO price
                entry   = pos["entry"]
                pnl_pct = (current - entry) / entry if entry > 0 else 0

                if pnl_pct <= spec["sl"]:
                    # Exit at SL threshold, not at (possibly far worse) tick price
                    sl_price = entry * (1 + spec["sl"])
                    st.close(mkt, mid_f, "stop_loss", ts, exit_no_price=sl_price)
                    continue
                if pnl_pct >= spec["tp"]:
                    # Exit at TP threshold, not at (possibly far better) tick price
                    tp_price = entry * (1 + spec["tp"])
                    st.close(mkt, mid_f, "take_profit", ts, exit_no_price=tp_price)
                    continue

                # Indicator exit — no fixed price threshold, use actual tick price
                if ind == "mean_rev_ema":
                    if len(prices) >= spec["period"] + 2:
                        e   = ema(prices, spec["period"])
                        dev = (prices[-1] - e) / e if e > 0 else 0
                        if dev < -spec["exit_param"]:
                            st.close(mkt, mid_f, "revert_exit", ts)
                    continue

                elif ind == "rsi":
                    if len(prices) >= spec["period"] + 1:
                        r = rsi(prices, spec["period"])
                        if r < spec["entry_param"]:   # RSI < 30 → oversold
                            st.close(mkt, mid_f, "rsi_oversold", ts)
                    continue

            # ── Entry ───────────────────────────────────────────────────
            if fees:           # fee_free_only
                continue
            if not (MIN_MID <= mid_f <= MAX_MID):
                continue
            if spread_pct > SPREAD_LIMIT:
                continue

            signal = None
            if ind == "mean_rev_ema":
                if len(prices) >= spec["period"] + 2:
                    e   = ema(prices, spec["period"])
                    dev = (prices[-1] - e) / e if e > 0 else 0
                    if dev > spec["entry_param"]:
                        signal = "sell"
            elif ind == "rsi":
                if len(prices) >= spec["period"] + 2:
                    r = rsi(prices, spec["period"])
                    if r > spec["exit_param"]:         # RSI > 0.5 → sell
                        signal = "sell"

            if signal == "sell":
                st.open_no(mkt, mid_f, ts)

        # Record equity once per hour (compare first 13 chars of ts "2026-05-13T14")
        hour_key = ts[:13]
        if hour_key != last_equity_ts:
            last_equity_ts = hour_key
            for st in states.values():
                st.equity_curve.append((ts, st.equity))

    return states


# ── Statistics ─────────────────────────────────────────────────────────────
def stats(st):
    trades   = st.trades
    n        = len(trades)
    wins     = sum(1 for t in trades if t["pnl"] > 0)
    wr       = wins / n * 100 if n else 0
    total_pnl = sum(t["pnl"] for t in trades)

    curve = [e for _, e in st.equity_curve]
    if not curve:
        return {}

    peak = curve[0]
    max_dd = 0.0
    dd_sum = 0.0
    dd_count = 0
    in_dd = False
    dd_start = 0

    for i, eq in enumerate(curve):
        if eq > peak:
            peak = eq
        dd = (eq - peak) / peak
        if dd < max_dd:
            max_dd = dd
        if dd < 0:
            dd_sum += dd
            dd_count += 1

    mean_dd = dd_sum / dd_count if dd_count else 0.0

    # Annualised return
    ts_first = st.equity_curve[0][0]
    ts_last  = st.equity_curve[-1][0]
    t0 = datetime.fromisoformat(ts_first).timestamp()
    t1 = datetime.fromisoformat(ts_last).timestamp()
    days = (t1 - t0) / 86400
    raw_ret = (curve[-1] - curve[0]) / curve[0]
    # Don't annualize short periods with extreme returns — produces nonsense
    ann_ret = (1 + raw_ret) ** (365 / days) - 1 if days >= 90 else None
    calmar  = ann_ret / abs(max_dd) if (ann_ret and max_dd < 0) else None
    # Raw-period Calmar (not annualized): ROI / |max_dd|
    calmar_raw = raw_ret / abs(max_dd) if max_dd < 0 else None

    reasons = {}
    for t in trades:
        r = t["reason"]
        reasons[r] = reasons.get(r, 0) + 1

    return {
        "trades": n, "wins": wins, "wr": wr,
        "total_pnl": total_pnl,
        "final_equity": curve[-1],
        "roi_pct": (curve[-1] / STARTING_BALANCE - 1) * 100,
        "max_dd": max_dd * 100,
        "mean_dd": mean_dd * 100,
        "ann_ret": ann_ret * 100 if ann_ret else None,
        "calmar": calmar,
        "calmar_raw": calmar_raw,
        "days": days,
        "reasons": reasons,
    }


# ── Plotting ───────────────────────────────────────────────────────────────
# Real arena final equities for reference (from arena_results.json 2026-05-23)
REAL_EQUITY = {"S306": 2789.14, "S307": 2999.38}
REAL_TRADES = {"S306": "705 trades, W/L=250/57 (WR 81%)",
               "S307": "2102 trades, W/L=626/167 (WR 79%)"}


def plot(states, outpath="/tmp/backtest_s306_s307_full.png"):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    colors = {"S306": "#2ecc71", "S307": "#3498db"}
    names  = {"S306": "S306 | mean_rev_ema p10 e0.02 NO-only",
              "S307": "S307 | rsi p14 e30 exit0.5 NO-only"}

    all_dates = []
    for name, st in states.items():
        if not st.equity_curve:
            continue
        dates  = [datetime.fromisoformat(ts) for ts, _ in st.equity_curve]
        equity = [e for _, e in st.equity_curve]
        all_dates.extend(dates)
        s = stats(st)

        ax = axes[list(states.keys()).index(name)]
        ax.plot(dates, equity, color=colors[name], linewidth=1.5, label=names[name])
        ax.axhline(STARTING_BALANCE, color="gray", linestyle="--", linewidth=0.8, alpha=0.6)

        # Gap region shading
        gap_s = datetime(2026, 5, 13, tzinfo=timezone.utc)
        gap_e = datetime(2026, 5, 23, tzinfo=timezone.utc)
        ax.axvspan(gap_s, gap_e, alpha=0.08, color="orange", label="recovered ticks (May 13-22)")

        # Real arena reference line
        real_eq = REAL_EQUITY.get(name)
        if real_eq:
            ax.axhline(real_eq, color="red", linestyle=":", linewidth=1.2, alpha=0.7,
                       label=f"Real arena {name}: ${real_eq:.0f}")

        lbl = (f"SIMULATION (Apr 17–May 22)\n"
               f"Equity: ${s['final_equity']:.0f}  ROI: {s['roi_pct']:+.1f}%\n"
               f"Trades: {s['trades']}  WR: {s['wr']:.0f}%  "
               f"MaxDD: {s['max_dd']:.1f}%  MeanDD: {s['mean_dd']:.1f}%\n"
               f"Calmar(raw period): {s['calmar_raw']:.1f}\n"
               f"────────────────────────\n"
               f"REAL ARENA (Apr 21–May 23)\n"
               f"Equity: ${real_eq:.0f}  {REAL_TRADES[name]}")
        if s["calmar"] is not None:
            lbl += f"\nCalmar(ann): {s['calmar']:.2f}  Ann. ret: {s['ann_ret']:+.1f}%"
        ax.set_title(names[name], fontsize=11, fontweight="bold")
        ax.set_ylabel("Equity ($)")
        ax.legend(loc="upper left", fontsize=8)
        ax.text(0.99, 0.04, lbl, transform=ax.transAxes, fontsize=8,
                ha="right", va="bottom",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
        ax.grid(True, alpha=0.3)

    if all_dates:
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        axes[-1].xaxis.set_major_locator(mdates.WeekdayLocator())
        fig.autofmt_xdate()

    fig.suptitle("S306 & S307 Full Backtest — Apr 17 to May 22 2026\n"
                 "(original ticks + recovered May 13-22 via CLOB prices-history)",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.savefig(outpath, dpi=140, bbox_inches="tight")
    print(f"Chart saved → {outpath}")
    return outpath


# ── Main ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("Loading ticks …")
    ticks = load_ticks(
        "bot-data/arena_ticks.jsonl",
        "bot-data/arena_ticks_recovered.jsonl",
    )
    print(f"Total ticks: {len(ticks):,} covering "
          f"{ticks[0]['ts'][:10]} → {ticks[-1]['ts'][:10]}")

    print("\nRunning backtest …")
    states = run_backtest(ticks)

    print("\n── Results ─────────────────────────────────────────────────────")
    for name, st in states.items():
        s = stats(st)
        if not s:
            print(f"{name}: no equity data")
            continue
        print(f"\n{name}:")
        print(f"  Period  : {s['days']:.0f} days")
        print(f"  Equity  : ${s['final_equity']:.2f}  (start ${STARTING_BALANCE:.0f})")
        print(f"  ROI     : {s['roi_pct']:+.1f}%")
        print(f"  Trades  : {s['trades']}  wins={s['wins']}  WR={s['wr']:.0f}%")
        print(f"  Max DD  : {s['max_dd']:.1f}%")
        print(f"  Mean DD : {s['mean_dd']:.1f}%")
        print(f"  Calmar(raw): {s['calmar_raw']:.2f}  (ROI / |maxDD|, not annualized)")
        if s["calmar"] is not None:
            print(f"  Calmar(ann): {s['calmar']:.2f}  Ann.ret={s['ann_ret']:+.1f}%")
        else:
            print(f"  Calmar(ann): n/a — need ≥90 days to annualize")
        print(f"  Exit reasons: {s['reasons']}")

    print("\nPlotting …")
    out = plot(states)
    import subprocess
    subprocess.run(["open", out], check=False)
