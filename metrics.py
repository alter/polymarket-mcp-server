#!/usr/bin/env python3
"""
Metrics computation from trade logs.

Reads:
  bot-data/arena_trades.jsonl       — every closed trade from arena (per-strategy)
  bot-data/oil_iran_portfolio.json  — oil-iran history
  bot-data/prediction_portfolio.json — legacy structural NO history

Computes proper metrics:
  - Equity curve (per strategy)
  - Max drawdown ($ and %)
  - Mean drawdown (in DD periods)
  - Current drawdown
  - Time in drawdown
  - Sharpe (from equity curve returns)
  - Calmar (annualized return / max DD%) — only if >= 30 days
  - Avg trade, avg win, avg loss, profit factor
"""
import json, os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

DATA = Path("bot-data")
TRADES = DATA / "arena_trades.jsonl"
OI_FILE = DATA / "oil_iran_portfolio.json"
LEGACY_FILE = DATA / "prediction_portfolio.json"

STARTING = 1000.0


def load_arena_trades():
    """Yield all arena trades from persistent log."""
    if not TRADES.exists():
        return []
    trades = []
    with open(TRADES) as f:
        for line in f:
            try:
                trades.append(json.loads(line))
            except Exception:
                continue
    return trades


def compute_equity_curve(trades, starting=STARTING):
    """Build equity curve from chronological trades.
    Returns list of (timestamp, equity) tuples."""
    if not trades:
        return [(0, starting)]
    sorted_trades = sorted(trades, key=lambda t: t.get("closed_at", 0))
    curve = [(sorted_trades[0].get("opened_at", sorted_trades[0]["closed_at"]), starting)]
    eq = starting
    for t in sorted_trades:
        eq += t["pnl"]
        curve.append((t["closed_at"], eq))
    return curve


def compute_drawdown_stats(curve):
    """Returns dict of DD metrics from equity curve."""
    if len(curve) < 2:
        return {
            "max_dd_abs": 0, "max_dd_pct": 0,
            "mean_dd_pct": 0, "current_dd_pct": 0,
            "peak_equity": curve[0][1] if curve else STARTING,
            "trough_equity": curve[0][1] if curve else STARTING,
            "time_in_dd_pct": 0,
            "recovery_periods": 0,
        }
    peak = curve[0][1]
    max_dd_abs = 0
    max_dd_pct = 0
    dd_values = []
    time_in_dd = 0
    total_time = 0
    recoveries = 0
    was_in_dd = False

    prev_ts = curve[0][0]
    for ts, eq in curve:
        if eq > peak:
            peak = eq
            if was_in_dd:
                recoveries += 1
                was_in_dd = False
        dd_abs = peak - eq
        dd_pct = dd_abs / peak if peak > 0 else 0
        if dd_abs > max_dd_abs:
            max_dd_abs = dd_abs
            max_dd_pct = dd_pct
        if dd_pct > 0.001:
            dd_values.append(dd_pct)
            if ts > prev_ts:
                time_in_dd += ts - prev_ts
            was_in_dd = True
        if ts > prev_ts:
            total_time += ts - prev_ts
        prev_ts = ts

    final_eq = curve[-1][1]
    current_dd_pct = (peak - final_eq) / peak if peak > 0 else 0

    return {
        "max_dd_abs": round(max_dd_abs, 2),
        "max_dd_pct": round(max_dd_pct * 100, 2),
        "mean_dd_pct": round(sum(dd_values) / max(len(dd_values), 1) * 100, 2),
        "current_dd_pct": round(current_dd_pct * 100, 2),
        "peak_equity": round(peak, 2),
        "trough_equity": round(min(c[1] for c in curve), 2),
        "time_in_dd_pct": round(time_in_dd / max(total_time, 1) * 100, 1),
        "recovery_periods": recoveries,
    }


def compute_trade_stats(trades):
    """Returns dict of trade-level metrics."""
    if not trades:
        return {"n": 0, "wr": 0, "avg": 0, "avg_win": 0, "avg_loss": 0,
                "profit_factor": 0, "expectancy": 0}
    wins = [t["pnl"] for t in trades if t["pnl"] > 0]
    losses = [t["pnl"] for t in trades if t["pnl"] <= 0]
    total_pnl = sum(t["pnl"] for t in trades)
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "n": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "wr": round(len(wins) / len(trades) * 100, 1),
        "total_pnl": round(total_pnl, 2),
        "avg": round(total_pnl / len(trades), 3),
        "avg_win": round(sum(wins) / max(len(wins), 1), 3),
        "avg_loss": round(sum(losses) / max(len(losses), 1), 3),
        "profit_factor": round(gross_win / max(gross_loss, 0.01), 2),
        "expectancy": round(total_pnl / len(trades), 3),
    }


def compute_returns_stats(curve, period_seconds=3600):
    """Rolling returns over fixed intervals for Sharpe calc."""
    if len(curve) < 3:
        return {"sharpe": None, "volatility": None}
    # Resample to hourly
    import math
    start_ts = curve[0][0]
    end_ts = curve[-1][0]
    total_hours = max((end_ts - start_ts) / period_seconds, 1)
    if total_hours < 3:
        return {"sharpe": None, "volatility": None}

    # Find equity at each hourly point via forward-fill
    buckets = []
    for i in range(int(total_hours) + 1):
        target = start_ts + i * period_seconds
        # Find last equity <= target
        last = curve[0][1]
        for ts, eq in curve:
            if ts <= target:
                last = eq
            else:
                break
        buckets.append(last)

    returns = []
    for i in range(1, len(buckets)):
        if buckets[i-1] > 0:
            returns.append((buckets[i] - buckets[i-1]) / buckets[i-1])

    if len(returns) < 2:
        return {"sharpe": None, "volatility": None}

    mean_r = sum(returns) / len(returns)
    var = sum((r - mean_r) ** 2 for r in returns) / len(returns)
    sd = math.sqrt(var)
    # Hourly Sharpe annualized (sqrt(24*365) ≈ 93.4)
    sharpe = (mean_r / sd * math.sqrt(24 * 365)) if sd > 0 else 0
    return {
        "sharpe": round(sharpe, 2),
        "volatility_hourly_pct": round(sd * 100, 3),
        "mean_return_hourly_pct": round(mean_r * 100, 4),
        "n_hourly_samples": len(returns),
    }


def compute_calmar(curve, starting=STARTING):
    """Calmar = annualized return / max DD%. Only valid for >=30 days of data."""
    if len(curve) < 2:
        return {"calmar": None, "valid": False, "reason": "no data"}
    duration_sec = curve[-1][0] - curve[0][0]
    duration_days = duration_sec / 86400
    if duration_days < 30:
        return {"calmar": None, "valid": False,
                "reason": f"insufficient history ({duration_days:.1f}d < 30d)"}
    total_return = (curve[-1][1] - starting) / starting
    annualized = total_return * (365 / duration_days)
    dd = compute_drawdown_stats(curve)
    max_dd_pct = dd["max_dd_pct"]
    calmar = (annualized * 100) / max(max_dd_pct, 0.01) if max_dd_pct > 0 else None
    return {
        "calmar": round(calmar, 2) if calmar is not None else None,
        "valid": True,
        "annualized_pct": round(annualized * 100, 2),
        "duration_days": round(duration_days, 1),
    }


def full_metrics(trades, starting=STARTING, label=""):
    """Comprehensive metrics report."""
    curve = compute_equity_curve(trades, starting)
    dd = compute_drawdown_stats(curve)
    ts = compute_trade_stats(trades)
    rs = compute_returns_stats(curve)
    ca = compute_calmar(curve, starting)
    current_eq = curve[-1][1] if curve else starting

    # Duration
    if trades:
        start_ts = min(t.get("opened_at", t["closed_at"]) for t in trades)
        end_ts = max(t["closed_at"] for t in trades)
        duration_h = (end_ts - start_ts) / 3600
    else:
        duration_h = 0

    return {
        "label": label,
        "duration_hours": round(duration_h, 1),
        "duration_days": round(duration_h / 24, 2),
        "starting": starting,
        "current_equity": round(current_eq, 2),
        "total_return_pct": round((current_eq - starting) / starting * 100, 2),
        "drawdown": dd,
        "trades": ts,
        "returns": rs,
        "calmar": ca,
    }


def arena_per_strategy_metrics(min_trades=20):
    """Compute metrics per strategy."""
    all_trades = load_arena_trades()
    by_strat = defaultdict(list)
    for t in all_trades:
        by_strat[t["strategy_id"]].append(t)

    results = []
    for sid, trades in by_strat.items():
        if len(trades) < min_trades:
            continue
        m = full_metrics(trades, label=f"S{sid:03d}")
        m["strategy_name"] = trades[0].get("strategy_name", "?")
        m["indicator"] = trades[0].get("indicator", "?")
        m["id"] = sid
        results.append(m)

    return results


def legacy_metrics():
    """Metrics for legacy structural NO portfolio."""
    if not LEGACY_FILE.exists():
        return None
    d = json.load(open(LEGACY_FILE))
    closed = d.get("closed", [])
    if not closed:
        return None
    # Convert to trade format
    trades = []
    for t in closed:
        resolved_at = t.get("resolved_at", "")
        if isinstance(resolved_at, str):
            try:
                ts = datetime.fromisoformat(resolved_at.replace("Z", "+00:00")).timestamp()
            except Exception:
                ts = 0
        else:
            ts = resolved_at
        opened_at = t.get("opened_at", "")
        if isinstance(opened_at, str):
            try:
                op_ts = datetime.fromisoformat(opened_at.replace("Z", "+00:00")).timestamp()
            except Exception:
                op_ts = ts - 86400
        else:
            op_ts = opened_at
        trades.append({
            "pnl": t.get("pnl", 0),
            "opened_at": op_ts,
            "closed_at": ts,
            "question": t.get("question", ""),
        })
    return full_metrics(trades, label="Legacy Structural NO")


def oil_iran_metrics():
    if not OI_FILE.exists():
        return None
    d = json.load(open(OI_FILE))
    history = d.get("history", [])
    if not history:
        return None
    return full_metrics(history, label="Oil×Iran")


def print_report(m, indent=""):
    """Pretty-print full metrics report."""
    print(f"{indent}━━━ {m['label']} ━━━")
    print(f"{indent}  Duration:      {m['duration_hours']:.1f}h ({m['duration_days']:.2f} days)")
    print(f"{indent}  Starting:      ${m['starting']:,.2f}")
    print(f"{indent}  Current eq:    ${m['current_equity']:,.2f}  ({m['total_return_pct']:+.2f}%)")
    td = m["trades"]
    print(f"{indent}  Trades:        {td['n']}  ({td['wins']}W/{td['losses']}L = {td['wr']}% WR)")
    print(f"{indent}  Avg trade:     ${td['avg']:+.3f}  (win ${td['avg_win']:+.2f} / loss ${td['avg_loss']:+.2f})")
    print(f"{indent}  Profit factor: {td['profit_factor']}")
    dd = m["drawdown"]
    print(f"{indent}  Peak/Trough:   ${dd['peak_equity']:.2f} / ${dd['trough_equity']:.2f}")
    print(f"{indent}  Max DD:        ${dd['max_dd_abs']:.2f} ({dd['max_dd_pct']}%)")
    print(f"{indent}  Mean DD (in-DD periods): {dd['mean_dd_pct']}%")
    print(f"{indent}  Current DD:    {dd['current_dd_pct']}%")
    print(f"{indent}  Time in DD:    {dd['time_in_dd_pct']}%")
    print(f"{indent}  Recoveries:    {dd['recovery_periods']}")
    rs = m["returns"]
    if rs.get("sharpe") is not None:
        print(f"{indent}  Sharpe (annl): {rs['sharpe']}")
        print(f"{indent}  Vol hourly:    {rs['volatility_hourly_pct']}%")
        print(f"{indent}  Hourly samples: {rs['n_hourly_samples']}")
    else:
        print(f"{indent}  Sharpe:        N/A (need ≥3h of data)")
    ca = m["calmar"]
    if ca["valid"]:
        print(f"{indent}  Calmar:        {ca['calmar']}  (annualized {ca['annualized_pct']}% / max DD {dd['max_dd_pct']}%)")
    else:
        print(f"{indent}  Calmar:        N/A  ({ca['reason']})")


if __name__ == "__main__":
    import sys
    print("=" * 80)
    print("POLYMARKET METRICS — full context")
    print("=" * 80)

    # Legacy
    print()
    leg = legacy_metrics()
    if leg:
        print_report(leg)

    # Oil×Iran
    print()
    oi = oil_iran_metrics()
    if oi:
        print_report(oi)

    # Arena per-strategy
    print()
    print("━━━ ARENA per-strategy metrics ━━━")
    arena = arena_per_strategy_metrics(min_trades=20)
    if not arena:
        print("  No trades logged yet (arena_trades.jsonl empty — needs new closed trades)")
    else:
        arena.sort(key=lambda x: -x["current_equity"])
        print(f"  {len(arena)} strategies with ≥20 trades\n")
        print(f"  {'Strategy':<48} {'Eq':>8} {'Ret%':>7} {'DD%':>6} {'PF':>5} {'Sharpe':>8}")
        for m in arena[:15]:
            td = m['trades']
            dd = m['drawdown']
            rs = m['returns']
            sh = rs['sharpe'] if rs.get('sharpe') is not None else 'N/A'
            print(f"  {m['strategy_name'][:48]:<48} ${m['current_equity']:>6.0f} "
                  f"{m['total_return_pct']:>+6.1f}% {dd['max_dd_pct']:>5.1f}% "
                  f"{td['profit_factor']:>5} {str(sh):>8}")
        print(f"\n  (Run earlier for full picture — arena trade log starts now)")
