#!/usr/bin/env python3
"""Reconstruct equity time-series from arena_results.json snapshots in bot-data/
and bot-data/backups/, then compute max/mean drawdown per strategy.

Snapshots are taken every ~30 min by watchdog. With ~22h of history that
gives 13-50 sample points — coarse, but enough to estimate peak-to-trough
loss per strategy. Calmar requires >=30 days; under that, prints
"insufficient history".
"""
import json, os, glob, sys
from collections import defaultdict
from datetime import datetime, timezone

BACKUPS = "bot-data/backups"
LIVE = "bot-data/arena_results.json"
STARTING_BALANCE = 1000.0


def parse_snap_time(path):
    """arena_results.json.20260430_1731 -> datetime; .daily.20260429 -> midnight."""
    name = os.path.basename(path)
    suf = name.replace("arena_results.json.", "").replace("arena_results.json", "live")
    if suf == "live":
        return datetime.now(timezone.utc)
    if suf.startswith("daily."):
        d = suf.replace("daily.", "")
        return datetime.strptime(d, "%Y%m%d").replace(tzinfo=timezone.utc)
    try:
        return datetime.strptime(suf, "%Y%m%d_%H%M").replace(tzinfo=timezone.utc)
    except Exception:
        # Fallback to file mtime
        return datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)


def load_snapshots():
    paths = glob.glob(os.path.join(BACKUPS, "arena_results.json.*")) + [LIVE]
    paths = [p for p in paths if os.path.exists(p)]
    paths_ts = sorted([(parse_snap_time(p), p) for p in paths], key=lambda x: x[0])
    by_strat = defaultdict(list)   # sid -> [(ts, equity, retired)]
    names = {}
    final_state = {}
    final_path = paths_ts[-1][1] if paths_ts else None
    for ts, p in paths_ts:
        try:
            d = json.load(open(p))
        except Exception:
            continue
        for r in d.get("results", []):
            sid = r["id"]
            by_strat[sid].append((ts, r["equity"], r.get("retired", False)))
            names[sid] = r["name"]
            if p == final_path:
                final_state[sid] = r
    for sid in by_strat:
        by_strat[sid].sort(key=lambda x: x[0])
    return by_strat, names, final_state


def compute_dd(series):
    """series = list of (ts, equity). Returns max_dd, mean_dd, peak_eq, recovered."""
    if len(series) < 2:
        return 0.0, 0.0, series[0][1] if series else STARTING_BALANCE, True
    peak = STARTING_BALANCE
    max_dd = 0.0
    drawdowns = []
    for ts, eq, _ in series:
        if eq > peak:
            peak = eq
        dd = peak - eq
        drawdowns.append(dd)
        if dd > max_dd:
            max_dd = dd
    mean_dd = sum(drawdowns) / len(drawdowns) if drawdowns else 0.0
    recovered = drawdowns[-1] < max_dd * 0.5 if max_dd else True
    return max_dd, mean_dd, peak, recovered


def report():
    snaps, names, final = load_snapshots()
    if not snaps:
        print("No snapshots found")
        return

    # Time window
    all_ts = [t for series in snaps.values() for t, _, _ in series]
    start = min(all_ts); end = max(all_ts)
    duration_h = (end - start).total_seconds() / 3600

    print(f"Time window: {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M} UTC ({duration_h:.1f}h)")
    print(f"Starting capital: ${STARTING_BALANCE:.0f}/strategy × {len(snaps)} = ${STARTING_BALANCE*len(snaps):,.0f} pool")
    print(f"Position size: $50/trade (arena-scale = $0.01 actual × 5000)")
    print(f"Snapshots: {len(set(all_ts))}")
    print()

    # Total realized
    total_realized = sum(r.get("realized", 0) for r in final.values())
    total_trades = sum(r.get("trades", 0) for r in final.values())
    profitable = sum(1 for r in final.values() if r["equity"] > 1000 and not r.get("retired"))
    retired = sum(1 for r in final.values() if r.get("retired"))
    active = len(final) - retired
    print(f"Trades: {total_trades:,} | Realized: ${total_realized:+,.0f} | Active: {active} | Profitable: {profitable} | Retired: {retired}")
    print()

    # Top by equity with DD
    rows = []
    for sid, series in snaps.items():
        max_dd, mean_dd, peak, recovered = compute_dd(series)
        eq_now = series[-1][1]
        rows.append({
            "name": names[sid],
            "eq_now": eq_now,
            "peak": peak,
            "max_dd": max_dd,
            "max_dd_pct": max_dd / peak * 100 if peak else 0,
            "mean_dd": mean_dd,
            "trades": final[sid].get("trades", 0),
            "wins": final[sid].get("wins", 0),
            "losses": final[sid].get("losses", 0),
        })

    print("=== TOP 5 by current equity (with drawdown) ===")
    rows.sort(key=lambda r: -r["eq_now"])
    print(f"{'name':<55} {'eq':>7} {'peak':>7} {'maxDD':>7} {'maxDD%':>6} {'meanDD':>7} {'trades':>6} {'WR':>4}")
    for r in rows[:5]:
        wl = r["wins"] + r["losses"]
        wr = r["wins"]/wl*100 if wl else 0
        print(f"{r['name'][:55]:<55} ${r['eq_now']:>6.0f} ${r['peak']:>6.0f} ${r['max_dd']:>6.0f} {r['max_dd_pct']:>5.1f}% ${r['mean_dd']:>6.1f} {r['trades']:>6} {wr:>3.0f}%")

    print()
    # Worst DD across the active set
    print("=== TOP 5 worst max-DD (active strategies, eq>=$500) ===")
    active_rows = [r for r in rows if r["eq_now"] >= 500]
    active_rows.sort(key=lambda r: -r["max_dd"])
    for r in active_rows[:5]:
        print(f"{r['name'][:55]:<55} ${r['eq_now']:>6.0f} maxDD=${r['max_dd']:>5.0f} ({r['max_dd_pct']:.1f}% of peak)")

    print()
    # Portfolio-level
    portfolio_series = defaultdict(float)
    for sid, series in snaps.items():
        for ts, eq, _ in series:
            portfolio_series[ts] += eq
    pts = sorted(portfolio_series.items())
    p_max_dd, p_mean_dd, p_peak, _ = compute_dd([(t, v, False) for t, v in pts])
    p_now = pts[-1][1]
    p_start_total = STARTING_BALANCE * len(snaps)
    print(f"=== Portfolio total ===")
    print(f"  start=${p_start_total:,.0f} peak=${p_peak:,.0f} now=${p_now:,.0f} maxDD=${p_max_dd:,.0f} ({p_max_dd/p_peak*100:.2f}%)")

    print()
    if duration_h < 24*30:
        print(f"Calmar: insufficient history ({duration_h:.1f}h < 720h / 30 days)")
        print(f"Sharpe: skipped (no tick-level equity, only {len(set(all_ts))} snapshot points)")
        print(f"Annualized: skipped (<30 days; raw {duration_h:.1f}h period only)")
    else:
        # If we ever get >30 days
        years = duration_h / (24 * 365)
        ann_ret = (p_now / p_start_total) ** (1/years) - 1 if p_start_total else 0
        calmar = ann_ret / (p_max_dd/p_peak) if p_max_dd else float("inf")
        print(f"Annualized return: {ann_ret*100:+.1f}% | Calmar: {calmar:.2f}")


if __name__ == "__main__":
    report()
