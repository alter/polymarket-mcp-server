#!/usr/bin/env python3
"""TOP 20 with full metrics in two views: LIVE paper + BACKTEST.

LIVE: scans arena_trades.jsonl for exact per-trade pnl → exact Profit Factor.
BACKTEST: reads arena_backtest_full.json (replay over full tick history).

Metrics per strategy:
  Equity $, Equity %, Profit $
  max DD $, max DD %, mean DD
  Calmar (annualized return / max DD%)
  WR, Profit Factor, trades count, period dates
"""
import json, glob, os
from collections import defaultdict
from datetime import datetime, timezone


# ─── LIVE PAPER ─────────────────────────────────────────────────────────────
def parse_snap_time(path):
    name = os.path.basename(path)
    suf = name.replace("arena_results.json.", "").replace("arena_results.json", "live")
    if suf == "live":
        return datetime.now(timezone.utc)
    if suf.startswith("daily."):
        return datetime.strptime(suf.replace("daily.", ""), "%Y%m%d").replace(tzinfo=timezone.utc)
    try:
        return datetime.strptime(suf, "%Y%m%d_%H%M").replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)


def calc_dd_metrics(eq_series):
    if len(eq_series) < 2:
        return 0, 0, 0
    peak = eq_series[0]
    max_dd = 0
    drawdowns = []
    for e in eq_series:
        if e > peak: peak = e
        dd = peak - e
        drawdowns.append(dd)
        if dd > max_dd: max_dd = dd
    mean_dd = sum(drawdowns) / len(drawdowns)
    return max_dd, mean_dd, peak


def calmar(roi_pct, max_dd_pct, duration_days):
    if max_dd_pct <= 0 or duration_days <= 0:
        return float("inf") if roi_pct > 0 else 0
    if duration_days < 30:
        return None  # not meaningful
    ann_ret = ((1 + roi_pct/100) ** (365/duration_days) - 1) * 100
    return ann_ret / max_dd_pct


def fmt_calmar(c):
    if c is None: return "n/a*"
    if c == float("inf"): return "inf"
    return f"{c:.1f}"


def live_pf(trades_file, top_ids):
    """Scan arena_trades.jsonl for per-trade pnl by strategy id.
    Returns: sid → (pos_sum, neg_sum, n_trades, first_ts, last_ts)"""
    out = defaultdict(lambda: {"pos": 0.0, "neg": 0.0, "n": 0, "first": None, "last": None})
    if not os.path.exists(trades_file):
        return out
    with open(trades_file) as f:
        for line in f:
            try:
                t = json.loads(line)
            except Exception:
                continue
            sid = t.get("strategy_id")
            if sid not in top_ids:
                continue
            pnl = t.get("pnl", 0)
            ts = t.get("closed_at", 0)
            r = out[sid]
            if pnl > 0: r["pos"] += pnl
            else: r["neg"] += pnl
            r["n"] += 1
            if r["first"] is None or ts < r["first"]: r["first"] = ts
            if r["last"] is None or ts > r["last"]: r["last"] = ts
    return out


def section_live():
    print("=" * 165)
    print("LIVE PAPER TRADING (arena, blanket $50/trade, started 2026-04-17)")
    print("=" * 165)

    paths = sorted([(parse_snap_time(p), p)
                    for p in glob.glob("bot-data/backups/arena_results.json.*") +
                             ["bot-data/arena_results.json"] if os.path.exists(p)],
                   key=lambda x: x[0])
    series = defaultdict(list)
    for ts, p in paths:
        try: d = json.load(open(p))
        except: continue
        for r in d.get("results", []):
            series[r["id"]].append(r["equity"])

    final = json.load(open("bot-data/arena_results.json"))
    res = final["results"]
    res.sort(key=lambda r: -r["equity"])
    top20 = res[:20]
    top_ids = {r["id"] for r in top20}
    print("Scanning arena_trades.jsonl for exact PF (this takes ~30s)...")
    pf_data = live_pf("bot-data/arena_trades.jsonl", top_ids)

    duration_d = (paths[-1][0] - paths[0][0]).total_seconds() / 86400
    print(f"Window: {paths[0][0]:%Y-%m-%d %H:%M} → {paths[-1][0]:%Y-%m-%d %H:%M} UTC ({duration_d:.1f}d, {len(paths)} snapshots)")
    print()

    hdr = f"{'#':>2} {'name':<55} {'eq$':>5} {'eq%':>5} {'profit':>7} {'maxDD$':>6} {'maxDD%':>6} {'meanDD':>6} {'Calmar':>7} {'WR':>4} {'PF':>5} {'trades':>7} {'first':<11}"
    print(hdr)
    print("-" * 165)
    for i, r in enumerate(top20, 1):
        eq_series = series.get(r["id"], [r["equity"]])
        max_dd, mean_dd, peak = calc_dd_metrics(eq_series)
        roi = (r["equity"] - 1000) / 1000 * 100
        max_dd_pct = max_dd / peak * 100 if peak else 0
        cal = calmar(roi, max_dd_pct, duration_d)
        # Profit factor — exact from arena_trades.jsonl
        pfd = pf_data.get(r["id"])
        if pfd and pfd["n"] > 0 and pfd["neg"] != 0:
            pf = pfd["pos"] / abs(pfd["neg"])
            first = datetime.fromtimestamp(pfd["first"], tz=timezone.utc).strftime("%Y-%m-%d")
        else:
            pf = float("inf")
            first = ""
        wr = r.get("wins",0) / max(r.get("wins",0)+r.get("losses",0), 1) * 100
        print(f"{i:>2} {r['name'][:55]:<55} ${r['equity']:>4.0f} {roi:>+4.0f}% ${r['equity']-1000:>+5.0f} ${max_dd:>5.0f} {max_dd_pct:>5.1f}% ${mean_dd:>4.1f} {fmt_calmar(cal):>7} {wr:>3.0f}% {pf:>4.2f} {r['trades']:>7} {first:<11}")
    print()
    print("* Calmar = n/a при duration<30d (по CLAUDE.md правилам отчётности)")


def section_backtest():
    print()
    print("=" * 165)
    print("HISTORICAL BACKTEST (arena_backtest_full.json — replay полной tick-истории, EVAL_EVERY=20)")
    print("=" * 165)

    bt = json.load(open("bot-data/arena_backtest_full.json"))
    # Window: arena_ticks first/last ts
    with open("bot-data/arena_ticks.jsonl") as f:
        first_tick = json.loads(f.readline())
    with open("bot-data/arena_ticks.jsonl", "rb") as f:
        f.seek(-1024, 2)
        last_tick = json.loads(f.read().decode().rstrip("\n").rsplit("\n", 1)[-1])
    t_first = datetime.fromisoformat(first_tick["ts"].replace("Z", "+00:00"))
    t_last = datetime.fromisoformat(last_tick["ts"].replace("Z", "+00:00"))
    duration_d = (t_last - t_first).total_seconds() / 86400
    bt_ran = bt.get("ran_at", "")
    print(f"Tick history window: {t_first:%Y-%m-%d %H:%M} → {t_last:%Y-%m-%d %H:%M} UTC ({duration_d:.1f}d)")
    print(f"Backtest ran: {bt_ran}, {bt['n_ticks']:,} ticks × {bt['n_strategies']} strategies")
    print()

    res = sorted(bt["results"], key=lambda r: -r["roi_pct"])[:20]
    hdr = f"{'#':>2} {'name':<55} {'eq$':>5} {'eq%':>5} {'profit':>7} {'maxDD%':>6} {'fees$':>6} {'Calmar':>7} {'WR':>4} {'PF':>5} {'trades':>7}"
    print(hdr)
    print("-" * 145)
    for i, r in enumerate(res, 1):
        eq = r["equity"]; roi = r["roi_pct"]; profit = eq - 1000
        max_dd_pct = r["max_dd_pct"]
        # Approx PF: realized + losses×avg_loss / losses×avg_loss
        # avg_loss approx = $50 - small_pnl (if losses are full bet, ≈ $50)
        # Better: use realized + total_loss = total_win, then PF = total_win/total_loss
        wins, losses = r["wins"], r["losses"]
        if losses > 0:
            # Assume avg loss = -$50/2 = -$25 (since exit at SL only loses ~half the bet)
            # Actually — backtest closes at price. Losses average smaller than $50.
            # We'll estimate from realized: total_pnl = wins×avg_win + losses×avg_loss
            # We don't have separate sums, so approximation:
            # if realized > 0, avg_win > avg_loss, ratio ≈ 2:1 typical → PF approx
            # Better: PF ≈ (realized/n_bets + b)/b where b = avg loss magnitude
            # Skip exact calc — show n/a
            avg_realized_per_trade = r["realized"] / r["trades"] if r["trades"] else 0
            # Crude estimate
            pf = float("nan")
        else:
            pf = float("inf")
        cal = calmar(roi, max_dd_pct, duration_d)
        print(f"{i:>2} {r['name'][:55]:<55} ${eq:>4.0f} {roi:>+4.1f}% ${profit:>+5.0f} {max_dd_pct:>5.1f}% ${r['fees']:>5.1f} {fmt_calmar(cal):>7} {r['wr']:>3.0f}% {'~':>4} {r['trades']:>7}")
    print()
    print("Note: PF в backtest помечен '~' — backtest хранит только агрегаты (realized + W/L), per-trade pnl не сохранены.")
    print("Для точного PF в backtest нужно re-run с per-trade emit. В live — PF точный, из arena_trades.jsonl.")


if __name__ == "__main__":
    section_live()
    section_backtest()
