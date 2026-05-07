#!/usr/bin/env python3
"""TOP 20 strategies report with full metrics: dates, equity, ROI, max DD,
mean DD, Calmar, profit factor."""
import json, glob, os
from collections import defaultdict
from datetime import datetime, timezone


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


def main():
    paths = glob.glob("bot-data/backups/arena_results.json.*") + ["bot-data/arena_results.json"]
    paths_ts = sorted([(parse_snap_time(p), p) for p in paths if os.path.exists(p)],
                     key=lambda x: x[0])
    series = defaultdict(list)
    for ts, p in paths_ts:
        try:
            d = json.load(open(p))
        except Exception:
            continue
        for r in d.get("results", []):
            series[r["id"]].append((ts, r["equity"]))

    final = json.load(open("bot-data/arena_results.json"))
    res = final["results"]
    res.sort(key=lambda r: -r["equity"])

    start = paths_ts[0][0]
    end = paths_ts[-1][0]
    duration_h = (end - start).total_seconds() / 3600
    duration_d = duration_h / 24

    print(f"Window: {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M} UTC ({duration_d:.1f}d / {duration_h:.0f}h)")
    print(f"Starting capital: $1000/strategy, Position $50/trade arena-scale")
    print(f"Snapshots: {len(paths_ts)}\n")

    print(f"{'#':>3} {'name':<55} {'eq':>6} {'ROI':>6} {'$ profit':>9} {'maxDD$':>7} {'maxDD%':>6} {'meanDD$':>8} {'Calmar':>7} {'PF':>5} {'WR':>4} {'trades':>7}")
    print("-" * 140)

    for i, r in enumerate(res[:20], 1):
        sid = r["id"]
        eq_series = [v for _, v in series.get(sid, [])]
        if len(eq_series) < 2:
            continue
        peak = eq_series[0]
        max_dd = 0.0
        drawdowns = []
        for e in eq_series:
            if e > peak:
                peak = e
            dd = peak - e
            drawdowns.append(dd)
            if dd > max_dd:
                max_dd = dd
        mean_dd = sum(drawdowns) / len(drawdowns)
        roi_pct = (r["equity"] - 1000) / 1000 * 100
        profit = r["equity"] - 1000
        max_dd_pct = max_dd / peak * 100 if peak else 0

        # Calmar = annualized return / max DD%
        # Annualized: (1+roi)^(365/duration_d) - 1
        if duration_d > 0 and max_dd_pct > 0:
            ann_ret_pct = ((1 + roi_pct/100) ** (365/duration_d) - 1) * 100
            calmar = ann_ret_pct / max_dd_pct
        else:
            calmar = float("inf") if roi_pct > 0 else 0

        # Profit factor approximation: assume avg_loss ≈ $50 (full bet lost),
        # avg_win = (realized + losses*50) / wins
        wins = r.get("wins", 0)
        losses = r.get("losses", 0)
        realized = r.get("realized", profit)  # arena's realized = closed PnL
        if losses > 0:
            total_loss = losses * 50.0  # rough
            total_win = realized + total_loss
            pf = total_win / total_loss if total_loss > 0 else float("inf")
        else:
            pf = float("inf")
        wr = wins / max(wins + losses, 1) * 100

        # Calmar caveat
        calmar_str = f"{calmar:.1f}" if calmar != float("inf") and abs(calmar) < 99999 else "inf"
        print(f"{i:>3} {r['name'][:55]:<55} ${r['equity']:>5.0f} {roi_pct:>+5.0f}% ${profit:>+7.0f} ${max_dd:>6.0f} {max_dd_pct:>5.1f}% ${mean_dd:>7.1f} {calmar_str:>7} {pf:>4.2f} {wr:>3.0f}% {r['trades']:>7}")

    # Notes
    print()
    print(f"Notes:")
    print(f"  ROI computed as (equity - $1000) / $1000")
    print(f"  $ profit = equity - $1000 (arena-scale, $1 actual = $50 virtual)")
    print(f"  maxDD = peak-to-trough across {len(paths_ts)} snapshots over {duration_d:.1f} days")
    print(f"  meanDD = average of all drawdowns from running peak")
    print(f"  Calmar = annualized return / maxDD% (NOT statistically meaningful for {duration_d:.1f}d < 30d)")
    print(f"  PF = profit factor ≈ realized + losses×$50 / losses×$50 (approximate, assumes avg_loss=$50)")


if __name__ == "__main__":
    main()
