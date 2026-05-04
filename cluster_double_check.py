#!/usr/bin/env python3
"""
Cross-reference clusters that pass BOTH backtest AND live arena.

Reads:
  bot-data/arena_results.json — live equity (5.9 days)
  bot-data/arena_backtest_full.json — historical backtest (26 days)

Clusters by parameter neighborhood, computes within-cluster median ROI from
each source. A cluster is "GOLD" if median > 0 in BOTH sources.

This catches the overfit trap: live winner that backtest says is loser →
режимный edge, не доверять. Live + backtest both positive → real edge.
"""
import json
from collections import defaultdict
from cluster_strategies import cluster_key

LIVE = "bot-data/arena_results.json"
BT = "bot-data/arena_backtest_full.json"


def main():
    live = json.load(open(LIVE))
    bt = json.load(open(BT))
    live_by_id = {r["id"]: r for r in live["results"]}
    bt_by_id = {r["id"]: r for r in bt["results"]}

    # Cluster key per strategy via params from live (has full params dict)
    cluster_live = defaultdict(list)
    cluster_bt = defaultdict(list)
    for sid, r in live_by_id.items():
        roi = (r["equity"] - 1000) / 1000 * 100
        cluster_live[cluster_key(r["params"])].append(roi)
    for sid, r in bt_by_id.items():
        if sid not in live_by_id:
            continue
        bt_roi = r.get("roi_pct", 0)
        cluster_bt[cluster_key(live_by_id[sid]["params"])].append(bt_roi)

    # Combined: clusters present in both
    combined = []
    for c in set(cluster_live.keys()) & set(cluster_bt.keys()):
        live_rois = cluster_live[c]
        bt_rois = cluster_bt[c]
        if not live_rois or not bt_rois or len(live_rois) < 2:
            continue
        live_med = sorted(live_rois)[len(live_rois)//2]
        bt_med = sorted(bt_rois)[len(bt_rois)//2]
        live_min = min(live_rois)
        bt_min = min(bt_rois)
        n = len(live_rois)
        combined.append({
            "cluster": c, "n": n,
            "live_median": round(live_med, 1),
            "live_min": round(live_min, 1),
            "live_max": round(max(live_rois), 1),
            "bt_median": round(bt_med, 1),
            "bt_min": round(bt_min, 1),
            "bt_max": round(max(bt_rois), 1),
            "robust_live": live_min > 0,
            "robust_bt": bt_min > 0,
            "gold": live_med > 0 and bt_med > 0,
            "double_robust": live_min > 0 and bt_min > 0,
        })

    # Sort by gold + bt_median
    combined.sort(key=lambda x: (-int(x["gold"]), -x["bt_median"]))

    out = {
        "n_clusters": len(combined),
        "n_gold": sum(1 for c in combined if c["gold"]),
        "n_double_robust": sum(1 for c in combined if c["double_robust"]),
        "results": combined,
    }
    json.dump(out, open("bot-data/cluster_double_check.json", "w"), indent=1)

    gold = [c for c in combined if c["gold"]]
    double = [c for c in combined if c["double_robust"]]

    print(f"Clusters analyzed: {len(combined)}")
    print(f"  GOLD (both medians > 0): {len(gold)}")
    print(f"  DOUBLE ROBUST (all variants positive in both): {len(double)}")
    print()
    print(f"=== TOP 20 GOLD by backtest median (real edge candidates) ===")
    print(f"{'cluster':<70} {'n':>3} {'live_med':>8} {'bt_med':>7} {'robust':>6}")
    for c in gold[:20]:
        flag = ""
        if c["double_robust"]: flag = "DBL"
        elif c["robust_live"]: flag = "L"
        elif c["robust_bt"]: flag = "B"
        print(f"  {c['cluster'][:68]:<70} {c['n']:>3} "
              f"{c['live_median']:>+6.0f}%   {c['bt_median']:>+5.0f}%   {flag:>4}")

    print()
    print(f"=== OVERFIT (live > 30% but backtest < -10%) ===")
    overfit = [c for c in combined if c["live_median"] > 30 and c["bt_median"] < -10]
    overfit.sort(key=lambda c: c["live_median"] - c["bt_median"], reverse=True)
    for c in overfit[:15]:
        print(f"  {c['cluster'][:68]:<70} live=+{c['live_median']:.0f}% bt={c['bt_median']:+.0f}% (n={c['n']})")

    print()
    print(f"=== UNDERVALUED (backtest > 0 but live < 0) ===")
    under = [c for c in combined if c["bt_median"] > 5 and c["live_median"] < -5]
    under.sort(key=lambda c: c["bt_median"], reverse=True)
    for c in under[:10]:
        print(f"  {c['cluster'][:68]:<70} live={c['live_median']:+.0f}% bt={c['bt_median']:+.0f}% (n={c['n']})")


if __name__ == "__main__":
    main()
