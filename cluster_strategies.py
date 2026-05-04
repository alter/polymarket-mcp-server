#!/usr/bin/env python3
"""
Parameter-neighborhood clustering of strategies.

User's insight: SMA(5,100) ≈ SMA(6,100) ≈ SMA(7,101) — these are essentially
the same strategy with parameter wiggle. We should:
  1) group strategies by parameter neighborhood
  2) within each cluster pick the BEST risk-adjusted variant
     (ROI / max_DD ratio, or Sharpe-equivalent)
  3) deploy only the cluster representatives — not all permutations

This validates ROBUSTNESS: a real edge survives parameter perturbation.
A single-point optimum is overfit.

Reads:
  bot-data/arena_results.json — current live equities (5+ days of paper trading)
  bot-data/arena_backtest_full.json — backtest ROI (when ready)
  bot-data/backups/*.json — equity time series for DD computation

Writes:
  bot-data/strategy_clusters.json
"""
import json, glob, os
from collections import defaultdict


def cluster_key(params):
    """Bucket parameters into neighborhoods.

    period bucket: [3-5], [6-10], [11-15], [16-25], [26-40], [41-100]
    entry: rounded to coarse buckets
    stop_loss: [sloff], [-5 to -10], [-15 to -25], [-30+]
    take_profit: 5/10/15/20/25%
    """
    ind = params["indicator"]
    p = params["period"]
    e = params["entry_param"]
    sl = params["stop_loss"]
    tp = params["take_profit"]
    fee = params["fee_free_only"]
    side = params.get("side_bias", "both")

    # Period bucket
    if p <= 5:    p_buck = "p3-5"
    elif p <= 10: p_buck = "p6-10"
    elif p <= 15: p_buck = "p11-15"
    elif p <= 25: p_buck = "p16-25"
    elif p <= 40: p_buck = "p26-40"
    else:         p_buck = "p41+"

    # Entry — depends on indicator type. RSI uses ints, others use floats.
    if e >= 1:  # RSI threshold
        if e <= 25: e_buck = "e<=25"
        elif e <= 50: e_buck = "e26-50"
        elif e <= 70: e_buck = "e51-70"
        elif e <= 80: e_buck = "e71-80"
        else: e_buck = "e>80"
    else:
        if e < 0.015: e_buck = "e<1.5%"
        elif e < 0.03: e_buck = "e1.5-3%"
        elif e < 0.07: e_buck = "e3-7%"
        else: e_buck = "e>7%"

    # Stop-loss
    if sl < -0.90: sl_buck = "sloff"
    elif sl >= -0.10: sl_buck = "sl-tight"   # 5-10%
    elif sl >= -0.25: sl_buck = "sl-mid"     # 15-25%
    else: sl_buck = "sl-wide"

    # Take-profit
    if tp <= 0.05: tp_buck = "tp<=5"
    elif tp <= 0.10: tp_buck = "tp10"
    elif tp <= 0.20: tp_buck = "tp15-20"
    else: tp_buck = "tp25+"

    fee_buck = "free" if fee else "paid"
    return f"{ind}|{p_buck}|{e_buck}|{sl_buck}|{tp_buck}|{fee_buck}|{side}"


def compute_dd(equities):
    """Max drawdown from peak."""
    if not equities:
        return 0.0
    peak = equities[0]
    max_dd = 0.0
    for eq in equities:
        if eq > peak: peak = eq
        dd = peak - eq
        if dd > max_dd: max_dd = dd
    return max_dd, peak


def main():
    ar = json.load(open("bot-data/arena_results.json"))
    res = ar["results"]
    print(f"Loaded {len(res)} strategies")

    # Build equity time-series per strategy from snapshots
    paths = sorted(glob.glob("bot-data/backups/arena_results.json.*") +
                   ["bot-data/arena_results.json"])
    series = defaultdict(list)
    for p in paths:
        try:
            d = json.load(open(p))
        except Exception:
            continue
        for r in d.get("results", []):
            series[r["id"]].append(r["equity"])

    # For each strategy compute risk-adjusted metric
    enriched = []
    for r in res:
        eq_series = series.get(r["id"], [r["equity"]])
        max_dd, peak = compute_dd(eq_series)
        max_dd_pct = max_dd / peak * 100 if peak > 0 else 0
        roi = (r["equity"] - 1000) / 1000 * 100
        # Calmar-ish: ROI / max_DD% (higher is better, normalized)
        # If DD=0, we use ROI directly with cap to avoid infinity
        risk_adj = roi / max(max_dd_pct, 1.0)
        enriched.append({
            **r,
            "roi": round(roi, 1),
            "max_dd": round(max_dd, 1),
            "max_dd_pct": round(max_dd_pct, 1),
            "risk_adj": round(risk_adj, 2),
            "cluster": cluster_key(r["params"]),
        })

    # Group by cluster
    clusters = defaultdict(list)
    for e in enriched:
        clusters[e["cluster"]].append(e)

    print(f"Clusters: {len(clusters)} (avg {len(enriched)/len(clusters):.1f} strats/cluster)")

    # For each cluster pick best by risk_adj (with tie-break on ROI)
    summary = []
    for cluster, strats in clusters.items():
        strats.sort(key=lambda s: (-s["risk_adj"], -s["roi"]))
        best = strats[0]
        median_roi = sorted([s["roi"] for s in strats])[len(strats)//2]
        all_positive = all(s["roi"] > 0 for s in strats)
        summary.append({
            "cluster": cluster,
            "n_strats": len(strats),
            "best_id": best["id"], "best_name": best["name"],
            "best_roi": best["roi"], "best_dd_pct": best["max_dd_pct"],
            "best_risk_adj": best["risk_adj"],
            "median_roi": round(median_roi, 1),
            "robust": all_positive,        # all variants positive = robust edge
            "max_roi_in_cluster": max(s["roi"] for s in strats),
            "min_roi_in_cluster": min(s["roi"] for s in strats),
        })

    # Sort by best risk_adj
    summary.sort(key=lambda s: -s["best_risk_adj"])

    # Save
    out = {
        "n_strategies": len(res),
        "n_clusters": len(clusters),
        "clusters": summary,
        "details_per_strategy": enriched,
    }
    json.dump(out, open("bot-data/strategy_clusters.json", "w"), indent=1)

    # Print top
    print(f"\nTOP 15 clusters by risk-adjusted (ROI / max_DD%):")
    print(f"{'cluster':<70} {'n':>3} {'roi':>5} {'dd%':>4} {'adj':>5} {'med_roi':>7} {'robust':>6}")
    for c in summary[:15]:
        print(f"  {c['cluster'][:68]:<70} {c['n_strats']:>3} {c['best_roi']:>+4.0f}% {c['best_dd_pct']:>3.0f}% "
              f"{c['best_risk_adj']:>4.1f} {c['median_roi']:>+6.0f}% {'YES' if c['robust'] else '   '}")

    # Robust clusters with high median (the gold)
    robust = [c for c in summary if c["robust"] and c["median_roi"] > 20 and c["n_strats"] >= 2]
    robust.sort(key=lambda c: -c["median_roi"])
    print(f"\n{'='*80}")
    print(f"ROBUST CLUSTERS (all variants ROI>0, median>20%, n>=2):")
    print(f"{'cluster':<70} {'n':>3} {'min':>5} {'med':>5} {'max':>5}")
    for c in robust[:20]:
        print(f"  {c['cluster'][:68]:<70} {c['n_strats']:>3} "
              f"{c['min_roi_in_cluster']:>+4.0f}% {c['median_roi']:>+4.0f}% "
              f"{c['max_roi_in_cluster']:>+4.0f}%")

    # Anti-robust: clusters where some variants succeed and others fail badly (overfit signature)
    overfit = [c for c in summary
               if c["max_roi_in_cluster"] > 50 and c["min_roi_in_cluster"] < -20
               and c["n_strats"] >= 3]
    overfit.sort(key=lambda c: c["min_roi_in_cluster"])
    print(f"\nOVERFIT CLUSTERS (some variants +50%, others <-20%, n>=3):")
    for c in overfit[:10]:
        print(f"  {c['cluster'][:68]:<70} {c['n_strats']:>3} "
              f"min={c['min_roi_in_cluster']:>+4.0f}% max={c['max_roi_in_cluster']:>+4.0f}%")


if __name__ == "__main__":
    main()
