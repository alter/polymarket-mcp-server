#!/usr/bin/env python3
"""
Final strategy shortlist — combines all gates:
  1. Bonferroni z-test on WR (real binary edge after multi-testing correction)
  2. Cluster GOLD (parameter-robust: median ROI > 0 in BOTH live arena AND
     historical backtest)
  3. HMM regime fit (current regime favours this strategy family)

Strategies passing all 3 gates are the SHORT LIST for real-money deployment.

Output: bot-data/final_shortlist.json
"""
import json
from collections import defaultdict
from cluster_strategies import cluster_key

DEFLATED = "bot-data/deflated_significance.json"
DOUBLE = "bot-data/cluster_double_check.json"
ARENA = "bot-data/arena_results.json"
REGIME = "bot-data/regime_hmm.json"
OUT = "bot-data/final_shortlist.json"


def main():
    sig = json.load(open(DEFLATED))
    dbl = json.load(open(DOUBLE))
    arena = json.load(open(ARENA))
    regime = json.load(open(REGIME))

    # Build lookups
    arena_by_id = {r["id"]: r for r in arena["results"]}
    sig_by_id = {r["id"]: r for r in sig["results"]}
    gold_clusters = {c["cluster"]: c for c in dbl["results"] if c["gold"]}
    double_robust_clusters = {c["cluster"]: c for c in dbl["results"]
                              if c["double_robust"]}

    # Regime distribution
    regime_state_counts = regime.get("current_state_distribution", {})
    n_regime_markets = sum(regime_state_counts.values())

    # Family → which regimes recommend it (inverse mapping)
    REGIME_FAMILIES = {
        "calm_revert": ["mean_rev_ema", "wavelet_mr", "wavelet_ms",
                        "bollinger", "zscore"],
        "trend":        ["breakout", "momentum", "rsi"],
        "chaos":        ["mean_rev_ema", "wavelet_mr"],
    }
    family_to_regimes = defaultdict(list)
    for state, fams in REGIME_FAMILIES.items():
        for f in fams:
            family_to_regimes[f].append(state)

    # For each strategy: collect all gates
    rows = []
    for r in arena["results"]:
        if r.get("retired"):
            continue
        ind = r["params"]["indicator"]
        ck = cluster_key(r["params"])
        sig_row = sig_by_id.get(r["id"], {})
        gold = ck in gold_clusters
        double_robust = ck in double_robust_clusters
        bonf_pass = sig_row.get("bonferroni_pass", False)
        roi = (r["equity"] - 1000) / 1000 * 100

        # Find regimes that recommend this family
        # Map "wavelet_ms" → "wavelet_mr" same family etc
        family_map = {"mean_rev_sma": "mean_rev_ema",
                      "wavelet_ms": "wavelet_mr"}
        family = family_map.get(ind, ind)
        recommended_regimes = family_to_regimes.get(family, [])
        # How many markets currently in each recommended regime
        regime_market_count = sum(regime_state_counts.get(s, 0)
                                  for s in recommended_regimes)
        regime_alignment_pct = (regime_market_count / n_regime_markets * 100
                                if n_regime_markets else 0)

        n_gates = sum([bonf_pass, gold, double_robust])
        rows.append({
            "id": r["id"], "name": r["name"][:60],
            "indicator": ind,
            "trades": r["trades"], "wr": sig_row.get("wr", 0),
            "z_binomial": sig_row.get("z_binomial", 0),
            "roi_pct": round(roi, 1),
            "cluster": ck,
            "gates": {
                "bonferroni": bonf_pass,
                "gold_cluster": gold,
                "double_robust_cluster": double_robust,
            },
            "n_gates_passed": n_gates,
            "regime_alignment_pct": round(regime_alignment_pct, 1),
            "recommended_regimes": recommended_regimes,
        })

    # Filter: must pass Bonferroni (real edge), be in gold cluster (robust to params),
    # have positive ROI, and >0 regime alignment
    shortlist = [
        r for r in rows
        if r["gates"]["bonferroni"]
        and r["gates"]["gold_cluster"]
        and r["roi_pct"] > 0
        and r["regime_alignment_pct"] > 30
    ]
    shortlist.sort(key=lambda r: -r["z_binomial"])

    # Even tighter: double_robust
    elite = [r for r in shortlist if r["gates"]["double_robust_cluster"]]

    # Save
    out = {
        "n_total_strategies": len(rows),
        "n_passing_bonferroni": sum(1 for r in rows if r["gates"]["bonferroni"]),
        "n_in_gold_cluster": sum(1 for r in rows if r["gates"]["gold_cluster"]),
        "n_double_robust_cluster": sum(1 for r in rows
                                       if r["gates"]["double_robust_cluster"]),
        "n_shortlist": len(shortlist),
        "n_elite": len(elite),
        "shortlist": shortlist,
        "elite": elite,
    }
    json.dump(out, open(OUT, "w"), indent=1)
    print(f"Total active strategies: {len(rows)}")
    print(f"  Pass Bonferroni:        {out['n_passing_bonferroni']}")
    print(f"  In GOLD cluster:        {out['n_in_gold_cluster']}")
    print(f"  In DOUBLE_ROBUST:       {out['n_double_robust_cluster']}")
    print(f"  SHORTLIST (all gates):  {len(shortlist)}")
    print(f"  ELITE (also double-robust): {len(elite)}")

    print(f"\n=== ELITE SHORTLIST — strategies passing ALL gates ===")
    print(f"{'name':<55} {'ROI':>6} {'WR':>5} {'z':>5} {'reg%':>5} {'cluster':<60}")
    for r in elite[:20]:
        print(f"  {r['name'][:55]:<55} {r['roi_pct']:>+5.0f}% {r['wr']:>4.0f}% "
              f"{r['z_binomial']:>+4.1f} {r['regime_alignment_pct']:>4.0f}% {r['cluster'][:58]:<60}")

    print(f"\n=== SHORTLIST (gold but not double-robust) ===")
    not_elite = [r for r in shortlist if r not in elite]
    for r in not_elite[:15]:
        print(f"  {r['name'][:55]:<55} {r['roi_pct']:>+5.0f}% {r['wr']:>4.0f}% "
              f"{r['z_binomial']:>+4.1f} {r['regime_alignment_pct']:>4.0f}% {r['cluster'][:58]:<60}")


if __name__ == "__main__":
    main()
