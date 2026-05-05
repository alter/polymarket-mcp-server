#!/usr/bin/env python3
"""
Bonferroni-corrected significance + Deflated Sharpe-equivalent for arena.

Theory (Bailey, Borwein, Lopez de Prado, Zhu 2014-2016):
  - With N=1535 strategies tested, expected max Sharpe under null hypothesis
    grows as ~√(2 ln N). For N=1535: √(2 × 7.34) ≈ 3.83.
  - The Deflated Sharpe Ratio (DSR) corrects for selection bias under multi-
    testing. DSR > 0.95 → 95% confident the strategy isn't a fluke.
  - For binary win/loss: each strategy has WR. Use binomial z-test:
        z = (WR - 0.5) / √(0.25/N)
    Bonferroni-correct: require p < 0.05/N → z_critical ≈ 4.0 for N=1535.

We compute both:
  1. Binomial z-score per strategy (vs WR=50%) and Bonferroni-significant flag.
  2. Pseudo-Sharpe from snapshot equity series, then Deflated Sharpe.

Output: bot-data/deflated_significance.json with per-strategy metrics ranked
by adjusted-significance. The robust short list = strategies that survived
multi-testing.
"""
import json, math, glob
from collections import defaultdict
from scipy import stats
import numpy as np

LIVE = "bot-data/arena_results.json"
BACKUPS_GLOB = "bot-data/backups/arena_results.json.*"
OUT = "bot-data/deflated_significance.json"

ALPHA = 0.05
EULER = 0.57721566490153286
STARTING = 1000.0


def expected_max_sharpe(N, sharpe_var):
    """E[max Sharpe under null] per Bailey & López de Prado formula.

      E[max_N] = √V × ((1-γ)Φ⁻¹(1-1/N) + γΦ⁻¹(1 - 1/(N·e)))

    Where γ = Euler-Mascheroni constant, Φ⁻¹ = inverse normal CDF.
    """
    if N <= 1 or sharpe_var <= 0:
        return 0.0
    z1 = stats.norm.ppf(1 - 1/N)
    z2 = stats.norm.ppf(1 - 1/(N * math.e))
    return math.sqrt(sharpe_var) * ((1 - EULER) * z1 + EULER * z2)


def deflated_sharpe(observed_sr, N, T, sharpe_var, skew=0, kurtosis=3):
    """Deflated Sharpe Ratio: probability the observed SR is non-random.

    Z = (SR_observed - E[max_N]) × √(T-1) / √(1 - skew·SR + (kurt-1)/4 · SR²)
    DSR = Φ(Z)

    Returns DSR ∈ [0, 1]; >0.95 means 95% confidence non-random.
    """
    if T <= 1 or N <= 1 or sharpe_var <= 0:
        return 0.0
    em = expected_max_sharpe(N, sharpe_var)
    denom_sq = 1 - skew * observed_sr + ((kurtosis - 1) / 4) * (observed_sr ** 2)
    if denom_sq <= 0:
        return 0.0
    z = (observed_sr - em) * math.sqrt(T - 1) / math.sqrt(denom_sq)
    return float(stats.norm.cdf(z))


def main():
    # Load current state
    ar = json.load(open(LIVE))
    res = ar["results"]
    N = len(res)
    print(f"Loaded {N} strategies from arena")

    # Build equity time series per strategy from snapshots
    series = defaultdict(list)
    paths = sorted(glob.glob(BACKUPS_GLOB) + [LIVE])
    for p in paths:
        try:
            d = json.load(open(p))
        except Exception:
            continue
        for r in d.get("results", []):
            series[r["id"]].append(r["equity"])
    T = max(len(v) for v in series.values()) if series else 1

    # Bonferroni critical z (one-sided)
    z_crit = stats.norm.ppf(1 - ALPHA / N)
    print(f"  Snapshots T={T}, N={N}, Bonferroni z_critical={z_crit:.2f}")
    print(f"  Expected max Sharpe under null (with var=1): "
          f"{expected_max_sharpe(N, 1.0):.2f}")

    # Compute Sharpe across population (need variance for DSR)
    sharpes = []
    pseudo_metrics = []
    for r in res:
        eqs = series.get(r["id"], [r["equity"]])
        if len(eqs) >= 3:
            rets = np.diff(eqs) / np.array(eqs[:-1])
            mu = np.mean(rets)
            sigma = np.std(rets)
            sr = mu / sigma * math.sqrt(len(rets)) if sigma > 1e-9 else 0
        else:
            sr = 0
        pseudo_metrics.append({"id": r["id"], "sr": sr})
        if abs(sr) > 0.01:
            sharpes.append(sr)
    sharpe_var = float(np.var(sharpes)) if sharpes else 1.0
    print(f"  Sharpe variance across population: {sharpe_var:.3f}")
    print(f"  Adjusted expected max Sharpe: {expected_max_sharpe(N, sharpe_var):.3f}")

    # Compute per-strategy metrics
    rows = []
    for r in res:
        wins = r.get("wins", 0)
        losses = r.get("losses", 0)
        n = wins + losses
        if n == 0:
            continue
        wr = wins / n
        # Binomial z-test vs WR=0.5
        z_bin = (wr - 0.5) / math.sqrt(0.25 / n)
        # Bonferroni-corrected p-value
        p_raw = 2 * (1 - stats.norm.cdf(abs(z_bin)))
        p_bonf = min(p_raw * N, 1.0)
        bonferroni_pass = z_bin >= z_crit
        # Sharpe + DSR
        eqs = series.get(r["id"], [r["equity"]])
        T_i = len(eqs)
        if T_i >= 3:
            rets = np.diff(eqs) / np.array(eqs[:-1])
            mu = np.mean(rets)
            sigma = np.std(rets)
            sharpe = mu / sigma * math.sqrt(T_i) if sigma > 1e-9 else 0
            # Skew/kurtosis
            try:
                skew = float(stats.skew(rets))
                kurt = float(stats.kurtosis(rets, fisher=False))
            except Exception:
                skew, kurt = 0, 3
            dsr = deflated_sharpe(sharpe, N, T_i, sharpe_var, skew, kurt)
        else:
            sharpe = 0; dsr = 0
        roi = (r["equity"] - STARTING) / STARTING * 100
        rows.append({
            "id": r["id"], "name": r["name"][:60],
            "indicator": r["params"]["indicator"],
            "trades": n, "wr": round(wr * 100, 1),
            "roi_pct": round(roi, 1),
            "z_binomial": round(z_bin, 2),
            "p_bonferroni": round(p_bonf, 4),
            "bonferroni_pass": bool(bonferroni_pass),
            "sharpe_pseudo": round(sharpe, 2),
            "dsr": round(dsr, 3),
            "dsr_pass": bool(dsr > 0.95 and sharpe > 0 and roi > 0),  # non-random AND winning
            "retired": r.get("retired", False),
        })

    rows.sort(key=lambda r: -r["z_binomial"])
    n_bonf_pass = sum(1 for r in rows if r["bonferroni_pass"])
    n_dsr_pass = sum(1 for r in rows if r["dsr_pass"])
    n_both = sum(1 for r in rows if r["bonferroni_pass"] and r["dsr_pass"])

    out = {
        "n_strategies": N, "snapshots_T": T,
        "alpha": ALPHA, "z_critical_bonferroni": round(z_crit, 3),
        "sharpe_var_population": round(sharpe_var, 3),
        "expected_max_sharpe": round(expected_max_sharpe(N, sharpe_var), 3),
        "n_pass_bonferroni": n_bonf_pass,
        "n_pass_dsr": n_dsr_pass,
        "n_pass_both": n_both,
        "results": rows,
    }
    json.dump(out, open(OUT, "w"), indent=1)
    print(f"\nSaved {OUT}")
    print(f"  Pass Bonferroni (z>{z_crit:.2f}): {n_bonf_pass}/{N} ({n_bonf_pass/N*100:.0f}%)")
    print(f"  Pass DSR > 0.95: {n_dsr_pass}/{N} ({n_dsr_pass/N*100:.0f}%)")
    print(f"  Pass BOTH: {n_both}/{N} ({n_both/N*100:.0f}%) ← REAL EDGE candidates")

    print(f"\n=== TOP 25 by z_binomial (must also pass DSR > 0.95) ===")
    print(f"{'name':<55} {'WR':>5} {'n':>5} {'z':>6} {'p_bonf':>8} {'DSR':>5} {'pass':>4}")
    for r in rows[:25]:
        flag = ("BOTH" if r["bonferroni_pass"] and r["dsr_pass"]
                else "BONF" if r["bonferroni_pass"]
                else "DSR" if r["dsr_pass"] else "")
        print(f"  {r['name'][:55]:<55} {r['wr']:>4.0f}% {r['trades']:>5} "
              f"{r['z_binomial']:>+5.1f} {r['p_bonferroni']:>7.2g} {r['dsr']:>4.2f} {flag:>4}")


if __name__ == "__main__":
    main()
