#!/usr/bin/env python3
"""
Phase 3 OBI/microprice backtest — measures predictive power of L1 orderbook
imbalance signals over future price changes (does NOT require resolutions —
uses next-snapshot mid change as outcome).

Output: bot-data/phase3_obi_results.json
"""
import json, os, sys, time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

DATA = Path("data")
SNAPS_FILE = DATA / "orderbook_snapshots.jsonl"
RESULTS_FILE = DATA / "phase3_obi_results.json"

# Forward horizons (snapshot index offsets — each ~30s)
FORWARD_K = [1, 2, 5, 10]  # 30s, 60s, 150s, 300s

# Thresholds for binning
OBI_THR = [0.1, 0.2, 0.3, 0.5, 0.7]
MU_THR = [0.001, 0.005, 0.01, 0.02, 0.03]

WINSOR_PCT = 0.10  # clip fwd at +/-10%


def load_snapshots():
    """Return dict[market_id] = list of {ts, mid, OBI_L1, OBI_L3, mu_dev}."""
    by_mkt = defaultdict(list)
    if not SNAPS_FILE.exists():
        print(f"  {SNAPS_FILE} not found")
        return by_mkt
    with open(SNAPS_FILE) as f:
        for line in f:
            try:
                s = json.loads(line)
                ts = datetime.fromisoformat(
                    s["ts"].replace("Z", "+00:00")
                ).timestamp()
                bids, asks = s["bids"], s["asks"]
                if not bids or not asks:
                    continue
                b_p, b_s = bids[-1]
                a_p, a_s = asks[-1]
                if b_s + a_s <= 0 or b_p <= 0 or a_p <= 0:
                    continue
                # OBI L1
                obi_l1 = (b_s - a_s) / (b_s + a_s)
                # OBI L3 (sum top 3 levels)
                b_top3 = sum(b[1] for b in bids[-3:])
                a_top3 = sum(a[1] for a in asks[-3:])
                obi_l3 = ((b_top3 - a_top3) / (b_top3 + a_top3)
                          if (b_top3 + a_top3) > 0 else 0)
                # Microprice deviation
                mid = (b_p + a_p) / 2
                micro = (b_s * a_p + a_s * b_p) / (b_s + a_s)
                mu_dev = (micro - mid) / mid
                by_mkt[s["market_id"]].append({
                    "ts": ts, "mid": mid,
                    "obi_l1": obi_l1, "obi_l3": obi_l3,
                    "mu_dev": mu_dev,
                })
            except Exception:
                continue
    for mid in by_mkt:
        by_mkt[mid].sort(key=lambda x: x["ts"])
    return by_mkt


def winsorize(v, pct):
    return max(min(v, pct), -pct)


def evaluate(by_mkt):
    """For each market, build (signal, fwd[K]) pairs and aggregate by bucket."""
    # results[(feature, k, threshold)] = {n, mean_fwd_pos, mean_fwd_neg, ...}
    pairs = []  # (obi_l1, obi_l3, mu_dev, [fwd_k])
    n_total = 0
    for mid, snaps in by_mkt.items():
        n = len(snaps)
        if n < max(FORWARD_K) + 1:
            continue
        # Build numpy arrays
        ts_arr = np.array([s["ts"] for s in snaps])
        mid_arr = np.array([s["mid"] for s in snaps])
        obi_l1 = np.array([s["obi_l1"] for s in snaps])
        obi_l3 = np.array([s["obi_l3"] for s in snaps])
        mu_dev = np.array([s["mu_dev"] for s in snaps])
        # Forward returns (vectorized)
        for i in range(n - max(FORWARD_K)):
            cur_mid = mid_arr[i]
            if cur_mid <= 0:
                continue
            fwd = []
            for k in FORWARD_K:
                if i + k < n:
                    f = (mid_arr[i + k] - cur_mid) / cur_mid
                    fwd.append(winsorize(f, WINSOR_PCT))
                else:
                    fwd.append(None)
            pairs.append((obi_l1[i], obi_l3[i], mu_dev[i], fwd))
            n_total += 1

    print(f"  Pairs: {n_total:,}")

    # Aggregate by feature × threshold × forward k
    def bucket_stats(values, thr, fwd_arr):
        pos = [f for v, f in zip(values, fwd_arr) if v > thr and f is not None]
        neg = [f for v, f in zip(values, fwd_arr) if v < -thr and f is not None]
        return {
            "n_pos": len(pos),
            "n_neg": len(neg),
            "mean_fwd_pos_pct": (np.mean(pos) * 100) if pos else 0,
            "mean_fwd_neg_pct": (np.mean(neg) * 100) if neg else 0,
            "wr_pos": ((sum(1 for f in pos if f > 0) / len(pos) * 100)
                       if pos else 0),
            "wr_neg": ((sum(1 for f in neg if f < 0) / len(neg) * 100)
                       if neg else 0),
        }

    summary = {}
    obi_l1_vals = [p[0] for p in pairs]
    obi_l3_vals = [p[1] for p in pairs]
    mu_vals = [p[2] for p in pairs]

    for ki, k in enumerate(FORWARD_K):
        fwd_vals = [p[3][ki] for p in pairs]
        # Skip Nones
        for thr in OBI_THR:
            summary[f"obi_l1|k{k}|thr{thr}"] = bucket_stats(obi_l1_vals, thr, fwd_vals)
            summary[f"obi_l3|k{k}|thr{thr}"] = bucket_stats(obi_l3_vals, thr, fwd_vals)
        for thr in MU_THR:
            summary[f"mu_dev|k{k}|thr{thr}"] = bucket_stats(mu_vals, thr, fwd_vals)

    # Correlations (raw)
    import statistics
    corrs = {}
    for ki, k in enumerate(FORWARD_K):
        fwd_vals = [p[3][ki] for p in pairs if p[3][ki] is not None]
        obi_l1_paired = [p[0] for p in pairs if p[3][ki] is not None]
        obi_l3_paired = [p[1] for p in pairs if p[3][ki] is not None]
        mu_paired = [p[2] for p in pairs if p[3][ki] is not None]
        if len(fwd_vals) < 50:
            continue
        try:
            corrs[f"obi_l1_k{k}"] = round(
                statistics.correlation(obi_l1_paired, fwd_vals), 4)
            corrs[f"obi_l3_k{k}"] = round(
                statistics.correlation(obi_l3_paired, fwd_vals), 4)
            corrs[f"mu_dev_k{k}"] = round(
                statistics.correlation(mu_paired, fwd_vals), 4)
        except Exception:
            pass

    return n_total, summary, corrs


def main():
    print(f"[{datetime.now():%H:%M:%S}] Phase 3 OBI backtest starting...")
    t0 = time.time()
    by_mkt = load_snapshots()
    print(f"  {len(by_mkt)} markets, "
          f"{sum(len(v) for v in by_mkt.values()):,} total snapshots")

    if not by_mkt:
        print("  No data — aborting")
        return

    n_pairs, summary, corrs = evaluate(by_mkt)
    print(f"\n  Evaluation took {time.time()-t0:.1f}s")
    print(f"\n━━━ Correlations (winsorized fwd ±10%) ━━━")
    for k, v in sorted(corrs.items()):
        print(f"  {k}: {v:+.4f}")

    # Top-edge buckets (mean_fwd_pos high → predicts upward; mean_fwd_neg low → predicts downward)
    print(f"\n━━━ Top buckets by signed edge ━━━")
    candidates = []
    for key, st in summary.items():
        feat, k, thr = key.split("|")
        if st["n_pos"] >= 20:
            candidates.append({
                "key": key, "side": "pos", "n": st["n_pos"],
                "mean_fwd_pct": st["mean_fwd_pos_pct"],
                "wr": st["wr_pos"],
            })
        if st["n_neg"] >= 20:
            candidates.append({
                "key": key, "side": "neg", "n": st["n_neg"],
                "mean_fwd_pct": st["mean_fwd_neg_pct"],
                "wr": st["wr_neg"],
            })

    # Best edges by abs(mean_fwd_pct × sign expectation)
    # Edge: pos side should have positive fwd (follow), or negative fwd (fade)
    # Show top by absolute mean
    candidates.sort(key=lambda c: -abs(c["mean_fwd_pct"]))
    print(f"  {'feature':<30} {'side':<5} {'n':>5} {'mean_fwd':>9} {'WR':>5}")
    for c in candidates[:25]:
        print(f"  {c['key']:<30} {c['side']:<5} {c['n']:>5} "
              f"{c['mean_fwd_pct']:>+8.4f}% {c['wr']:>5.1f}%")

    # Save
    out = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "n_markets": len(by_mkt),
        "n_snapshots": sum(len(v) for v in by_mkt.values()),
        "n_pairs": n_pairs,
        "winsor_pct": WINSOR_PCT,
        "forward_k": FORWARD_K,
        "correlations": corrs,
        "buckets": summary,
        "candidates": candidates[:50],
    }
    with open(RESULTS_FILE, "w") as f:
        json.dump(out, f, indent=1)
    print(f"\nSaved to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
