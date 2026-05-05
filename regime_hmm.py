#!/usr/bin/env python3
"""
HMM-based regime detection (Hamilton 1989-style).

Replaces the hand-coded vol/trend buckets in regime_classifier.py with a
3-state Gaussian Hidden Markov Model fit per market via Baum-Welch on log
returns + rolling volatility, with Viterbi inference for current state.

States are labeled by their mean return + std:
  - "calm_revert"  — low vol, near-zero mean → mean reversion plays
  - "trend"        — low-mid vol, signed mean → momentum/breakout plays
  - "chaos"        — high vol → fade extremes / stay out

Theory references:
  Hamilton (1989) "A New Approach to the Economic Analysis of Nonstationary Time
  Series" — original Markov-switching regime model.
  Rabiner (1989) HMM tutorial — Baum-Welch training, Viterbi decoding.
  hmmlearn library implements both.

Output: bot-data/regime_hmm.json with per-market current state + model params.
"""
import json, os, time, warnings
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from hmmlearn import hmm

warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=UserWarning)

TICKS = "bot-data/arena_ticks.jsonl"
OUT = "bot-data/regime_hmm.json"

N_STATES = 3
WIN_VOL = 30          # window for rolling vol
MIN_TICKS = 60        # need at least this many ticks before fitting


def features(prices):
    """Compute log returns + rolling vol per tick."""
    p = np.asarray(prices)
    if len(p) < WIN_VOL + 2:
        return None
    log_ret = np.diff(np.log(np.clip(p, 0.01, 0.99)))
    # Rolling std of log returns over last WIN_VOL points (causal)
    vol = np.zeros_like(log_ret)
    for i in range(len(log_ret)):
        lo = max(0, i - WIN_VOL + 1)
        vol[i] = np.std(log_ret[lo:i+1]) if i >= 1 else 0
    # Stack as 2D obs
    X = np.column_stack([log_ret, vol])
    return X


def fit_hmm(X):
    """Fit Gaussian HMM with N_STATES, return model + state assignments."""
    if X is None or len(X) < MIN_TICKS:
        return None, None
    # Try, fall back if singular
    for attempt in range(3):
        try:
            model = hmm.GaussianHMM(
                n_components=N_STATES,
                covariance_type="diag",
                n_iter=50,
                random_state=42 + attempt,
                tol=1e-3,
            )
            model.fit(X)
            states = model.predict(X)
            return model, states
        except Exception:
            continue
    return None, None


def label_states(model, states, X):
    """Map state index → semantic label using mean return + vol of state cluster."""
    means = model.means_   # shape (n_states, 2)
    # means[:, 0] = log return, means[:, 1] = vol
    state_vol = means[:, 1]
    state_ret = means[:, 0]
    sorted_by_vol = np.argsort(state_vol)
    low_vol_state = sorted_by_vol[0]
    mid_vol_state = sorted_by_vol[1]
    high_vol_state = sorted_by_vol[2]
    # Of the two low-vol states, the one with abs(ret) closer to 0 is "calm_revert",
    # other is "trend"
    if abs(state_ret[low_vol_state]) < abs(state_ret[mid_vol_state]):
        labels = {low_vol_state: "calm_revert",
                  mid_vol_state: "trend",
                  high_vol_state: "chaos"}
    else:
        labels = {low_vol_state: "trend",
                  mid_vol_state: "calm_revert",
                  high_vol_state: "chaos"}
    return labels


# Strategy family recommendations per regime — refined from cluster_double_check
# DOUBLE ROBUST findings: in calm_revert use mean_rev + wavelet (small periods),
# in trend use breakout/momentum with right entry threshold, in chaos fade extremes
REGIME_FAMILIES = {
    "calm_revert": ["mean_rev_ema", "wavelet_mr", "wavelet_ms", "bollinger", "zscore"],
    "trend":        ["breakout", "momentum", "rsi"],
    "chaos":        ["mean_rev_ema", "wavelet_mr"],   # fade big moves
}


def main():
    if not os.path.exists(TICKS):
        print(f"ERROR: {TICKS} not found")
        return

    # Per-market price + ts series
    per_market = defaultdict(list)
    print("Reading ticks...")
    t0 = time.time()
    n = 0
    with open(TICKS) as f:
        for line in f:
            try:
                d = json.loads(line)
            except Exception:
                continue
            mid_id = d.get("market_id")
            if not mid_id:
                continue
            per_market[mid_id].append((d.get("mid", 0.0), d.get("ts", "")))
            n += 1
    print(f"  {n:,} ticks across {len(per_market)} markets ({time.time()-t0:.0f}s)")

    # Fit HMM per market
    out_regimes = {}
    state_dist = defaultdict(int)
    n_fitted = 0; n_skipped = 0
    print("Fitting HMM per market (this takes a minute or two)...")
    t1 = time.time()
    for i, (mid_id, pts) in enumerate(per_market.items()):
        if len(pts) < MIN_TICKS:
            n_skipped += 1
            continue
        prices = [p[0] for p in pts]
        X = features(prices)
        if X is None:
            n_skipped += 1
            continue
        model, states = fit_hmm(X)
        if model is None:
            n_skipped += 1
            continue
        labels = label_states(model, states, X)
        # Current state = last predicted
        cur_state_idx = int(states[-1])
        cur_label = labels[cur_state_idx]
        # Persistence — mean run length of current state
        runs = []
        run = 1
        for j in range(1, len(states)):
            if states[j] == states[j-1]:
                run += 1
            else:
                runs.append(run)
                run = 1
        runs.append(run)
        state_dist[cur_label] += 1
        out_regimes[mid_id] = {
            "state": cur_label,
            "state_idx": cur_state_idx,
            "n_ticks": len(prices),
            "mean_log_return": round(float(model.means_[cur_state_idx, 0]), 5),
            "vol": round(float(model.means_[cur_state_idx, 1]), 5),
            "transition_to_chaos_prob": round(
                float(model.transmat_[cur_state_idx,
                                      [k for k, v in labels.items() if v == "chaos"][0]]), 3
            ),
            "mean_run_len": round(float(np.mean(runs)), 1),
            "recommended_families": REGIME_FAMILIES.get(cur_label, []),
        }
        n_fitted += 1
        if (i + 1) % 200 == 0:
            print(f"  ... {i+1}/{len(per_market)} markets fitted, "
                  f"{n_fitted} ok, {n_skipped} skipped, {time.time()-t1:.0f}s")
    print(f"  Done in {time.time()-t1:.0f}s — fitted {n_fitted}, skipped {n_skipped}")

    out = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "method": "GaussianHMM(n_states=3) on (log_return, rolling_vol)",
        "n_markets": n_fitted,
        "current_state_distribution": dict(state_dist),
        "regimes": out_regimes,
    }
    Path(OUT).parent.mkdir(parents=True, exist_ok=True)
    json.dump(out, open(OUT, "w"), indent=1)
    print(f"\nSaved {OUT}")
    print(f"Current state distribution:")
    for state, n in state_dist.items():
        pct = n / n_fitted * 100 if n_fitted else 0
        print(f"  {state:<14} {n:>4} ({pct:.0f}%)")


if __name__ == "__main__":
    main()
