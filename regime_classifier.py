#!/usr/bin/env python3
"""
Regime classifier for Polymarket markets.

Idea: different parameter regimes favor different strategy families.
Detect current regime per-market via:
  1. Volatility (rolling std of mid)
  2. Trend slope (linear regression of mid)
  3. Hour of day (UTC) — US/EU/Asia/Off
  4. Time-to-resolution (days)
  5. Mid-price band (extreme/middle)

Map regime → recommended strategy family. The "regime adapter" can route
fresh signals based on current regime instead of running blanket.

Output: bot-data/regime_per_market.json — per-market current regime + suggested
strategy families. Updated periodically.

Standalone classifier — no live trading. Run as offline analysis OR integrate
into an "adapter" bot that listens for signals and gates by regime.
"""
import json, math, os, time
from collections import defaultdict, deque
from datetime import datetime, timezone
import numpy as np

TICKS = "bot-data/arena_ticks.jsonl"
OUT = "bot-data/regime_per_market.json"


# ─── Regime buckets ────────────────────────────────────────────────────────
def vol_bucket(std_pct):
    """Volatility as % of mid."""
    if std_pct < 0.5: return "vol_low"
    if std_pct < 2.0: return "vol_mid"
    return "vol_high"


def trend_bucket(slope_pct_per_hour):
    """Slope normalized to %/hour."""
    if abs(slope_pct_per_hour) < 0.5: return "trend_flat"
    if abs(slope_pct_per_hour) < 2.0: return ("trend_up_mild" if slope_pct_per_hour > 0
                                              else "trend_down_mild")
    return ("trend_up_strong" if slope_pct_per_hour > 0 else "trend_down_strong")


def hour_bucket(hour_utc):
    if 0 <= hour_utc < 8: return "asia"
    if 8 <= hour_utc < 14: return "europe"
    if 14 <= hour_utc < 22: return "us"
    return "off"


def price_bucket(mid):
    if mid < 0.20: return "low"
    if mid > 0.80: return "high"
    if 0.40 <= mid <= 0.60: return "middle"
    return "mid"


# ─── Regime → strategy mapping ──────────────────────────────────────────────
# Based on backtest findings:
#  - vol_low + trend_flat → mean_rev (price pinning) wins
#  - vol_mid + trend_up_strong → momentum / breakout follow
#  - vol_high + trend_flat → mean_rev fade big moves
#  - off hours → fewer opportunities, slow strategies
REGIME_TO_FAMILIES = {
    # (vol, trend) → preferred families
    ("vol_low", "trend_flat"):          ["mean_rev_ema", "wavelet_mr", "bollinger"],
    ("vol_low", "trend_up_mild"):       ["mean_rev_ema", "rsi"],
    ("vol_low", "trend_down_mild"):     ["mean_rev_ema", "rsi"],
    ("vol_mid", "trend_flat"):          ["wavelet_mr", "bollinger", "zscore"],
    ("vol_mid", "trend_up_mild"):       ["breakout", "momentum"],
    ("vol_mid", "trend_down_mild"):     ["breakout", "momentum"],
    ("vol_mid", "trend_up_strong"):     ["breakout", "momentum"],
    ("vol_mid", "trend_down_strong"):   ["breakout", "momentum"],
    ("vol_high", "trend_flat"):         ["mean_rev_ema", "wavelet_mr"],   # fade extremes
    ("vol_high", "trend_up_strong"):    ["breakout"],   # follow if persistent
    ("vol_high", "trend_down_strong"):  ["breakout"],
}


def classify_market(prices, timestamps_unix):
    """Compute regime tags from a price series and timestamps."""
    n = len(prices)
    if n < 30:
        return None
    arr = np.array(prices)
    mid = arr[-1]
    # Volatility — std of last 60 ticks as % of mid
    win = arr[-60:] if n >= 60 else arr
    std_pct = np.std(win) / max(mid, 0.01) * 100
    vol = vol_bucket(std_pct)

    # Trend — linear regression slope over last 60 ticks
    if n >= 30:
        ts = np.array(timestamps_unix[-60:] if n >= 60 else timestamps_unix)
        ts_norm = ts - ts[0]
        if ts_norm[-1] > 0:
            slope = np.polyfit(ts_norm, arr[-len(ts_norm):], 1)[0]   # price per second
            slope_pct_hr = slope / max(mid, 0.01) * 100 * 3600
        else:
            slope_pct_hr = 0
    else:
        slope_pct_hr = 0
    trend = trend_bucket(slope_pct_hr)

    # Time
    last_ts = datetime.fromtimestamp(timestamps_unix[-1], tz=timezone.utc)
    hour = last_ts.hour
    hour_b = hour_bucket(hour)
    price_b = price_bucket(mid)

    # Lookup recommended families
    families = REGIME_TO_FAMILIES.get((vol, trend), ["mean_rev_ema"])

    return {
        "vol": vol, "vol_pct": round(std_pct, 2),
        "trend": trend, "slope_pct_hr": round(slope_pct_hr, 3),
        "hour_utc": hour, "hour_bucket": hour_b,
        "price": round(mid, 4), "price_bucket": price_b,
        "n_ticks_window": int(min(60, n)),
        "recommended_families": families,
    }


def main():
    if not os.path.exists(TICKS):
        print(f"ERROR: {TICKS} not found")
        return

    print(f"Reading ticks from {TICKS}...")
    # Per-market deque of (mid, ts)
    per_market = defaultdict(lambda: deque(maxlen=200))
    n_ticks = 0
    t0 = time.time()
    with open(TICKS) as f:
        for line in f:
            try:
                d = json.loads(line)
            except Exception:
                continue
            mid_id = d.get("market_id")
            if not mid_id:
                continue
            ts_iso = d.get("ts", "")
            try:
                ts_unix = datetime.fromisoformat(
                    ts_iso.replace("Z", "+00:00")
                ).timestamp()
            except Exception:
                continue
            per_market[mid_id].append((float(d.get("mid", 0)), ts_unix))
            n_ticks += 1
    print(f"  Read {n_ticks:,} ticks across {len(per_market)} markets in {time.time()-t0:.0f}s")

    # Classify CURRENT regime for each market (last 60 ticks)
    regimes = {}
    family_counts = defaultdict(int)
    vol_counts = defaultdict(int)
    trend_counts = defaultdict(int)
    for mid_id, pts in per_market.items():
        if len(pts) < 30:
            continue
        prices = [p[0] for p in pts]
        timestamps = [p[1] for p in pts]
        r = classify_market(prices, timestamps)
        if r is None:
            continue
        regimes[mid_id] = r
        for fam in r["recommended_families"]:
            family_counts[fam] += 1
        vol_counts[r["vol"]] += 1
        trend_counts[r["trend"]] += 1

    # Save
    out = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "n_markets_classified": len(regimes),
        "regime_distribution": {
            "vol": dict(vol_counts),
            "trend": dict(trend_counts),
            "recommended_family_demand": dict(family_counts),
        },
        "regimes": regimes,
    }
    with open(OUT, "w") as f:
        json.dump(out, f, indent=1)
    print(f"\n  Classified {len(regimes)} markets")
    print(f"  Saved {OUT}")
    print(f"\n  Volatility distribution:")
    for k, v in sorted(vol_counts.items()):
        print(f"    {k:<12} {v:>5} markets")
    print(f"\n  Trend distribution:")
    for k, v in sorted(trend_counts.items()):
        print(f"    {k:<22} {v:>5} markets")
    print(f"\n  Strategy family demand (number of markets where family is recommended):")
    for fam, n in sorted(family_counts.items(), key=lambda kv: -kv[1]):
        print(f"    {fam:<20} {n:>5}")


if __name__ == "__main__":
    main()
