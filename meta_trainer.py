#!/usr/bin/env python3
"""
Meta-labeler trainer (Stage 66 adaptation).

Reads arena_trades.jsonl, builds a bucket-based lookup table predicting
P(trade wins) given categorical features. Saves to bot-data/meta_model.json.

Features (all available at entry time):
  - indicator family (mean_rev_ema, wavelet_mr, forest_25, etc.)
  - has_fee (fee_type present)
  - side (YES/NO)
  - entry price bucket (low/mid_low/mid/mid_high/high)
  - hour of day (0-5, 6-11, 12-17, 18-23)

Bucket must have >= MIN_SAMPLES observations to be used. If lookup misses
(never seen combo), returns None → caller uses default (no meta gate).
"""
import json, os, time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

DATA = Path("bot-data")
TRADES = DATA / "arena_trades.jsonl"
MODEL = DATA / "meta_model.json"
MIN_SAMPLES = 30      # need 30+ trades in bucket to trust WR
MIN_WR_TRADE = 0.55   # only trade when meta WR >= 55%


def bucket_price(p):
    if p is None: return "unknown"
    if p < 0.20: return "low"
    if p < 0.40: return "mid_low"
    if p < 0.60: return "mid"
    if p < 0.80: return "mid_high"
    return "high"


def bucket_hour(h):
    if h < 6: return "night"
    if h < 12: return "morning"
    if h < 18: return "afternoon"
    return "evening"


def indicator_family(ind):
    if ind.startswith("forest"):
        return "forest"
    if ind.startswith("ensemble"):
        return "ensemble"
    if ind.startswith("wavelet"):
        return "wavelet"
    return ind  # keep original for EMA/SMA/RSI/etc.


def feature_key(trade):
    """Build lookup key from trade record."""
    opened_at = trade.get("opened_at", 0)
    try:
        hour = datetime.fromtimestamp(opened_at, timezone.utc).hour
    except (OSError, ValueError, TypeError):
        hour = 0

    ind = trade.get("indicator", "?")
    fam = indicator_family(ind)
    has_fee = "fee" if trade.get("fee", 0) > 0 else "free"
    side = trade.get("side", "?")
    price_b = bucket_price(trade.get("entry"))
    hour_b = bucket_hour(hour)

    return (fam, has_fee, side, price_b, hour_b)


def train():
    if not TRADES.exists():
        print(f"No trade log at {TRADES}")
        return

    # Load all trades
    trades = []
    with open(TRADES) as f:
        for line in f:
            try:
                trades.append(json.loads(line))
            except Exception:
                continue
    print(f"Loaded {len(trades):,} trades from {TRADES}")

    # Aggregate by bucket
    stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
    for t in trades:
        key = feature_key(t)
        if t.get("pnl", 0) > 0:
            stats[key]["wins"] += 1
        else:
            stats[key]["losses"] += 1
        stats[key]["pnl"] += t.get("pnl", 0)

    # Filter buckets with enough samples
    buckets = {}
    for key, v in stats.items():
        n = v["wins"] + v["losses"]
        if n >= MIN_SAMPLES:
            wr = v["wins"] / n
            avg_pnl = v["pnl"] / n
            buckets["|".join(key)] = {
                "wr": round(wr, 3),
                "n": n,
                "avg_pnl": round(avg_pnl, 3),
                "tradeable": wr >= MIN_WR_TRADE,
            }

    # Aggregate statistics
    tradeable_buckets = {k: v for k, v in buckets.items() if v["tradeable"]}

    model = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "total_trades": len(trades),
        "n_buckets": len(buckets),
        "n_tradeable": len(tradeable_buckets),
        "min_samples": MIN_SAMPLES,
        "min_wr_trade": MIN_WR_TRADE,
        "buckets": buckets,
        "feature_order": ["family", "fee_status", "side", "price_bucket", "hour_bucket"],
    }

    os.makedirs(DATA, exist_ok=True)
    with open(MODEL, "w") as f:
        json.dump(model, f, indent=2)

    print(f"\n=== META MODEL ===")
    print(f"  Buckets: {len(buckets)}")
    print(f"  Tradeable (WR >= {MIN_WR_TRADE}): {len(tradeable_buckets)}")
    print(f"  Saved to {MODEL}")

    # Top 15 tradeable by WR
    sorted_good = sorted(tradeable_buckets.items(), key=lambda kv: -kv[1]["wr"])
    print(f"\n  Top 15 tradeable buckets:")
    for k, v in sorted_good[:15]:
        print(f"    {k:<45}  WR={v['wr']*100:>4.1f}%  n={v['n']:>4}  avg_pnl=${v['avg_pnl']:+.3f}")

    # Worst buckets (should be blocked)
    all_sorted = sorted(buckets.items(), key=lambda kv: kv[1]["wr"])
    print(f"\n  Worst 10 buckets (meta will BLOCK these):")
    for k, v in all_sorted[:10]:
        print(f"    {k:<45}  WR={v['wr']*100:>4.1f}%  n={v['n']:>4}  avg_pnl=${v['avg_pnl']:+.3f}")


if __name__ == "__main__":
    train()
