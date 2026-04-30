"""Pure-Python meta-labeler lookup.

Used by multi_strategy.py to gate new entries. No external dependencies.
Loads bot-data/meta_model.json (from meta_trainer.py) and returns P(win).
"""
import json, os, time
from datetime import datetime, timezone
from pathlib import Path


MODEL_PATH = "data/meta_model.json"   # relative to docker working dir
_MODEL_CACHE = {"data": None, "loaded_at": 0}
_RELOAD_INTERVAL = 600  # reload model every 10 min


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
    return ind


def load_model():
    """Load & cache meta model. Returns None if missing/empty."""
    now = time.time()
    if _MODEL_CACHE["data"] is not None and now - _MODEL_CACHE["loaded_at"] < _RELOAD_INTERVAL:
        return _MODEL_CACHE["data"]

    # Try multiple paths
    for path in [MODEL_PATH, "/app/data/meta_model.json", "bot-data/meta_model.json"]:
        if os.path.exists(path):
            try:
                with open(path) as f:
                    _MODEL_CACHE["data"] = json.load(f)
                    _MODEL_CACHE["loaded_at"] = now
                    return _MODEL_CACHE["data"]
            except Exception:
                continue
    return None


def meta_predict(indicator, side, entry_price, has_fee, timestamp=None):
    """Returns (wr, n_samples) or (None, 0) if bucket not in model.

    Args:
        indicator: strategy's indicator string
        side: "YES" or "NO"
        entry_price: the price we'd pay (0-1)
        has_fee: bool — is market fee-enabled?
        timestamp: unix ts; defaults to now

    Returns:
        (predicted_wr, n_samples) or (None, 0) if bucket is unseen
    """
    model = load_model()
    if model is None:
        return None, 0

    if timestamp is None:
        timestamp = time.time()
    try:
        hour = datetime.fromtimestamp(timestamp, timezone.utc).hour
    except (OSError, ValueError):
        hour = 12

    key_parts = [
        indicator_family(indicator),
        "fee" if has_fee else "free",
        side,
        bucket_price(entry_price),
        bucket_hour(hour),
    ]
    key = "|".join(key_parts)

    buckets = model.get("buckets", {})
    b = buckets.get(key)
    if b is None:
        return None, 0
    return b.get("wr", 0.5), b.get("n", 0)


def should_trade(indicator, side, entry_price, has_fee, min_wr=0.55, require_bucket=False):
    """Meta gate decision.

    Args:
        min_wr: require predicted WR >= this to allow trade
        require_bucket: if True, block when bucket is unseen (no data).
                        if False, allow trade when bucket is unseen.

    Returns:
        (allow: bool, reason: str, info: dict)
    """
    wr, n = meta_predict(indicator, side, entry_price, has_fee)
    if wr is None:
        if require_bucket:
            return False, "no_bucket", {"wr": None, "n": 0}
        return True, "no_bucket_allow", {"wr": None, "n": 0}

    if wr < min_wr:
        return False, f"low_wr_{wr:.2f}", {"wr": wr, "n": n}
    return True, f"ok_wr_{wr:.2f}", {"wr": wr, "n": n}


if __name__ == "__main__":
    # Quick self-test
    model = load_model()
    if model:
        print(f"Model loaded: {model['total_trades']} trades, "
              f"{model['n_buckets']} buckets, "
              f"{model['n_tradeable']} tradeable")
        # Test predictions
        tests = [
            ("forest_25", "NO", 0.35, False, "should be good — forest/free/NO/mid_low"),
            ("forest_25", "YES", 0.35, False, "should be bad — forest/free/YES/mid_low"),
            ("wavelet_mr", "NO", 0.35, False, "should be great — wavelet/free/NO/mid_low"),
        ]
        for ind, side, price, fee, desc in tests:
            allow, reason, info = should_trade(ind, side, price, fee)
            print(f"  {'ALLOW' if allow else 'BLOCK'} {ind}/{side}/p={price} "
                  f"wr={info['wr']} n={info['n']} — {desc}")
    else:
        print("No model found")
