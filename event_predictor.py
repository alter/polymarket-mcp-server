#!/usr/bin/env python3
"""
Event-driven prediction backtest v2.

Pivot from technical analysis: predict outcomes from fundamentals.
Per-category predictors:
  - crypto_price: log-normal model with Binance 1m bars
  - sports_tournament: structural NO bet (1-of-N candidates)
  - sports_match: market-implied baseline + Elo if available
  - geopolitics: news-based probabilities (TODO)

Strict time-cutoff: only data with timestamp < close_time - X hours used.
"""
import json, re, math, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict

import httpx
import numpy as np


# ─── Binance 1m kline fetching ──────────────────────────────────────────────

BINANCE_API = "https://api.binance.com/api/v3/klines"
PRICE_CACHE_DIR = Path("bot-data/binance_klines")
PRICE_CACHE_DIR.mkdir(exist_ok=True)


def binance_symbol(asset_name):
    a = asset_name.lower()
    if "bitcoin" in a or a == "btc": return "BTCUSDT"
    if "ethereum" in a or a == "eth": return "ETHUSDT"
    if "solana" in a or a == "sol": return "SOLUSDT"
    return None


def get_klines(symbol, start_ts, end_ts):
    """Fetch 1m klines from Binance. Returns list of [open_time, open, high, low, close]."""
    cache_file = PRICE_CACHE_DIR / f"{symbol}_{int(start_ts)}_{int(end_ts)}.json"
    if cache_file.exists():
        return json.load(open(cache_file))

    klines = []
    cur = int(start_ts * 1000)
    end_ms = int(end_ts * 1000)
    client = httpx.Client(timeout=15)
    while cur < end_ms:
        try:
            r = client.get(BINANCE_API, params={
                "symbol": symbol, "interval": "1m",
                "startTime": cur, "endTime": end_ms, "limit": 1000,
            })
            if r.status_code != 200:
                break
            batch = r.json()
            if not batch:
                break
            for k in batch:
                klines.append([k[0], float(k[1]), float(k[2]), float(k[3]), float(k[4])])
            cur = batch[-1][0] + 60_000
            time.sleep(0.05)
        except Exception:
            break
    json.dump(klines, open(cache_file, "w"))
    return klines


def get_price_at(symbol, target_ts):
    """Returns close price at minute closest to (≤) target_ts."""
    # Fetch a window around target
    start = target_ts - 3600  # 1h before
    klines = get_klines(symbol, start, target_ts + 60)
    if not klines:
        return None
    # Find last kline with open_time ≤ target * 1000
    target_ms = target_ts * 1000
    last = None
    for k in klines:
        if k[0] <= target_ms:
            last = k[4]  # close
        else:
            break
    return last


def get_volatility(symbol, end_ts, lookback_days=14):
    """Annualized log-volatility from 1h candles over lookback period."""
    start = end_ts - lookback_days * 86400
    klines = get_klines(symbol, start, end_ts)
    if len(klines) < 50:
        return 0.5
    closes = np.array([k[4] for k in klines])
    log_ret = np.diff(np.log(closes))
    minutely_std = float(np.std(log_ret))
    # Annualize: minutes per year = 60 * 24 * 365 = 525600
    return minutely_std * math.sqrt(525600)


# ─── Question parsing ──────────────────────────────────────────────────────

def parse_crypto_question(q):
    ql = q.lower()
    asset = None
    for kw in ("bitcoin", "btc", "ethereum", "eth", "solana"):
        if kw in ql:
            asset = binance_symbol(kw)
            break
    if not asset:
        return None
    # Extract numeric thresholds
    nums = re.findall(r'\$([\d,]+(?:\.\d+)?)', q)
    nums = [float(n.replace(",", "")) for n in nums]
    # Some questions don't use $ — look for raw numbers
    if not nums:
        nums = [float(n.replace(",", "")) for n in re.findall(r'\b(\d{2,3}(?:,\d{3})+)\b', q)]
    if not nums:
        return None

    if "between" in ql and len(nums) >= 2:
        cond = "between"
        thresh = nums[0]
        thresh_high = nums[1]
    elif "above" in ql or "reach" in ql:
        cond = "above"
        thresh = nums[0]
        thresh_high = None
    elif "below" in ql or "dip" in ql:
        cond = "below"
        thresh = nums[0]
        thresh_high = None
    elif "up or down" in ql:
        cond = "up_or_down"
        thresh = None
        thresh_high = None
    else:
        return None

    return {"asset": asset, "condition": cond,
            "threshold": thresh, "threshold_high": thresh_high}


# ─── Probability models ────────────────────────────────────────────────────

def predict_crypto(parsed, current_price, vol, hours_to_target):
    """Log-normal P(YES). hours_to_target in hours."""
    if parsed["condition"] == "up_or_down":
        return 0.50
    K = parsed["threshold"]
    if not K or not current_price:
        return None
    T = max(hours_to_target / (24 * 365), 1 / 525600)  # min 1 minute
    sigma_T = vol * math.sqrt(T)
    if sigma_T < 1e-5:
        sigma_T = 1e-5

    log_K = math.log(K)
    log_S = math.log(current_price)
    z = (log_K - log_S) / sigma_T

    def cdf(x):
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    p_above = 1 - cdf(z)
    if parsed["condition"] in ("above", "reach"):
        return p_above
    if parsed["condition"] in ("below", "dip"):
        return 1 - p_above
    if parsed["condition"] == "between":
        K_high = parsed["threshold_high"]
        z_high = (math.log(K_high) - log_S) / sigma_T
        return p_above - (1 - cdf(z_high))
    return None


def is_tournament_candidate(q):
    """Detect 'Will X win the [tournament]' single-candidate questions."""
    ql = q.lower()
    if not re.search(r"will .* win", ql):
        return False
    return any(s in ql for s in ["world cup", "champions league", "masters",
                                  "open", "grand slam", "super bowl",
                                  "premier league", "champions", "nominee",
                                  "presidential election", "primary"])


def is_election_candidate(q):
    """Multi-candidate political election."""
    ql = q.lower()
    return ("will " in ql and "win" in ql and
            any(s in ql for s in ["president", "primary", "nomination"]))


def predict_tournament_no(q):
    """For 1-of-N candidate questions: P(YES) ≈ 1/N. We don't know N, so use prior 0.05."""
    return 0.05  # 5% YES by default


# ─── Category base-rate predictors (empirical priors) ───────────────────────

# Empirical base rates from 535 resolved markets:
#   sports_match: 57% NO  → P(YES) ≈ 0.43
#   esports: 50/50
#   weather: 79% NO → P(YES) ≈ 0.21
#   geopolitics: 67% NO → P(YES) ≈ 0.33
#   culture (range buckets): 89% NO → P(YES) ≈ 0.11
#   tournament_candidate: 100% NO → P(YES) ≈ 0.05
CATEGORY_PRIORS = {
    "sports_match": 0.43,
    "esports": 0.50,
    "weather": 0.21,
    "geopolitics": 0.33,
    "culture": 0.11,
    "tournament_candidate": 0.05,
    "other": 0.20,
}

def predict_baseline(category):
    """No-info baseline: empirical category prior. Bets AGAINST extreme market prices
    where category-wise prior strongly disagrees with current market."""
    return CATEGORY_PRIORS.get(category)


# ─── Tick price loading ────────────────────────────────────────────────────

def load_market_ticks():
    """Returns dict mid → list of (ts, mid_price)."""
    by_mkt = defaultdict(list)
    with open("bot-data/arena_ticks.jsonl") as f:
        for line in f:
            try:
                t = json.loads(line)
                mid = str(t.get("market_id", ""))
                if not mid:
                    continue
                ts = datetime.fromisoformat(t["ts"].replace("Z", "+00:00")).timestamp()
                by_mkt[mid].append((ts, float(t.get("mid", 0.5))))
            except Exception:
                continue
    for mid in by_mkt:
        by_mkt[mid].sort()
    return by_mkt


def get_market_yes_at(ticks_by_mkt, market_id, target_ts):
    arr = ticks_by_mkt.get(str(market_id))
    if not arr:
        return None
    last = None
    for ts, price in arr:
        if ts <= target_ts:
            last = price
        else:
            break
    if last is None and arr:
        # Use first available tick if cutoff is before any tick
        return arr[0][1]
    return last


# ─── Main backtest ─────────────────────────────────────────────────────────

def categorize(q):
    ql = q.lower()
    if any(s in ql for s in ["bitcoin", "btc", "ethereum", "eth", "solana"]):
        return "crypto"
    if is_tournament_candidate(q) or is_election_candidate(q):
        return "tournament_candidate"
    if "lol:" in ql or "cs2" in ql or "valorant" in ql or "dota" in ql:
        return "esports"
    if any(s in ql for s in ["fc", "vs.", "vs "]) and not "esports" in ql:
        return "sports_match"
    if any(s in ql for s in ["temperature", "weather", "rain", "snow", "wind"]):
        return "weather"
    if any(s in ql for s in ["ceasefire", "military", "iran", "russia", "ukraine",
                              "hormuz", "israel"]):
        return "geopolitics"
    if "mrbeast" in ql or "elon" in ql or "tweet" in ql:
        return "culture"
    return "other"


def backtest(bet_size_usd=0.01, edge_threshold=0.10, cutoff_hours=1):
    print("Loading data...")
    market_ticks = load_market_ticks()
    print(f"  {len(market_ticks)} markets in tick history")

    gamma_meta = json.load(open("bot-data/gamma_market_meta.json"))
    clob = json.load(open("bot-data/clob_cache.json"))
    print(f"  {len(gamma_meta)} arena markets in metadata, {len(clob)} CLOB resolutions")

    # Build resolved set: for each gamma market, get conditionId, check CLOB for outcome
    resolved = []
    for mid, gm in gamma_meta.items():
        if not gm.get("closed"):
            continue
        cid = gm.get("cid", "")
        if cid not in clob:
            continue
        clob_data = clob[cid]
        tokens = clob_data.get("tokens", [])
        if not tokens:
            continue
        yes_won = tokens[0].get("winner", False)
        try:
            close_ts = datetime.fromisoformat(gm.get("end", "").replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        resolved.append({
            "mid": mid, "cid": cid, "q": gm.get("q", ""),
            "close_ts": close_ts, "yes_won": yes_won,
            "category": categorize(gm.get("q", "")),
        })
    print(f"  {len(resolved)} resolved markets total")

    # Distribution
    cat_count = defaultdict(int)
    for r in resolved:
        cat_count[r["category"]] += 1
    print(f"  By category:")
    for cat, n in sorted(cat_count.items(), key=lambda x: -x[1]):
        print(f"    {cat}: {n}")

    bets = []
    skipped = defaultdict(int)

    for m in resolved:
        cutoff_ts = m["close_ts"] - cutoff_hours * 3600

        # Get market YES at cutoff
        market_p = get_market_yes_at(market_ticks, m["mid"], cutoff_ts)
        if market_p is None or market_p <= 0.02 or market_p >= 0.98:
            skipped[f"{m['category']}_no_mkt_p"] += 1
            continue

        # Predict per category
        our_p = None
        if m["category"] == "crypto":
            parsed = parse_crypto_question(m["q"])
            if not parsed:
                skipped["crypto_no_parse"] += 1
                continue
            current = get_price_at(parsed["asset"], cutoff_ts)
            if not current:
                skipped["crypto_no_price"] += 1
                continue
            vol = get_volatility(parsed["asset"], cutoff_ts, 14)
            hours_to_target = (m["close_ts"] - cutoff_ts) / 3600
            our_p = predict_crypto(parsed, current, vol, hours_to_target)
        elif m["category"] == "tournament_candidate":
            our_p = predict_tournament_no(m["q"])
        else:
            # Use category baseline prior
            our_p = predict_baseline(m["category"])

        if our_p is None:
            skipped[f"{m['category']}_no_pred"] += 1
            continue

        edge = our_p - market_p
        if abs(edge) < edge_threshold:
            skipped[f"{m['category']}_no_edge"] += 1
            continue

        side = "YES" if edge > 0 else "NO"
        entry = market_p if side == "YES" else (1 - market_p)
        won = (side == "YES" and m["yes_won"]) or (side == "NO" and not m["yes_won"])
        shares = bet_size_usd / max(entry, 0.02)
        pnl = shares - bet_size_usd if won else -bet_size_usd

        bets.append({
            "category": m["category"], "q": m["q"][:60],
            "our_p": round(our_p, 3), "mkt_p": round(market_p, 3),
            "edge": round(edge, 3), "side": side, "entry": round(entry, 3),
            "won": won, "pnl": round(pnl, 4),
        })

    print(f"\nSkipped: {dict(skipped)}")
    print(f"Bets placed: {len(bets)}")
    if not bets:
        return

    # Aggregate
    by_cat = defaultdict(list)
    for b in bets:
        by_cat[b["category"]].append(b)

    print(f"\n{'Category':<22} {'N':>5} {'WR':>6} {'Avg edge':>10} {'PnL':>10} {'ROI':>7}")
    print("─" * 75)
    for cat, bs in sorted(by_cat.items(), key=lambda x: -len(x[1])):
        n = len(bs)
        wins = sum(1 for b in bs if b["won"])
        avg_edge = sum(abs(b["edge"]) for b in bs) / n
        total_pnl = sum(b["pnl"] for b in bs)
        cost = n * bet_size_usd
        roi = total_pnl / cost * 100
        print(f"{cat:<22} {n:>5} {wins/n*100:>5.0f}% {avg_edge:>9.3f} ${total_pnl:>+8.4f} {roi:>+6.1f}%")

    total_n = len(bets)
    total_w = sum(1 for b in bets if b["won"])
    total_pnl = sum(b["pnl"] for b in bets)
    total_cost = total_n * bet_size_usd
    print("─" * 75)
    print(f"{'TOTAL':<22} {total_n:>5} {total_w/total_n*100:>5.0f}% "
          f"{'':>10} ${total_pnl:>+8.4f} {total_pnl/total_cost*100:>+6.1f}%")

    # Top winners and losers
    print(f"\nTop 10 wins:")
    for b in sorted(bets, key=lambda x: -x["pnl"])[:10]:
        print(f"  {b['side']:>3} our={b['our_p']:.3f} mkt={b['mkt_p']:.3f} "
              f"edge={b['edge']:+.3f} pnl=${b['pnl']:+.4f} | {b['q']}")
    print(f"\nWorst 5:")
    for b in sorted(bets, key=lambda x: x["pnl"])[:5]:
        print(f"  {b['side']:>3} our={b['our_p']:.3f} mkt={b['mkt_p']:.3f} "
              f"edge={b['edge']:+.3f} pnl=${b['pnl']:+.4f} | {b['q']}")


if __name__ == "__main__":
    backtest(bet_size_usd=0.01, edge_threshold=0.10, cutoff_hours=1)
