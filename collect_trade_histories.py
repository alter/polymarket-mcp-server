#!/usr/bin/env python3
"""
Collect detailed trade histories from Polymarket for the top 200 active markets
by 24h volume, build hourly VWAP price series, and analyze patterns:
  1. Mean reversion after 5%+ moves
  2. Momentum after 3 consecutive up-hours
  3. Whale impact (trades > $500)
  4. Time-of-day volume/movement patterns
  5. Spread vs subsequent move size
"""

import json
import time
import os
from collections import defaultdict
from datetime import datetime, timezone

import httpx

GAMMA_URL = "https://gamma-api.polymarket.com"
DATA_URL = "https://data-api.polymarket.com"
OUTPUT_PATH = "bot-data/trade_flow_analysis.json"

client = httpx.Client(timeout=20)


# ── 1. Fetch top 200 markets by 24h volume ──────────────────────────────────

def fetch_top_markets(n=200):
    markets = []
    per_page = 100
    offset = 0
    while len(markets) < n:
        resp = client.get(
            f"{GAMMA_URL}/markets",
            params={
                "active": "true",
                "closed": "false",
                "limit": per_page,
                "offset": offset,
                "order": "volume24hr",
                "ascending": "false",
            },
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        markets.extend(batch)
        offset += per_page
        time.sleep(0.2)
    markets = markets[:n]
    print(f"[markets] fetched {len(markets)} markets")
    return markets


# ── 2. Fetch trades for a single market ──────────────────────────────────────

def fetch_trades(condition_id):
    """Return list of trade dicts for a market condition_id."""
    resp = client.get(
        f"{DATA_URL}/trades",
        params={"market": condition_id, "limit": 500},
    )
    resp.raise_for_status()
    return resp.json()


# ── 3. Build hourly VWAP buckets ─────────────────────────────────────────────

def build_hourly_vwap(trades):
    """
    Group trades into 1-hour UTC buckets.
    Returns sorted list of dicts: {hour_ts, vwap, volume, trade_count}.
    """
    buckets = defaultdict(lambda: {"pv": 0.0, "vol": 0.0, "count": 0})
    for t in trades:
        ts = int(t.get("timestamp") or t.get("createdAt") or 0)
        if ts == 0:
            continue
        price = float(t.get("price", 0))
        size = float(t.get("size", 0))
        if price <= 0 or size <= 0:
            continue
        hour_ts = (ts // 3600) * 3600
        b = buckets[hour_ts]
        b["pv"] += price * size
        b["vol"] += size
        b["count"] += 1

    series = []
    for hour_ts in sorted(buckets):
        b = buckets[hour_ts]
        series.append(
            {
                "hour_ts": hour_ts,
                "vwap": b["pv"] / b["vol"] if b["vol"] > 0 else 0,
                "volume": b["vol"],
                "trade_count": b["count"],
            }
        )
    return series


# ── 4. Analysis helpers ──────────────────────────────────────────────────────

def analyze_mean_reversion(series):
    """After a 5%+ move in 1 hour, does price revert in next 2 hours?"""
    revert = 0
    cont = 0
    for i in range(len(series) - 3):
        p0 = series[i]["vwap"]
        p1 = series[i + 1]["vwap"]
        if p0 == 0:
            continue
        move = (p1 - p0) / p0
        if abs(move) < 0.05:
            continue
        # check next 2 hours
        p3 = series[i + 3]["vwap"]
        subsequent = (p3 - p1) / p1 if p1 != 0 else 0
        if move > 0 and subsequent < 0:
            revert += 1
        elif move < 0 and subsequent > 0:
            revert += 1
        else:
            cont += 1
    return {"revert": revert, "continue": cont, "total": revert + cont}


def analyze_momentum(series):
    """After 3 consecutive up-hours, does price continue up?"""
    cont_up = 0
    reverses = 0
    for i in range(3, len(series)):
        up_streak = all(
            series[j]["vwap"] > series[j - 1]["vwap"] for j in range(i - 2, i + 1)
        )
        if not up_streak:
            continue
        if i + 1 >= len(series):
            continue
        if series[i + 1]["vwap"] > series[i]["vwap"]:
            cont_up += 1
        else:
            reverses += 1
    return {"continue_up": cont_up, "reverse": reverses, "total": cont_up + reverses}


def analyze_whale_impact(trades, series):
    """Trades > $500 — does price move in trade direction next hour?"""
    if not series:
        return {"same_dir": 0, "opp_dir": 0, "total": 0}

    # build hour->vwap lookup
    hour_vwap = {s["hour_ts"]: s["vwap"] for s in series}

    same_dir = 0
    opp_dir = 0
    for t in trades:
        price = float(t.get("price", 0))
        size = float(t.get("size", 0))
        value = price * size
        if value < 500:
            continue
        ts = int(t.get("timestamp") or t.get("createdAt") or 0)
        if ts == 0:
            continue
        side = (t.get("side") or "").upper()
        if side not in ("BUY", "SELL"):
            continue
        hour_ts = (ts // 3600) * 3600
        next_hour = hour_ts + 3600
        if hour_ts not in hour_vwap or next_hour not in hour_vwap:
            continue
        move = hour_vwap[next_hour] - hour_vwap[hour_ts]
        if side == "BUY" and move > 0:
            same_dir += 1
        elif side == "SELL" and move < 0:
            same_dir += 1
        else:
            opp_dir += 1
    return {"same_dir": same_dir, "opp_dir": opp_dir, "total": same_dir + opp_dir}


def analyze_time_of_day(trades, series):
    """Volume and price movement per UTC hour."""
    hour_volume = defaultdict(float)
    hour_trade_count = defaultdict(int)
    hour_abs_move = defaultdict(list)

    for t in trades:
        ts = int(t.get("timestamp") or t.get("createdAt") or 0)
        if ts == 0:
            continue
        price = float(t.get("price", 0))
        size = float(t.get("size", 0))
        utc_hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour
        hour_volume[utc_hour] += price * size
        hour_trade_count[utc_hour] += 1

    for s in series:
        utc_hour = datetime.fromtimestamp(s["hour_ts"], tz=timezone.utc).hour
        hour_abs_move[utc_hour].append(s["vwap"])

    # compute avg abs hourly returns per hour-of-day
    hour_avg_move = {}
    for h, prices in hour_abs_move.items():
        if len(prices) < 2:
            hour_avg_move[h] = 0
        else:
            returns = [
                abs(prices[i] - prices[i - 1]) / prices[i - 1]
                for i in range(1, len(prices))
                if prices[i - 1] > 0
            ]
            hour_avg_move[h] = sum(returns) / len(returns) if returns else 0

    result = {}
    for h in range(24):
        result[str(h)] = {
            "volume_usd": round(hour_volume.get(h, 0), 2),
            "trade_count": hour_trade_count.get(h, 0),
            "avg_abs_return": round(hour_avg_move.get(h, 0), 6),
        }
    return result


def analyze_spread_vs_move(markets_data):
    """
    Markets with wider spreads -> larger subsequent moves?
    markets_data: list of {spread, avg_hourly_abs_return}
    """
    if not markets_data:
        return {"narrow_spread_avg_move": 0, "wide_spread_avg_move": 0}
    spreads = [m["spread"] for m in markets_data if m["spread"] > 0]
    if not spreads:
        return {"narrow_spread_avg_move": 0, "wide_spread_avg_move": 0}
    median_spread = sorted(spreads)[len(spreads) // 2]

    narrow = [m["avg_move"] for m in markets_data if 0 < m["spread"] <= median_spread]
    wide = [m["avg_move"] for m in markets_data if m["spread"] > median_spread]

    return {
        "median_spread": round(median_spread, 4),
        "narrow_spread_avg_move": round(sum(narrow) / len(narrow), 6) if narrow else 0,
        "wide_spread_avg_move": round(sum(wide) / len(wide), 6) if wide else 0,
        "narrow_count": len(narrow),
        "wide_count": len(wide),
    }


# ── 5. Main collection loop ─────────────────────────────────────────────────

def main():
    start = time.time()

    markets = fetch_top_markets(200)

    # accumulators for cross-market analysis
    all_mean_reversion = {"revert": 0, "continue": 0, "total": 0}
    all_momentum = {"continue_up": 0, "reverse": 0, "total": 0}
    all_whale = {"same_dir": 0, "opp_dir": 0, "total": 0}
    all_time_of_day = defaultdict(lambda: {"volume_usd": 0, "trade_count": 0, "moves": []})
    spread_move_data = []
    per_market = []
    total_trades = 0
    markets_with_trades = 0

    for idx, mkt in enumerate(markets):
        cid = mkt.get("conditionId") or mkt.get("condition_id") or mkt.get("id", "")
        question = (mkt.get("question") or "")[:80]
        vol24 = float(mkt.get("volume24hr") or 0)

        if not cid:
            continue

        try:
            trades = fetch_trades(cid)
        except Exception as e:
            print(f"  [{idx+1}/{len(markets)}] SKIP {question[:40]}... error={e}")
            time.sleep(0.2)
            continue

        time.sleep(0.2)

        if not trades:
            if (idx + 1) % 20 == 0:
                print(f"  [{idx+1}/{len(markets)}] {question[:50]}... 0 trades")
            continue

        total_trades += len(trades)
        markets_with_trades += 1
        series = build_hourly_vwap(trades)

        # per-market analyses
        mr = analyze_mean_reversion(series)
        mo = analyze_momentum(series)
        wh = analyze_whale_impact(trades, series)
        tod = analyze_time_of_day(trades, series)

        # accumulate
        for k in ("revert", "continue", "total"):
            all_mean_reversion[k] += mr[k]
        for k in ("continue_up", "reverse", "total"):
            all_momentum[k] += mo[k]
        for k in ("same_dir", "opp_dir", "total"):
            all_whale[k] += wh[k]
        for h in range(24):
            hs = str(h)
            all_time_of_day[hs]["volume_usd"] += tod[hs]["volume_usd"]
            all_time_of_day[hs]["trade_count"] += tod[hs]["trade_count"]
            all_time_of_day[hs]["moves"].append(tod[hs]["avg_abs_return"])

        # spread data
        tokens = mkt.get("tokens", [])
        spread = 0
        if mkt.get("outcomePrices"):
            try:
                prices = json.loads(mkt["outcomePrices"])
                if len(prices) >= 1:
                    # Approximate spread from price level (markets near 0.5 tend to be tighter)
                    # Real spread would need orderbook; use a proxy
                    spread = float(mkt.get("spread", 0) or 0)
            except (json.JSONDecodeError, TypeError):
                pass

        # compute average hourly abs return for this market
        if len(series) >= 2:
            returns = []
            for i in range(1, len(series)):
                if series[i - 1]["vwap"] > 0:
                    returns.append(
                        abs(series[i]["vwap"] - series[i - 1]["vwap"])
                        / series[i - 1]["vwap"]
                    )
            avg_move = sum(returns) / len(returns) if returns else 0
        else:
            avg_move = 0

        spread_move_data.append({"spread": spread, "avg_move": avg_move})

        per_market.append(
            {
                "condition_id": cid,
                "question": question,
                "volume_24h": vol24,
                "trade_count": len(trades),
                "hourly_buckets": len(series),
                "mean_reversion": mr,
                "momentum": mo,
                "whale_impact": wh,
            }
        )

        if (idx + 1) % 10 == 0:
            elapsed = time.time() - start
            print(
                f"  [{idx+1}/{len(markets)}] {markets_with_trades} with trades, "
                f"{total_trades} total trades, {elapsed:.0f}s"
            )

    # ── aggregate time-of-day ────────────────────────────────────────────
    agg_tod = {}
    for h in range(24):
        hs = str(h)
        d = all_time_of_day[hs]
        moves = [m for m in d["moves"] if m > 0]
        agg_tod[hs] = {
            "volume_usd": round(d["volume_usd"], 2),
            "trade_count": d["trade_count"],
            "avg_abs_return": round(sum(moves) / len(moves), 6) if moves else 0,
        }

    # ── spread analysis ──────────────────────────────────────────────────
    spread_analysis = analyze_spread_vs_move(spread_move_data)

    # ── build result ─────────────────────────────────────────────────────
    elapsed = time.time() - start
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "markets_fetched": len(markets),
        "markets_with_trades": markets_with_trades,
        "total_trades": total_trades,
        "aggregate": {
            "mean_reversion": all_mean_reversion,
            "momentum": all_momentum,
            "whale_impact": all_whale,
            "time_of_day": agg_tod,
            "spread_vs_move": spread_analysis,
        },
        "per_market": per_market,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Done in {elapsed:.0f}s")
    print(f"Markets: {len(markets)} fetched, {markets_with_trades} with trades")
    print(f"Total trades: {total_trades}")
    print(f"Mean reversion: {all_mean_reversion}")
    print(f"Momentum: {all_momentum}")
    print(f"Whale impact: {all_whale}")
    print(f"Spread analysis: {spread_analysis}")
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
