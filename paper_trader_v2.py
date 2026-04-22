#!/usr/bin/env python3
"""
Paper Trading Bot v2: Mean Reversion + Whale Fade
Starts with $1000. Scans top 200 active markets for:
1. Mean reversion: hourly VWAP moved 5%+ → bet on reversion
2. Whale fade: trade >$500 → bet against whale direction

Run: python3 paper_trader_v2.py
"""
import json, time, os
from collections import defaultdict
from datetime import datetime, timezone
import httpx

GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com"
PF_PATH = "bot-data/paper_portfolio_v2.json"
TRADE_SIZE = 20.0
MIN_PRICE = 0.10
MAX_PRICE = 0.90
INITIAL_CAPITAL = 1000.0

client = httpx.Client(timeout=20)


def load_portfolio():
    if os.path.exists(PF_PATH):
        return json.load(open(PF_PATH))
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "initial_capital": INITIAL_CAPITAL,
        "cash": INITIAL_CAPITAL,
        "positions": [],
        "closed_trades": [],
        "total_pnl": 0.0,
    }


def save_portfolio(pf):
    os.makedirs(os.path.dirname(PF_PATH), exist_ok=True)
    with open(PF_PATH, "w") as f:
        json.dump(pf, f, indent=2)


def fetch_markets(n=200):
    markets = []
    for offset in range(0, n, 100):
        r = client.get(f"{GAMMA}/markets", params={
            "active": "true", "closed": "false", "limit": 100,
            "offset": offset, "order": "volume24hr", "ascending": "false"})
        r.raise_for_status()
        b = r.json()
        if not b: break
        markets.extend(b)
        time.sleep(0.2)
    return markets[:n]


def fetch_trades(cid, limit=500):
    r = client.get(f"{DATA}/trades", params={"market": cid, "limit": limit})
    r.raise_for_status()
    return r.json()


def build_hourly_vwap(trades):
    buckets = defaultdict(lambda: {"pv": 0.0, "vol": 0.0, "cnt": 0, "vol_usd": 0.0})
    for t in trades:
        ts = int(t.get("timestamp") or 0)
        price = float(t.get("price", 0))
        size = float(t.get("size", 0))
        if ts == 0 or price <= 0 or size <= 0: continue
        hour = (ts // 3600) * 3600
        b = buckets[hour]
        b["pv"] += price * size
        b["vol"] += size
        b["vol_usd"] += price * size
        b["cnt"] += 1
    series = []
    for h in sorted(buckets):
        b = buckets[h]
        series.append({
            "ts": h, "vwap": b["pv"]/b["vol"] if b["vol"] > 0 else 0,
            "vol_usd": b["vol_usd"], "cnt": b["cnt"]
        })
    return series


def get_current_price(mkt):
    """Get YES price from market data."""
    op = mkt.get("outcomePrices", "")
    if isinstance(op, str):
        try: op = json.loads(op)
        except: return None
    if op and len(op) >= 1:
        try: return float(op[0])
        except: pass
    return None


def scan_mean_reversion(markets):
    """Find markets with 5%+ moves in recent hours."""
    signals = []
    for mkt in markets:
        cid = mkt.get("conditionId") or ""
        if not cid: continue
        question = mkt.get("question", "")[:80]

        try:
            trades = fetch_trades(cid)
        except:
            time.sleep(0.2)
            continue
        time.sleep(0.15)

        if not trades: continue
        series = build_hourly_vwap(trades)
        if len(series) < 3: continue

        # Check last 2 hourly moves
        for i in range(max(0, len(series)-3), len(series)-1):
            s0 = series[i]
            s1 = series[i+1]
            if s0["vwap"] <= 0 or s1["vwap"] <= 0: continue
            if s0["vol_usd"] < 200 or s1["vol_usd"] < 200: continue

            move = (s1["vwap"] - s0["vwap"]) / s0["vwap"]
            if abs(move) < 0.05: continue

            current_yes = s1["vwap"]
            if not (MIN_PRICE <= current_yes <= MAX_PRICE): continue

            if move > 0:
                # Price UP → buy NO (expect reversion)
                side = "NO"
                entry = 1.0 - current_yes
            else:
                # Price DOWN → buy YES (expect reversion)
                side = "YES"
                entry = current_yes

            if not (MIN_PRICE <= entry <= MAX_PRICE): continue

            signals.append({
                "strategy": "mean_reversion",
                "cid": cid, "question": question,
                "side": side, "entry": round(entry, 4),
                "current_yes": round(current_yes, 4),
                "move_pct": round(move * 100, 2),
                "hour_vol": round(s1["vol_usd"], 0),
                "expected_exit_ts": s1["ts"] + 7200,
                "signal_ts": s1["ts"],
            })
    return signals


def scan_whale_trades(markets):
    """Find recent whale trades (>$500) to fade."""
    signals = []
    now = int(time.time())
    cutoff = now - 7200  # last 2 hours

    for mkt in markets:
        cid = mkt.get("conditionId") or ""
        if not cid: continue
        question = mkt.get("question", "")[:80]

        try:
            trades = fetch_trades(cid, limit=200)
        except:
            time.sleep(0.2)
            continue
        time.sleep(0.15)

        if not trades: continue

        current_yes = get_current_price(mkt)
        if current_yes is None or not (MIN_PRICE <= current_yes <= MAX_PRICE):
            continue

        for t in trades:
            ts = int(t.get("timestamp") or 0)
            if ts < cutoff: continue  # only recent trades

            price = float(t.get("price", 0))
            size = float(t.get("size", 0))
            value = price * size
            if value < 500: continue

            whale_side = (t.get("side") or "").upper()
            if whale_side not in ("BUY", "SELL"): continue

            if whale_side == "BUY":
                # Whale bought YES → we buy NO
                side = "NO"
                entry = 1.0 - current_yes
            else:
                # Whale sold YES → we buy YES
                side = "YES"
                entry = current_yes

            if not (MIN_PRICE <= entry <= MAX_PRICE): continue

            signals.append({
                "strategy": "whale_fade",
                "cid": cid, "question": question,
                "side": side, "entry": round(entry, 4),
                "current_yes": round(current_yes, 4),
                "whale_side": whale_side,
                "whale_value": round(value, 0),
                "whale_ts": ts,
                "expected_exit_ts": ts + 3600,
            })

    # Deduplicate: one signal per market per strategy
    seen = set()
    unique = []
    for s in signals:
        key = (s["cid"], s["strategy"])
        if key not in seen:
            seen.add(key)
            unique.append(s)
    return unique


def execute_trades(pf, signals, max_trades=30):
    """Execute paper trades from signals."""
    new_trades = 0
    # Don't trade same market twice
    existing_cids = {p["cid"] for p in pf["positions"]}

    for sig in signals[:max_trades]:
        if pf["cash"] < TRADE_SIZE:
            print("  No more cash!")
            break
        if sig["cid"] in existing_cids:
            continue

        entry = sig["entry"]
        shares = TRADE_SIZE / entry
        spread_cost = TRADE_SIZE * 0.01  # 1% spread estimate

        position = {
            "strategy": sig["strategy"],
            "cid": sig["cid"],
            "question": sig["question"],
            "side": sig["side"],
            "entry": entry,
            "shares": round(shares, 4),
            "cost": TRADE_SIZE,
            "spread_est": round(spread_cost, 2),
            "current_yes": sig["current_yes"],
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "signal_data": {k: v for k, v in sig.items() if k not in ("cid", "question", "side", "entry", "current_yes")},
        }

        pf["positions"].append(position)
        pf["cash"] -= TRADE_SIZE
        existing_cids.add(sig["cid"])
        new_trades += 1

    return new_trades


def update_positions(pf, markets):
    """Mark-to-market all positions."""
    # Build price lookup
    price_lookup = {}
    for mkt in markets:
        cid = mkt.get("conditionId") or ""
        yp = get_current_price(mkt)
        if cid and yp is not None:
            price_lookup[cid] = yp

    total_value = pf["cash"]
    for pos in pf["positions"]:
        yp = price_lookup.get(pos["cid"])
        if yp is not None:
            if pos["side"] == "NO":
                current_price = 1.0 - yp
            else:
                current_price = yp
            pos["current_price"] = round(current_price, 4)
            pos["current_value"] = round(pos["shares"] * current_price, 2)
            pos["unrealized_pnl"] = round(pos["current_value"] - pos["cost"], 2)
            pos["pnl_pct"] = round(pos["unrealized_pnl"] / pos["cost"] * 100, 2)
            total_value += pos["current_value"]
        else:
            pos["current_price"] = pos["entry"]
            pos["current_value"] = pos["cost"]
            pos["unrealized_pnl"] = 0
            pos["pnl_pct"] = 0
            total_value += pos["cost"]

    pf["total_value"] = round(total_value, 2)
    pf["total_pnl"] = round(total_value - INITIAL_CAPITAL, 2)
    pf["total_roi"] = round((total_value - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 2)
    pf["last_update"] = datetime.now(timezone.utc).isoformat()


def main():
    start = time.time()
    pf = load_portfolio()
    is_new = pf["cash"] == INITIAL_CAPITAL and not pf["positions"]

    print(f"=== Paper Trader v2 {'(NEW PORTFOLIO)' if is_new else '(UPDATING)'} ===")
    print(f"Cash: ${pf['cash']:,.2f}, Positions: {len(pf['positions'])}")

    print("\n[1] Fetching markets...")
    markets = fetch_markets(200)
    print(f"  {len(markets)} markets")

    if pf["cash"] >= TRADE_SIZE:
        print("\n[2] Scanning mean reversion signals...")
        mr_signals = scan_mean_reversion(markets[:100])  # top 100 by volume
        print(f"  Found {len(mr_signals)} mean reversion signals")

        print("\n[3] Scanning whale fade signals...")
        wf_signals = scan_whale_trades(markets[:100])
        print(f"  Found {len(wf_signals)} whale fade signals")

        # Sort by signal quality
        mr_signals.sort(key=lambda s: -abs(s["move_pct"]))
        wf_signals.sort(key=lambda s: -s["whale_value"])

        # Execute: prioritize mean reversion, then whale fade
        all_signals = mr_signals[:15] + wf_signals[:15]
        print(f"\n[4] Executing trades (max 30)...")
        n = execute_trades(pf, all_signals)
        print(f"  Executed {n} new trades")
    else:
        print("\n  No cash for new trades.")

    print("\n[5] Updating positions (mark-to-market)...")
    update_positions(pf, markets)

    save_portfolio(pf)

    # Summary
    mr_pos = [p for p in pf["positions"] if p["strategy"] == "mean_reversion"]
    wf_pos = [p for p in pf["positions"] if p["strategy"] == "whale_fade"]
    other_pos = [p for p in pf["positions"] if p["strategy"] not in ("mean_reversion", "whale_fade")]

    print(f"\n{'='*80}")
    print(f"PORTFOLIO SUMMARY")
    print(f"{'='*80}")
    print(f"  Cash:          ${pf['cash']:>10,.2f}")
    print(f"  Positions:     {len(pf['positions']):>10}")
    print(f"  Total Value:   ${pf.get('total_value', 0):>10,.2f}")
    print(f"  Total PnL:     ${pf.get('total_pnl', 0):>+10,.2f}")
    print(f"  ROI:           {pf.get('total_roi', 0):>+9.2f}%")

    for label, positions in [("Mean Reversion", mr_pos), ("Whale Fade", wf_pos)]:
        if not positions: continue
        invested = sum(p["cost"] for p in positions)
        pnl = sum(p.get("unrealized_pnl", 0) for p in positions)
        winners = sum(1 for p in positions if p.get("unrealized_pnl", 0) > 0)
        print(f"\n  {label}: {len(positions)} positions, ${invested:,.0f} invested")
        print(f"    PnL: ${pnl:+,.2f} ({pnl/invested*100:+.1f}%)")
        print(f"    Winners: {winners}/{len(positions)}")
        # Top 3 and Bottom 3
        sorted_pos = sorted(positions, key=lambda p: p.get("pnl_pct", 0))
        if len(sorted_pos) >= 3:
            print(f"    Worst:")
            for p in sorted_pos[:3]:
                print(f"      {p['pnl_pct']:>+6.1f}% {p['side']:>3} @ {p['entry']:.3f} | {p['question'][:50]}")
            print(f"    Best:")
            for p in sorted_pos[-3:]:
                print(f"      {p['pnl_pct']:>+6.1f}% {p['side']:>3} @ {p['entry']:.3f} | {p['question'][:50]}")

    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.0f}s. Saved to {PF_PATH}")


if __name__ == "__main__":
    main()
