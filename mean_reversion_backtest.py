#!/usr/bin/env python3
"""
Mean reversion backtest with actual dollar P&L.
Uses trade data from top 200 active markets.
Strategy: After a 5%+ hourly VWAP move, bet on reversion within 2 hours.
"""
import json, time
from collections import defaultdict
from datetime import datetime, timezone
import httpx

GAMMA_URL = "https://gamma-api.polymarket.com"
DATA_URL = "https://data-api.polymarket.com"
client = httpx.Client(timeout=20)


def fetch_top_markets(n=100):
    markets = []
    for offset in range(0, n, 100):
        resp = client.get(f"{GAMMA_URL}/markets", params={
            "active": "true", "closed": "false", "limit": 100,
            "offset": offset, "order": "volume24hr", "ascending": "false"})
        resp.raise_for_status()
        batch = resp.json()
        if not batch: break
        markets.extend(batch)
        time.sleep(0.2)
    return markets[:n]


def fetch_trades(cid, limit=500):
    resp = client.get(f"{DATA_URL}/trades", params={"market": cid, "limit": limit})
    resp.raise_for_status()
    return resp.json()


def build_hourly_vwap(trades):
    buckets = defaultdict(lambda: {"pv": 0.0, "vol": 0.0, "cnt": 0})
    for t in trades:
        ts = int(t.get("timestamp") or 0)
        price = float(t.get("price", 0))
        size = float(t.get("size", 0))
        if ts == 0 or price <= 0 or size <= 0: continue
        hour = (ts // 3600) * 3600
        b = buckets[hour]
        b["pv"] += price * size
        b["vol"] += size
        b["cnt"] += 1
    series = []
    for h in sorted(buckets):
        b = buckets[h]
        series.append({"ts": h, "vwap": b["pv"]/b["vol"] if b["vol"]>0 else 0,
                        "vol": b["vol"], "cnt": b["cnt"]})
    return series


def backtest_mean_reversion(series, trade_size=20.0, spread=0.02):
    """
    After 5%+ move from hour[i] to hour[i+1]:
    - Buy opposite direction (if price went up, we buy NO = expect price drop)
    - Entry at hour[i+1] VWAP + spread/2
    - Exit at hour[i+3] VWAP - spread/2 (2 hours later)
    - PnL = (exit - entry) * shares or -(entry) * shares if wrong side

    For prediction markets, we simplify:
    - If price jumped UP 5%+, we expect reversion DOWN → short YES (= buy NO at 1-price)
    - If price dropped DOWN 5%+, we expect reversion UP → buy YES at price
    """
    trades_log = []
    for i in range(len(series) - 3):
        p0 = series[i]["vwap"]
        p1 = series[i+1]["vwap"]
        if p0 <= 0.01 or p0 >= 0.99: continue

        move = (p1 - p0) / p0
        if abs(move) < 0.05: continue

        p3 = series[i+3]["vwap"]

        if move > 0:
            # Price went UP → expect DOWN → BUY NO
            no_entry = 1.0 - p1 + spread/2
            no_exit = 1.0 - p3 - spread/2
            if no_entry <= 0.01 or no_entry >= 0.99: continue
            shares = trade_size / no_entry
            pnl = (no_exit - no_entry) * shares
            entry = no_entry
            exit_price = no_exit
        else:
            # Price went DOWN → expect UP → BUY YES
            entry = p1 + spread/2
            exit_price = p3 - spread/2
            if entry <= 0.01 or entry >= 0.99: continue
            shares = trade_size / entry
            pnl = (exit_price - entry) * shares

        trades_log.append({
            "hour": series[i+1]["ts"],
            "move": round(move*100, 2),
            "entry": round(entry, 4),
            "exit": round(exit_price, 4),
            "pnl": round(pnl, 4),
            "direction": "short" if move > 0 else "long",
        })
    return trades_log


def backtest_anti_momentum(series, trade_size=20.0, spread=0.02):
    """After 3 consecutive up-hours, bet on reversal (sell YES)."""
    trades_log = []
    for i in range(3, len(series)):
        up_streak = all(series[j]["vwap"] > series[j-1]["vwap"] for j in range(i-2, i+1))
        if not up_streak: continue
        if i+1 >= len(series): continue

        # Bet on reversal: BUY NO (since price went up, expect down)
        no_entry = 1.0 - series[i]["vwap"] + spread/2
        no_exit = 1.0 - series[i+1]["vwap"] - spread/2
        if no_entry <= 0.01 or no_entry >= 0.99: continue
        entry = no_entry
        exit_price = no_exit
        shares = trade_size / entry
        pnl = (exit_price - entry) * shares

        trades_log.append({
            "hour": series[i]["ts"],
            "entry": round(entry, 4),
            "exit": round(exit_price, 4),
            "pnl": round(pnl, 4),
        })
    return trades_log


def backtest_whale_fade(trades, series, trade_size=20.0, spread=0.02):
    """After whale trade (>$500), bet against the whale direction next hour."""
    if not series: return []
    hour_vwap = {s["ts"]: s["vwap"] for s in series}

    trades_log = []
    for t in trades:
        price = float(t.get("price", 0))
        size = float(t.get("size", 0))
        if price * size < 500: continue
        ts = int(t.get("timestamp") or 0)
        if ts == 0: continue
        side = (t.get("side") or "").upper()
        if side not in ("BUY", "SELL"): continue

        hour_ts = (ts // 3600) * 3600
        next_hour = hour_ts + 3600
        next2 = hour_ts + 7200
        if hour_ts not in hour_vwap or next_hour not in hour_vwap: continue

        # Fade the whale: if whale buys YES, we buy NO (expect price drop)
        p_now = hour_vwap[next_hour]
        p_later = hour_vwap.get(next2, p_now)
        if side == "BUY":
            # Whale bought YES → we buy NO
            entry = 1.0 - p_now + spread/2
            exit_price = 1.0 - p_later - spread/2
        else:
            # Whale sold YES → we buy YES
            entry = p_now + spread/2
            exit_price = p_later - spread/2

        if entry <= 0.01 or entry >= 0.99: continue
        shares = trade_size / entry
        pnl = (exit_price - entry) * shares

        trades_log.append({
            "hour": next_hour,
            "whale_side": side,
            "whale_size": round(price*size, 0),
            "entry": round(entry, 4),
            "exit": round(exit_price, 4),
            "pnl": round(pnl, 4),
        })
    return trades_log


def main():
    start = time.time()
    print("=== MEAN REVERSION / ANTI-MOMENTUM / WHALE FADE BACKTEST ===\n")

    markets = fetch_top_markets(200)
    print(f"Fetched {len(markets)} markets\n")

    all_mr = []
    all_am = []
    all_wf = []

    for spread_level in [0.0, 0.01, 0.02, 0.03, 0.05]:
        mr_trades = []
        am_trades = []
        wf_trades = []

        for idx, mkt in enumerate(markets):
            cid = mkt.get("conditionId") or mkt.get("condition_id") or ""
            if not cid: continue

            try:
                trades = fetch_trades(cid)
            except:
                time.sleep(0.2)
                continue
            time.sleep(0.15)

            if not trades: continue
            series = build_hourly_vwap(trades)

            mr = backtest_mean_reversion(series, spread=spread_level)
            am = backtest_anti_momentum(series, spread=spread_level)
            wf = backtest_whale_fade(trades, series, spread=spread_level)

            mr_trades.extend(mr)
            am_trades.extend(am)
            wf_trades.extend(wf)

            if (idx+1) % 50 == 0:
                elapsed = time.time() - start
                print(f"  [{idx+1}/{len(markets)}] spread={spread_level:.0%} | "
                      f"MR={len(mr_trades)} AM={len(am_trades)} WF={len(wf_trades)} | {elapsed:.0f}s")

        # Aggregate results
        for name, log in [("mean_reversion", mr_trades), ("anti_momentum", am_trades), ("whale_fade", wf_trades)]:
            if not log: continue
            wins = sum(1 for t in log if t["pnl"] > 0)
            losses = sum(1 for t in log if t["pnl"] <= 0)
            total_pnl = sum(t["pnl"] for t in log)
            avg_win = sum(t["pnl"] for t in log if t["pnl"] > 0) / max(wins, 1)
            avg_loss = sum(t["pnl"] for t in log if t["pnl"] <= 0) / max(losses, 1)
            n = len(log)

            result = {
                "strategy": name, "spread": spread_level,
                "trades": n, "wins": wins, "losses": losses,
                "wr": round(wins/n*100, 1) if n else 0,
                "total_pnl": round(total_pnl, 2),
                "avg_pnl": round(total_pnl/n, 4) if n else 0,
                "avg_win": round(avg_win, 4),
                "avg_loss": round(avg_loss, 4),
                "roi_1000": round(total_pnl/1000*100, 2),
            }

            if name == "mean_reversion": all_mr.append(result)
            elif name == "anti_momentum": all_am.append(result)
            else: all_wf.append(result)

        # Only fetch data once, reuse for other spreads
        if spread_level == 0.0:
            print(f"\n  Data collection complete. Running spread scenarios...\n")
            # Save the raw data and recompute with different spreads
            break  # Actually we need to refetch... let's restructure

    # Better approach: collect once, then simulate different spreads
    print("\nCollecting market data once...")
    market_data = []  # (series, trades) pairs
    for idx, mkt in enumerate(markets):
        cid = mkt.get("conditionId") or mkt.get("condition_id") or ""
        if not cid: continue
        try:
            trades = fetch_trades(cid)
        except:
            time.sleep(0.2)
            continue
        time.sleep(0.15)
        if not trades: continue
        series = build_hourly_vwap(trades)
        market_data.append((series, trades, mkt.get("question", "")[:50]))
        if (idx+1) % 50 == 0:
            print(f"  [{idx+1}/{len(markets)}] collected {len(market_data)} markets, {time.time()-start:.0f}s")

    print(f"\nCollected {len(market_data)} markets. Running backtests...\n")

    all_results = []
    for spread in [0.0, 0.005, 0.01, 0.02, 0.03, 0.05]:
        mr_all, am_all, wf_all = [], [], []
        for series, trades, q in market_data:
            mr_all.extend(backtest_mean_reversion(series, spread=spread))
            am_all.extend(backtest_anti_momentum(series, spread=spread))
            wf_all.extend(backtest_whale_fade(trades, series, spread=spread))

        for name, log in [("mean_reversion", mr_all), ("anti_momentum", am_all), ("whale_fade", wf_all)]:
            if not log: continue
            wins = sum(1 for t in log if t["pnl"] > 0)
            n = len(log)
            total_pnl = sum(t["pnl"] for t in log)
            avg_win = sum(t["pnl"] for t in log if t["pnl"] > 0) / max(wins, 1)
            avg_loss = sum(t["pnl"] for t in log if t["pnl"] <= 0) / max(n - wins, 1)

            r = {
                "strategy": name, "spread": spread, "trades": n,
                "wins": wins, "wr": round(wins/n*100,1) if n else 0,
                "total_pnl": round(total_pnl, 2),
                "avg_pnl_per_trade": round(total_pnl/n, 4) if n else 0,
                "avg_win": round(avg_win, 4), "avg_loss": round(avg_loss, 4),
                "pnl_per_1000": round(total_pnl / max(n * 20 / 1000, 0.01), 2),
            }
            all_results.append(r)

    # Print results
    print(f"\n{'='*100}")
    print(f"INTRADAY STRATEGY BACKTEST RESULTS ($20/trade)")
    print(f"{'='*100}")
    print(f"{'Strategy':<20} {'Spread':>6} {'Trades':>7} {'WR%':>5} {'TotalPnL':>10} {'AvgPnL':>8} {'AvgWin':>8} {'AvgLoss':>8}")
    print("-"*100)

    for r in sorted(all_results, key=lambda x: (x["strategy"], x["spread"])):
        print(f"{r['strategy']:<20} {r['spread']:>5.1%} {r['trades']:>7} {r['wr']:>4.0f}% "
              f"${r['total_pnl']:>+9.2f} ${r['avg_pnl_per_trade']:>+7.4f} "
              f"${r['avg_win']:>+7.4f} ${r['avg_loss']:>+7.4f}")

    # Summary
    print(f"\n{'='*100}")
    print("KEY FINDINGS:")
    for name in ["mean_reversion", "anti_momentum", "whale_fade"]:
        zero_spread = [r for r in all_results if r["strategy"]==name and r["spread"]==0.0]
        two_pct = [r for r in all_results if r["strategy"]==name and r["spread"]==0.02]
        if zero_spread:
            z = zero_spread[0]
            print(f"\n  {name}:")
            print(f"    0% spread: {z['trades']} trades, WR={z['wr']}%, PnL=${z['total_pnl']:+.2f}, avg=${z['avg_pnl_per_trade']:+.4f}/trade")
        if two_pct:
            t = two_pct[0]
            print(f"    2% spread: {t['trades']} trades, WR={t['wr']}%, PnL=${t['total_pnl']:+.2f}, avg=${t['avg_pnl_per_trade']:+.4f}/trade")

    # Breakeven spread
    print(f"\n  Breakeven spreads:")
    for name in ["mean_reversion", "anti_momentum", "whale_fade"]:
        strat = [r for r in all_results if r["strategy"]==name]
        strat.sort(key=lambda x: x["spread"])
        prev_positive = None
        for r in strat:
            if r["total_pnl"] > 0:
                prev_positive = r["spread"]
            elif prev_positive is not None:
                print(f"    {name}: profitable up to ~{prev_positive:.1%} spread")
                break
        else:
            if strat and strat[-1]["total_pnl"] > 0:
                print(f"    {name}: profitable even at {strat[-1]['spread']:.1%} spread")
            elif strat:
                print(f"    {name}: NOT profitable at any spread level")

    # Save
    with open("bot-data/intraday_backtest.json", "w") as f:
        json.dump({"results": all_results, "n_markets": len(market_data)}, f, indent=2)

    elapsed = time.time() - start
    print(f"\nCompleted in {elapsed:.0f}s. Saved to bot-data/intraday_backtest.json")


if __name__ == "__main__":
    main()
