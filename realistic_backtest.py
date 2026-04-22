#!/usr/bin/env python3
"""
REALISTIC intraday backtest with proper filters:
- Only mid-range prices (0.10-0.90)
- Minimum hourly volume $200+
- Position sizing = min($20, hourly_volume * 0.1)  # max 10% of volume
- Proper NO pricing for short side
"""
import json, time
from collections import defaultdict
import httpx

GAMMA_URL = "https://gamma-api.polymarket.com"
DATA_URL = "https://data-api.polymarket.com"
client = httpx.Client(timeout=20)

MIN_HOUR_VOL = 200  # minimum $200 hourly volume
MIN_PRICE = 0.10
MAX_PRICE = 0.90
MAX_POS_FRAC = 0.10  # max 10% of hourly volume
BASE_SIZE = 20.0


def fetch_markets(n=200):
    markets = []
    for offset in range(0, n, 100):
        r = client.get(f"{GAMMA_URL}/markets", params={
            "active": "true", "closed": "false", "limit": 100,
            "offset": offset, "order": "volume24hr", "ascending": "false"})
        r.raise_for_status()
        b = r.json()
        if not b: break
        markets.extend(b)
        time.sleep(0.2)
    return markets[:n]


def fetch_trades(cid, limit=500):
    r = client.get(f"{DATA_URL}/trades", params={"market": cid, "limit": limit})
    r.raise_for_status()
    return r.json()


def build_series(trades):
    """Build hourly VWAP with volume in USD."""
    buckets = defaultdict(lambda: {"pv": 0.0, "vol_shares": 0.0, "vol_usd": 0.0, "cnt": 0})
    for t in trades:
        ts = int(t.get("timestamp") or 0)
        price = float(t.get("price", 0))
        size = float(t.get("size", 0))
        if ts == 0 or price <= 0 or size <= 0:
            continue
        hour = (ts // 3600) * 3600
        b = buckets[hour]
        b["pv"] += price * size
        b["vol_shares"] += size
        b["vol_usd"] += price * size
        b["cnt"] += 1

    series = []
    for h in sorted(buckets):
        b = buckets[h]
        vwap = b["pv"] / b["vol_shares"] if b["vol_shares"] > 0 else 0
        series.append({
            "ts": h, "vwap": vwap, "vol_usd": b["vol_usd"],
            "vol_shares": b["vol_shares"], "cnt": b["cnt"]
        })
    return series


def mean_reversion_trades(series, spread=0.02):
    """After 5%+ hourly move, bet on reversion in 2h. Only mid-range prices."""
    trades = []
    for i in range(len(series) - 3):
        s0, s1, s3 = series[i], series[i+1], series[i+3]
        p0, p1, p3 = s0["vwap"], s1["vwap"], s3["vwap"]

        if p0 <= 0 or p1 <= 0 or p3 <= 0:
            continue

        move = (p1 - p0) / p0
        if abs(move) < 0.05:
            continue

        # Filter: only mid-range prices
        if not (MIN_PRICE <= p1 <= MAX_PRICE):
            continue

        # Filter: minimum volume at entry and exit hours
        if s1["vol_usd"] < MIN_HOUR_VOL or s3["vol_usd"] < MIN_HOUR_VOL:
            continue

        if move > 0:
            # Price UP → buy NO (expect reversion down)
            no_entry = 1.0 - p1 + spread / 2
            no_exit = 1.0 - p3 - spread / 2
            if not (MIN_PRICE <= no_entry <= MAX_PRICE):
                continue
            # Position size: min of base_size and 10% of available volume
            avail_vol = min(s1["vol_usd"], s3["vol_usd"]) * MAX_POS_FRAC
            trade_size = min(BASE_SIZE, avail_vol)
            if trade_size < 1:
                continue
            shares = trade_size / no_entry
            pnl = (no_exit - no_entry) * shares
            direction = "short"
            entry = no_entry
            exit_p = no_exit
        else:
            # Price DOWN → buy YES (expect reversion up)
            entry = p1 + spread / 2
            exit_p = p3 - spread / 2
            if not (MIN_PRICE <= entry <= MAX_PRICE):
                continue
            avail_vol = min(s1["vol_usd"], s3["vol_usd"]) * MAX_POS_FRAC
            trade_size = min(BASE_SIZE, avail_vol)
            if trade_size < 1:
                continue
            shares = trade_size / entry
            pnl = (exit_p - entry) * shares
            direction = "long"

        trades.append({
            "direction": direction, "entry": round(entry, 4),
            "exit": round(exit_p, 4), "pnl": round(pnl, 4),
            "size": round(trade_size, 2), "move_pct": round(move * 100, 2),
        })
    return trades


def anti_momentum_trades(series, spread=0.02):
    """After 3 up-hours, buy NO (bet on reversal)."""
    trades = []
    for i in range(3, len(series)):
        up = all(series[j]["vwap"] > series[j-1]["vwap"] for j in range(i-2, i+1))
        if not up:
            continue
        if i + 1 >= len(series):
            continue

        si, sn = series[i], series[i+1]
        pi, pn = si["vwap"], sn["vwap"]

        if not (MIN_PRICE <= pi <= MAX_PRICE):
            continue
        if si["vol_usd"] < MIN_HOUR_VOL or sn["vol_usd"] < MIN_HOUR_VOL:
            continue

        no_entry = 1.0 - pi + spread / 2
        no_exit = 1.0 - pn - spread / 2
        if not (MIN_PRICE <= no_entry <= MAX_PRICE):
            continue

        avail_vol = min(si["vol_usd"], sn["vol_usd"]) * MAX_POS_FRAC
        trade_size = min(BASE_SIZE, avail_vol)
        if trade_size < 1:
            continue

        shares = trade_size / no_entry
        pnl = (no_exit - no_entry) * shares

        trades.append({
            "entry": round(no_entry, 4), "exit": round(no_exit, 4),
            "pnl": round(pnl, 4), "size": round(trade_size, 2),
        })
    return trades


def whale_fade_trades(raw_trades, series, spread=0.02):
    """After whale trade >$500, fade them next hour."""
    if not series:
        return []
    hour_data = {s["ts"]: s for s in series}
    trades = []

    for t in raw_trades:
        price = float(t.get("price", 0))
        size = float(t.get("size", 0))
        if price * size < 500:
            continue
        ts = int(t.get("timestamp") or 0)
        if ts == 0:
            continue
        side = (t.get("side") or "").upper()
        if side not in ("BUY", "SELL"):
            continue

        hour_ts = (ts // 3600) * 3600
        next_h = hour_ts + 3600
        next2 = hour_ts + 7200

        s_now = hour_data.get(next_h)
        s_later = hour_data.get(next2)
        if not s_now or not s_later:
            continue

        p_now = s_now["vwap"]
        p_later = s_later["vwap"]

        if side == "BUY":
            # Whale bought YES → we buy NO
            entry = 1.0 - p_now + spread / 2
            exit_p = 1.0 - p_later - spread / 2
        else:
            # Whale sold YES → we buy YES
            entry = p_now + spread / 2
            exit_p = p_later - spread / 2

        if not (MIN_PRICE <= entry <= MAX_PRICE):
            continue
        if s_now["vol_usd"] < MIN_HOUR_VOL or s_later["vol_usd"] < MIN_HOUR_VOL:
            continue

        avail_vol = min(s_now["vol_usd"], s_later["vol_usd"]) * MAX_POS_FRAC
        trade_size = min(BASE_SIZE, avail_vol)
        if trade_size < 1:
            continue

        shares = trade_size / entry
        pnl = (exit_p - entry) * shares

        trades.append({
            "entry": round(entry, 4), "exit": round(exit_p, 4),
            "pnl": round(pnl, 4), "size": round(trade_size, 2),
            "whale_side": side,
        })
    return trades


def summarize(name, trade_list):
    if not trade_list:
        return {"name": name, "trades": 0}
    n = len(trade_list)
    wins = sum(1 for t in trade_list if t["pnl"] > 0)
    total_pnl = sum(t["pnl"] for t in trade_list)
    total_invested = sum(t["size"] for t in trade_list)
    avg_size = total_invested / n
    win_pnls = [t["pnl"] for t in trade_list if t["pnl"] > 0]
    loss_pnls = [t["pnl"] for t in trade_list if t["pnl"] <= 0]
    return {
        "name": name, "trades": n, "wins": wins,
        "wr": round(wins / n * 100, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / n, 4),
        "avg_size": round(avg_size, 2),
        "avg_win": round(sum(win_pnls) / len(win_pnls), 4) if win_pnls else 0,
        "avg_loss": round(sum(loss_pnls) / len(loss_pnls), 4) if loss_pnls else 0,
        "total_invested": round(total_invested, 2),
        "roi_pct": round(total_pnl / total_invested * 100, 2) if total_invested > 0 else 0,
    }


def main():
    start = time.time()
    print("=== REALISTIC INTRADAY BACKTEST ===")
    print(f"Filters: price {MIN_PRICE}-{MAX_PRICE}, min vol ${MIN_HOUR_VOL}, max {MAX_POS_FRAC:.0%} of volume\n")

    markets = fetch_markets(200)
    print(f"Fetched {len(markets)} markets\n")

    # Collect data
    market_data = []
    for idx, mkt in enumerate(markets):
        cid = mkt.get("conditionId") or mkt.get("condition_id") or ""
        if not cid:
            continue
        try:
            trades = fetch_trades(cid)
        except:
            time.sleep(0.2)
            continue
        time.sleep(0.15)
        if not trades:
            continue
        series = build_series(trades)
        market_data.append((series, trades, mkt.get("question", "")[:50]))
        if (idx + 1) % 50 == 0:
            print(f"  [{idx+1}/{len(markets)}] {len(market_data)} markets, {time.time()-start:.0f}s")

    print(f"\nCollected {len(market_data)} markets.\n")

    # Run backtests at different spread levels
    all_results = []
    for spread in [0.0, 0.01, 0.02, 0.03, 0.05]:
        mr_all, am_all, wf_all = [], [], []
        for series, raw_trades, q in market_data:
            mr_all.extend(mean_reversion_trades(series, spread))
            am_all.extend(anti_momentum_trades(series, spread))
            wf_all.extend(whale_fade_trades(raw_trades, series, spread))

        for name, tl in [
            (f"mean_reversion_sp{int(spread*100)}pct", mr_all),
            (f"anti_momentum_sp{int(spread*100)}pct", am_all),
            (f"whale_fade_sp{int(spread*100)}pct", wf_all),
        ]:
            all_results.append(summarize(name, tl))

    # Print
    print(f"{'='*110}")
    print(f"REALISTIC BACKTEST: prices {MIN_PRICE}-{MAX_PRICE}, min hourly vol ${MIN_HOUR_VOL}, max pos = {MAX_POS_FRAC:.0%} of vol")
    print(f"{'='*110}")
    print(f"{'Strategy':<35} {'Trades':>6} {'WR%':>5} {'TotPnL':>10} {'AvgPnL':>8} {'AvgSize':>8} {'ROI%':>7} {'AvgWin':>8} {'AvgLoss':>8}")
    print("-" * 110)
    for r in all_results:
        if r["trades"] == 0:
            continue
        print(f"{r['name']:<35} {r['trades']:>6} {r['wr']:>4.0f}% "
              f"${r['total_pnl']:>+9.2f} ${r['avg_pnl']:>+7.4f} "
              f"${r['avg_size']:>7.2f} {r['roi_pct']:>+6.2f}% "
              f"${r['avg_win']:>+7.4f} ${r['avg_loss']:>+7.4f}")

    # Headline summary
    print(f"\n{'='*110}")
    print("STRATEGY COMPARISON (2% spread — realistic)")
    print(f"{'='*110}")
    for strat in ["mean_reversion", "anti_momentum", "whale_fade"]:
        sp2 = [r for r in all_results if r["name"] == f"{strat}_sp2pct"]
        sp0 = [r for r in all_results if r["name"] == f"{strat}_sp0pct"]
        if sp2 and sp2[0]["trades"] > 0:
            r = sp2[0]
            print(f"\n  {strat}:")
            print(f"    Trades: {r['trades']}, Win rate: {r['wr']}%")
            print(f"    Total PnL: ${r['total_pnl']:+.2f} on ${r['total_invested']:,.0f} invested")
            print(f"    ROI: {r['roi_pct']:+.2f}%")
            print(f"    Avg trade: size=${r['avg_size']:.2f}, pnl=${r['avg_pnl']:+.4f}")
            print(f"    Avg win: ${r['avg_win']:+.4f}, Avg loss: ${r['avg_loss']:+.4f}")
        if sp0 and sp0[0]["trades"] > 0:
            r0 = sp0[0]
            print(f"    (0% spread: {r0['trades']} trades, ROI {r0['roi_pct']:+.2f}%)")

    # Breakeven analysis
    print(f"\n{'='*110}")
    print("BREAKEVEN SPREAD")
    for strat in ["mean_reversion", "anti_momentum", "whale_fade"]:
        for r in all_results:
            if strat in r["name"] and r["trades"] > 0 and r["roi_pct"] <= 0:
                # Find last profitable
                prev = [x for x in all_results if strat in x["name"] and x["roi_pct"] > 0]
                if prev:
                    last_pos = max(prev, key=lambda x: int(x["name"].split("sp")[1].split("pct")[0]))
                    print(f"  {strat}: profitable up to ~{last_pos['name'].split('sp')[1].split('pct')[0]}% spread (ROI {last_pos['roi_pct']:+.2f}%)")
                else:
                    print(f"  {strat}: NOT profitable at any spread")
                break
        else:
            best = [r for r in all_results if strat in r["name"] and r["trades"] > 0]
            if best:
                last = max(best, key=lambda x: int(x["name"].split("sp")[1].split("pct")[0]))
                print(f"  {strat}: profitable even at {last['name'].split('sp')[1].split('pct')[0]}% spread (ROI {last['roi_pct']:+.2f}%)")

    # Save
    with open("bot-data/realistic_backtest.json", "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nSaved to bot-data/realistic_backtest.json ({time.time()-start:.0f}s)")


if __name__ == "__main__":
    main()
