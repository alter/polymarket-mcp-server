#!/usr/bin/env python3
"""
Whale Trading Pattern Analyzer

Reads collected data from bot-data/analytics/ and identifies:
- Whale PnL and win rates across resolved markets
- Smart money vs dumb money classification
- Whale flow signals for active markets
- Whale timing patterns (early/late, with/against crowd)

Output: prints rankings + saves bot-data/whale_analysis.json
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot-data", "analytics")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot-data")


def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"[WARN] Missing file: {path}")
        return []
    with open(path) as f:
        return json.load(f)


def build_resolution_map(resolved_markets):
    """Build market_id -> winning_outcome_index map.

    outcome_prices is a list like ['1','0'] or ['0','1'].
    The index with price '1' is the winning outcome.
    If yes_won=True => outcome_prices=['1','0'] => outcomeIndex 0 won.
    If yes_won=False => outcome_prices=['0','1'] => outcomeIndex 1 won.
    """
    res_map = {}
    for m in resolved_markets:
        mid = m["id"]
        prices = m.get("outcome_prices", [])
        if not prices:
            continue
        # Find winning index: the one with price '1' (or closest to 1)
        try:
            float_prices = [float(p) for p in prices]
        except (ValueError, TypeError):
            continue
        winning_idx = float_prices.index(max(float_prices))
        # Only include markets that actually resolved (at least one price is 1)
        if max(float_prices) < 0.5:
            continue
        res_map[mid] = {
            "winning_index": winning_idx,
            "yes_won": m.get("yes_won", False),
            "question": m.get("question", ""),
            "volume": m.get("volume", 0),
            "end_date": m.get("end_date", ""),
        }
    return res_map


def calculate_trade_pnl(trade, resolution):
    """Calculate PnL for a single trade given market resolution.

    For a BUY:
      - If bought the winning outcome: PnL = (1 - price) * size  (pays out $1 per share)
      - If bought the losing outcome:  PnL = -price * size        (shares worth $0)
    For a SELL:
      - If sold the winning outcome:   PnL = -(1 - price) * size  (missed the $1 payout)
      - If sold the losing outcome:    PnL = price * size          (sold before it went to $0)
    """
    side = trade["side"]
    outcome_idx = trade["outcomeIndex"]
    price = trade["price"]
    size = trade["size"]
    winning_idx = resolution["winning_index"]

    is_winning_outcome = outcome_idx == winning_idx

    if side == "BUY":
        if is_winning_outcome:
            return (1.0 - price) * size, True  # profit, win
        else:
            return -price * size, False  # loss
    else:  # SELL
        if is_winning_outcome:
            return -(1.0 - price) * size, False  # sold winner = loss
        else:
            return price * size, True  # sold loser = profit


def analyze_whale_pnl(whales, trades, resolution_map):
    """For each whale wallet, calculate PnL across resolved markets."""
    # Index trades by wallet
    trades_by_wallet = defaultdict(list)
    for t in trades:
        trades_by_wallet[t["proxyWallet"]].append(t)

    # Top 50 whales by volume
    top_whales = sorted(whales, key=lambda w: w["total_volume"], reverse=True)[:50]
    whale_wallets = {w["wallet"] for w in top_whales}
    whale_info = {w["wallet"]: w for w in top_whales}

    wallet_stats = {}
    for wallet in whale_wallets:
        wtrades = trades_by_wallet.get(wallet, [])
        if not wtrades:
            continue

        total_pnl = 0.0
        wins = 0
        losses = 0
        resolved_trades = 0
        markets_traded = set()
        pnl_by_market = defaultdict(float)
        trade_details = []

        for t in wtrades:
            mid = t["market_id"]
            markets_traded.add(mid)
            if mid not in resolution_map:
                continue

            pnl, is_win = calculate_trade_pnl(t, resolution_map[mid])
            total_pnl += pnl
            pnl_by_market[mid] += pnl
            resolved_trades += 1
            if is_win:
                wins += 1
            else:
                losses += 1

            trade_details.append({
                "market_id": mid,
                "side": t["side"],
                "outcome": t["outcome"],
                "price": t["price"],
                "size": t["size"],
                "pnl": round(pnl, 2),
                "win": is_win,
            })

        total_decided = wins + losses
        win_rate = wins / total_decided if total_decided > 0 else 0.0
        avg_pnl = total_pnl / resolved_trades if resolved_trades > 0 else 0.0

        # Classify
        if total_decided >= 3 and win_rate > 0.55:
            classification = "smart_money"
        elif total_decided >= 3 and win_rate < 0.45:
            classification = "dumb_money"
        else:
            classification = "neutral"

        info = whale_info.get(wallet, {})
        wallet_stats[wallet] = {
            "wallet": wallet,
            "pseudonym": info.get("pseudonym", "unknown"),
            "total_volume": info.get("total_volume", 0),
            "total_trades": len(wtrades),
            "resolved_trades": resolved_trades,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 4),
            "total_pnl": round(total_pnl, 2),
            "avg_pnl_per_trade": round(avg_pnl, 2),
            "markets_traded": len(markets_traded),
            "markets_resolved": len(pnl_by_market),
            "classification": classification,
            "trade_details": trade_details,
        }

    return wallet_stats


def analyze_whale_flow(whales, trades, resolution_map, orderbook, wallet_stats):
    """Calculate net whale flow signals for active markets (those in orderbook)."""
    ob_map = {o["market_id"]: o for o in orderbook}
    resolved_ids = set(resolution_map.keys())

    # Active = in orderbook and not resolved
    active_ids = set(ob_map.keys()) - resolved_ids

    # Smart money wallets
    smart_wallets = {w for w, s in wallet_stats.items() if s["classification"] == "smart_money"}
    dumb_wallets = {w for w, s in wallet_stats.items() if s["classification"] == "dumb_money"}

    # Index trades by market
    trades_by_market = defaultdict(list)
    for t in trades:
        trades_by_market[t["market_id"]].append(t)

    # Also check whale activity for market membership (even without individual trades in trades.json)
    whale_market_membership = defaultdict(set)
    for w in whales:
        for mid in w.get("markets", []):
            whale_market_membership[mid].add(w["wallet"])

    signals = []
    for mid in active_ids:
        ob = ob_map[mid]
        mkt_trades = trades_by_market.get(mid, [])

        smart_buy_volume = 0.0
        smart_sell_volume = 0.0
        dumb_buy_volume = 0.0
        dumb_sell_volume = 0.0
        whale_buy_volume = 0.0
        whale_sell_volume = 0.0

        for t in mkt_trades:
            wallet = t["proxyWallet"]
            vol = t["size"] * t["price"]
            if wallet in smart_wallets:
                if t["side"] == "BUY":
                    smart_buy_volume += vol
                else:
                    smart_sell_volume += vol
            elif wallet in dumb_wallets:
                if t["side"] == "BUY":
                    dumb_buy_volume += vol
                else:
                    dumb_sell_volume += vol

            # All whales
            if wallet in {w["wallet"] for w in whales}:
                if t["side"] == "BUY":
                    whale_buy_volume += vol
                else:
                    whale_sell_volume += vol

        net_smart_flow = smart_buy_volume - smart_sell_volume
        net_dumb_flow = dumb_buy_volume - dumb_sell_volume
        net_whale_flow = whale_buy_volume - whale_sell_volume

        # Count smart/dumb wallets present in this market
        market_whales = whale_market_membership.get(mid, set())
        smart_count = len(market_whales & smart_wallets)
        dumb_count = len(market_whales & dumb_wallets)

        # Determine signal
        if net_smart_flow > 0 and smart_count > 0:
            signal = "FOLLOW_YES"
            confidence = "high" if smart_count >= 3 else "medium" if smart_count >= 2 else "low"
        elif net_smart_flow < 0 and smart_count > 0:
            signal = "FOLLOW_NO"
            confidence = "high" if smart_count >= 3 else "medium" if smart_count >= 2 else "low"
        elif net_dumb_flow > 0 and dumb_count > 0:
            signal = "FADE_YES"  # fade dumb money
            confidence = "low"
        elif net_dumb_flow < 0 and dumb_count > 0:
            signal = "FADE_NO"
            confidence = "low"
        elif smart_count > 0:
            signal = "SMART_PRESENT"
            confidence = "low"
        else:
            signal = "NO_SIGNAL"
            confidence = "none"

        signals.append({
            "market_id": mid,
            "question": ob.get("question", ""),
            "bid": ob.get("bid", 0),
            "ask": ob.get("ask", 0),
            "mid_price": ob.get("mid", 0),
            "spread": ob.get("spread", 0),
            "volume_24h": ob.get("volume_24h", 0),
            "smart_wallets_present": smart_count,
            "dumb_wallets_present": dumb_count,
            "net_smart_flow": round(net_smart_flow, 2),
            "net_dumb_flow": round(net_dumb_flow, 2),
            "net_whale_flow": round(net_whale_flow, 2),
            "whale_buy_volume": round(whale_buy_volume, 2),
            "whale_sell_volume": round(whale_sell_volume, 2),
            "signal": signal,
            "confidence": confidence,
        })

    signals.sort(key=lambda s: abs(s["net_smart_flow"]), reverse=True)
    return signals


def analyze_whale_timing(trades, resolution_map, wallet_stats):
    """Analyze whale timing: early/late entry, with/against crowd."""
    smart_wallets = {w for w, s in wallet_stats.items() if s["classification"] == "smart_money"}
    all_whale_wallets = set(wallet_stats.keys())

    # Group trades by market
    trades_by_market = defaultdict(list)
    for t in trades:
        trades_by_market[t["market_id"]].append(t)

    timing_stats = {
        "smart_avg_entry_price": [],
        "retail_avg_entry_price": [],
        "smart_buys_winning_outcome": 0,
        "smart_buys_losing_outcome": 0,
        "smart_early_trades": 0,  # first quartile of market trades
        "smart_late_trades": 0,  # last quartile
        "smart_mid_trades": 0,
        "retail_early_trades": 0,
        "retail_late_trades": 0,
    }

    for mid, mkt_trades in trades_by_market.items():
        if mid not in resolution_map:
            continue

        res = resolution_map[mid]
        winning_idx = res["winning_index"]

        # Sort by timestamp
        mkt_trades_sorted = sorted(mkt_trades, key=lambda t: t.get("timestamp", 0))
        n = len(mkt_trades_sorted)
        if n == 0:
            continue

        q1 = n // 4
        q3 = 3 * n // 4

        for i, t in enumerate(mkt_trades_sorted):
            wallet = t["proxyWallet"]
            is_smart = wallet in smart_wallets
            is_whale = wallet in all_whale_wallets

            if t["side"] == "BUY":
                # Track entry prices for winning outcome buyers
                if t["outcomeIndex"] == winning_idx:
                    if is_smart:
                        timing_stats["smart_avg_entry_price"].append(t["price"])
                        timing_stats["smart_buys_winning_outcome"] += 1
                    elif not is_whale:
                        timing_stats["retail_avg_entry_price"].append(t["price"])
                else:
                    if is_smart:
                        timing_stats["smart_buys_losing_outcome"] += 1

            # Timing quartile
            if is_smart:
                if i <= q1:
                    timing_stats["smart_early_trades"] += 1
                elif i >= q3:
                    timing_stats["smart_late_trades"] += 1
                else:
                    timing_stats["smart_mid_trades"] += 1
            elif not is_whale:
                if i <= q1:
                    timing_stats["retail_early_trades"] += 1
                elif i >= q3:
                    timing_stats["retail_late_trades"] += 1

    # Compute averages
    smart_prices = timing_stats.pop("smart_avg_entry_price")
    retail_prices = timing_stats.pop("retail_avg_entry_price")

    timing_stats["smart_avg_entry_price_winning"] = (
        round(sum(smart_prices) / len(smart_prices), 4) if smart_prices else None
    )
    timing_stats["retail_avg_entry_price_winning"] = (
        round(sum(retail_prices) / len(retail_prices), 4) if retail_prices else None
    )
    timing_stats["smart_entry_count"] = len(smart_prices)
    timing_stats["retail_entry_count"] = len(retail_prices)

    # Derive insights
    smart_total = (
        timing_stats["smart_early_trades"]
        + timing_stats["smart_mid_trades"]
        + timing_stats["smart_late_trades"]
    )
    if smart_total > 0:
        timing_stats["smart_early_pct"] = round(
            timing_stats["smart_early_trades"] / smart_total * 100, 1
        )
        timing_stats["smart_late_pct"] = round(
            timing_stats["smart_late_trades"] / smart_total * 100, 1
        )
    else:
        timing_stats["smart_early_pct"] = 0
        timing_stats["smart_late_pct"] = 0

    smart_bets = (
        timing_stats["smart_buys_winning_outcome"] + timing_stats["smart_buys_losing_outcome"]
    )
    timing_stats["smart_directional_accuracy"] = (
        round(timing_stats["smart_buys_winning_outcome"] / smart_bets * 100, 1)
        if smart_bets > 0
        else 0
    )

    return timing_stats


def print_rankings(wallet_stats):
    """Print top 20 smart money and top 20 dumb money wallets."""
    all_wallets = sorted(wallet_stats.values(), key=lambda w: w["total_pnl"], reverse=True)

    smart = [w for w in all_wallets if w["classification"] == "smart_money"]
    dumb = [w for w in all_wallets if w["classification"] == "dumb_money"]

    header = f"{'Rank':<5} {'Pseudonym':<25} {'WR':<7} {'PnL':>10} {'Trades':>7} {'Vol':>12}"
    sep = "-" * 75

    print("\n" + "=" * 75)
    print("  SMART MONEY RANKINGS (profitable, WR > 55%)")
    print("=" * 75)
    print(header)
    print(sep)
    for i, w in enumerate(smart[:20], 1):
        print(
            f"{i:<5} {w['pseudonym']:<25} {w['win_rate']*100:5.1f}% "
            f"${w['total_pnl']:>9,.2f} {w['resolved_trades']:>7} "
            f"${w['total_volume']:>11,.0f}"
        )
    if not smart:
        print("  (no wallets classified as smart money)")

    print("\n" + "=" * 75)
    print("  DUMB MONEY RANKINGS (losing, WR < 45%)")
    print("=" * 75)
    print(header)
    print(sep)
    dumb_sorted = sorted(dumb, key=lambda w: w["total_pnl"])  # worst first
    for i, w in enumerate(dumb_sorted[:20], 1):
        print(
            f"{i:<5} {w['pseudonym']:<25} {w['win_rate']*100:5.1f}% "
            f"${w['total_pnl']:>9,.2f} {w['resolved_trades']:>7} "
            f"${w['total_volume']:>11,.0f}"
        )
    if not dumb:
        print("  (no wallets classified as dumb money)")


def print_flow_signals(signals):
    """Print whale flow signals for active markets."""
    print("\n" + "=" * 90)
    print("  ACTIVE MARKET WHALE FLOW SIGNALS")
    print("=" * 90)
    print(
        f"{'Signal':<15} {'Conf':<7} {'SmartFlow':>11} {'DumbFlow':>11} "
        f"{'Mid':>6} {'Question':<40}"
    )
    print("-" * 90)

    actionable = [s for s in signals if s["signal"] != "NO_SIGNAL"]
    for s in actionable[:30]:
        q = s["question"][:38] + ".." if len(s["question"]) > 40 else s["question"]
        print(
            f"{s['signal']:<15} {s['confidence']:<7} "
            f"${s['net_smart_flow']:>10,.2f} ${s['net_dumb_flow']:>10,.2f} "
            f"{s['mid_price']:>5.3f} {q:<40}"
        )
    if not actionable:
        print("  (no actionable signals - whale trades not in current orderbook markets)")
    print(f"\n  Total active markets: {len(signals)} | With signals: {len(actionable)}")


def print_timing(timing):
    """Print whale timing analysis."""
    print("\n" + "=" * 75)
    print("  WHALE TIMING ANALYSIS")
    print("=" * 75)

    print(f"\n  Timing distribution (smart money):")
    print(f"    Early trades (1st quartile): {timing['smart_early_pct']}%")
    print(f"    Late trades (4th quartile):  {timing['smart_late_pct']}%")

    print(f"\n  Directional accuracy (smart money buying winning outcome):")
    print(f"    Accuracy: {timing['smart_directional_accuracy']}%")
    print(
        f"    Winning bets: {timing['smart_buys_winning_outcome']} | "
        f"Losing bets: {timing['smart_buys_losing_outcome']}"
    )

    print(f"\n  Average entry price on winning outcome:")
    smart_p = timing.get("smart_avg_entry_price_winning")
    retail_p = timing.get("retail_avg_entry_price_winning")
    if smart_p is not None:
        print(f"    Smart money: {smart_p:.4f}  ({timing['smart_entry_count']} entries)")
    else:
        print(f"    Smart money: N/A")
    if retail_p is not None:
        print(f"    Retail:      {retail_p:.4f}  ({timing['retail_entry_count']} entries)")
    else:
        print(f"    Retail:      N/A")

    if smart_p and retail_p:
        diff = retail_p - smart_p
        if diff > 0:
            print(f"    -> Smart money enters {diff:.4f} cheaper on average (better prices)")
        elif diff < 0:
            print(f"    -> Retail enters {abs(diff):.4f} cheaper on average")
        else:
            print(f"    -> Entry prices are similar")


def main():
    print("Loading data...")
    whales = load_json("whale_activity.json")
    trades = load_json("trades.json")
    resolved = load_json("resolved_markets.json")
    orderbook = load_json("orderbook_snapshots.json")

    if not whales or not trades:
        print("[ERROR] Missing whale_activity.json or trades.json. Exiting.")
        sys.exit(1)

    print(f"  Whales: {len(whales)} wallets")
    print(f"  Trades: {len(trades)} records")
    print(f"  Resolved markets: {len(resolved)}")
    print(f"  Orderbook snapshots: {len(orderbook)}")

    # Build resolution map
    resolution_map = build_resolution_map(resolved)
    print(f"  Resolved with clear outcome: {len(resolution_map)}")

    # 1. Whale PnL analysis
    print("\nAnalyzing whale PnL...")
    wallet_stats = analyze_whale_pnl(whales, trades, resolution_map)
    print(f"  Wallets with resolved trades: {len(wallet_stats)}")

    classifications = defaultdict(int)
    for s in wallet_stats.values():
        classifications[s["classification"]] += 1
    for cls, count in sorted(classifications.items()):
        print(f"    {cls}: {count}")

    # 2. Whale flow signals
    print("\nAnalyzing whale flow signals...")
    flow_signals = analyze_whale_flow(whales, trades, resolution_map, orderbook, wallet_stats)
    print(f"  Active markets analyzed: {len(flow_signals)}")

    # 3. Whale timing analysis
    print("\nAnalyzing whale timing...")
    timing = analyze_whale_timing(trades, resolution_map, wallet_stats)

    # Print results
    print_rankings(wallet_stats)
    print_flow_signals(flow_signals)
    print_timing(timing)

    # 4. Save full analysis
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_whales_analyzed": len(wallet_stats),
            "smart_money_count": classifications.get("smart_money", 0),
            "dumb_money_count": classifications.get("dumb_money", 0),
            "neutral_count": classifications.get("neutral", 0),
            "resolved_markets_used": len(resolution_map),
            "active_markets_with_signals": len(
                [s for s in flow_signals if s["signal"] != "NO_SIGNAL"]
            ),
        },
        "wallet_rankings": {
            "smart_money": sorted(
                [
                    {k: v for k, v in w.items() if k != "trade_details"}
                    for w in wallet_stats.values()
                    if w["classification"] == "smart_money"
                ],
                key=lambda w: w["total_pnl"],
                reverse=True,
            ),
            "dumb_money": sorted(
                [
                    {k: v for k, v in w.items() if k != "trade_details"}
                    for w in wallet_stats.values()
                    if w["classification"] == "dumb_money"
                ],
                key=lambda w: w["total_pnl"],
            ),
            "neutral": sorted(
                [
                    {k: v for k, v in w.items() if k != "trade_details"}
                    for w in wallet_stats.values()
                    if w["classification"] == "neutral"
                ],
                key=lambda w: w["total_pnl"],
                reverse=True,
            ),
        },
        "flow_signals": flow_signals,
        "timing_analysis": timing,
        "wallet_details": {
            w: s for w, s in wallet_stats.items()
        },
    }

    output_path = os.path.join(OUTPUT_DIR, "whale_analysis.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nFull analysis saved to {output_path}")


if __name__ == "__main__":
    main()
