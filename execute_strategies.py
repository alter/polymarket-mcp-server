#!/usr/bin/env python3
"""
Execute bias + arb strategies on Polymarket (paper trading).
Places paper orders based on bias scanner and arb scanner signals.
Tracks portfolio in bot-data/strategy_portfolio.json

Strategies:
  1. Nobel Prize arb: buy YES on all outcomes (sum < 1.0)
  2. NO bias: buy NO on markets where historical NO rate > current price implies
  3. Fed rate NO: buy NO on Fed-related questions
"""

import json
import os
import re
import time
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
OUT_DIR = "bot-data"
PORTFOLIO_FILE = os.path.join(OUT_DIR, "strategy_portfolio.json")
INITIAL_BALANCE = 1000.0


def load_portfolio():
    """Load or create portfolio."""
    if os.path.exists(PORTFOLIO_FILE):
        with open(PORTFOLIO_FILE) as f:
            return json.load(f)
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "initial_balance": INITIAL_BALANCE,
        "cash": INITIAL_BALANCE,
        "positions": [],
        "closed_positions": [],
        "trades": [],
        "total_invested": 0,
        "total_returned": 0,
    }


def save_portfolio(p):
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(PORTFOLIO_FILE, "w") as f:
        json.dump(p, f, indent=2)


def get_price(client, token_id):
    """Get bid/ask for a token."""
    try:
        r1 = client.get(f"{CLOB}/price", params={"token_id": token_id, "side": "buy"})
        r2 = client.get(f"{CLOB}/price", params={"token_id": token_id, "side": "sell"})
        bid = float(r1.json().get("price", 0)) if r1.status_code == 200 else 0
        ask = float(r2.json().get("price", 0)) if r2.status_code == 200 else 0
        return bid, ask
    except Exception:
        return 0, 0


def parse_clob_ids(raw):
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return []
    return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Strategy 1: Multi-outcome Arbitrage
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXCLUSIVE_KEYWORDS = ["winner", "presidential", "election", "nomination",
                       "champion", "cup winner", "prize", "first place"]


def is_exclusive(title, questions):
    t = title.lower()
    if any(w in t for w in EXCLUSIVE_KEYWORDS):
        return True
    return False


def execute_arb(client, portfolio, max_capital=200):
    """Find and execute multi-outcome arb."""
    print("\n━━ Strategy 1: Multi-outcome Arbitrage ━━")

    # Check if we already have arb positions
    existing_arb = [p for p in portfolio["positions"] if p.get("strategy") == "arb"]
    if existing_arb:
        print(f"  Already have {len(existing_arb)} arb positions, skipping new entries")
        return

    # Fetch events
    events = []
    for offset in range(0, 200, 50):
        resp = client.get(f"{GAMMA}/events", params={
            "closed": "false", "limit": 50, "offset": offset,
            "order": "volume24hr", "ascending": "false",
        })
        if resp.status_code != 200:
            break
        batch = resp.json()
        if not batch:
            break
        events.extend(batch)
        time.sleep(0.3)

    best_arb = None
    best_profit = 0

    for e in events:
        title = e.get("title", "")
        markets = e.get("markets", [])
        if len(markets) < 3:
            continue

        questions = [m.get("question", "") for m in markets[:10]]
        if not is_exclusive(title, questions):
            continue

        # Price all markets
        total_ask = 0
        market_list = []
        all_ok = True

        for m in markets:
            clob_ids = parse_clob_ids(m.get("clobTokenIds", "[]"))
            if not clob_ids:
                all_ok = False
                break

            bid, ask = get_price(client, clob_ids[0])
            if ask <= 0:
                # Try outcomePrices fallback
                op = m.get("outcomePrices", "")
                if isinstance(op, str):
                    try:
                        op = json.loads(op)
                    except:
                        op = []
                if op:
                    ask = float(op[0])
                    bid = ask
                else:
                    all_ok = False
                    break

            total_ask += ask
            market_list.append({
                "question": m.get("question", ""),
                "token_id": clob_ids[0],
                "ask": ask,
                "bid": bid,
            })
            time.sleep(0.08)

        if not all_ok or not market_list:
            continue

        profit = 1.0 - total_ask
        if profit > best_profit and profit > 0.03:  # At least 3% profit
            best_arb = {
                "event": title,
                "markets": market_list,
                "total_ask": total_ask,
                "profit_rate": profit,
                "n_markets": len(market_list),
            }
            best_profit = profit

    if not best_arb:
        print("  No arb opportunities found")
        return

    event = best_arb["event"]
    n = best_arb["n_markets"]
    total_cost = best_arb["total_ask"]
    profit_pct = best_arb["profit_rate"] * 100

    # Scale position: $1 per outcome share, total cost = total_ask * shares
    # With max_capital, shares = max_capital / total_ask
    shares = min(max_capital / total_cost, 100)  # Cap at 100 shares
    actual_cost = total_cost * shares
    expected_return = 1.0 * shares

    if actual_cost > portfolio["cash"]:
        shares = portfolio["cash"] / total_cost * 0.9  # Use 90% of available cash
        actual_cost = total_cost * shares
        expected_return = shares

    print(f"\n  Best arb: {event}")
    print(f"  {n} markets, sum(ask)={total_cost:.4f}, profit={profit_pct:.1f}%")
    print(f"  Buying {shares:.1f} shares across all outcomes")
    print(f"  Cost: ${actual_cost:.2f}, Expected return: ${expected_return:.2f}")
    print(f"  Expected profit: ${expected_return - actual_cost:.2f}")

    # Execute: "buy" YES on each outcome
    for m in best_arb["markets"]:
        cost_this = m["ask"] * shares
        portfolio["positions"].append({
            "strategy": "arb",
            "event": event,
            "question": m["question"][:80],
            "token_id": m["token_id"],
            "side": "YES",
            "entry_price": m["ask"],
            "shares": round(shares, 4),
            "cost": round(cost_this, 4),
            "entry_time": datetime.now(timezone.utc).isoformat(),
        })
        portfolio["trades"].append({
            "time": datetime.now(timezone.utc).isoformat(),
            "strategy": "arb",
            "action": "BUY YES",
            "question": m["question"][:60],
            "price": m["ask"],
            "shares": round(shares, 4),
            "cost": round(cost_this, 4),
        })

    portfolio["cash"] -= actual_cost
    portfolio["total_invested"] += actual_cost
    print(f"  Placed {n} paper orders. Cash remaining: ${portfolio['cash']:.2f}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Strategy 2: NO Bias Exploitation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Historical NO rates by category
CATEGORY_RATES = {
    "fdv_above": 0.765,
    "fed_rate": 0.875,
    "geopolitics_general": 0.899,
    "will_generic": 0.764,
}

CATEGORY_PATTERNS = [
    ("fdv_above", r"(fdv|fully diluted).*(above|reach|hit|over)"),
    ("fed_rate", r"(fed\b|interest rate|bps|basis point|fed chair)"),
    ("geopolitics_general", r"will.*(us|u\.s\.|united states).*(strike|bomb|invade|attack)"),
    ("will_generic", r"^will\s"),
]


def classify_market(q):
    q = q.lower()
    for cat, pat in CATEGORY_PATTERNS:
        if re.search(pat, q):
            return cat
    return None


def execute_no_bias(client, portfolio, max_per_trade=20, max_total=300):
    """Buy NO on markets with exploitable bias."""
    print("\n━━ Strategy 2: NO Bias Exploitation ━━")

    # Check how much we've already deployed
    existing_bias = [p for p in portfolio["positions"] if p.get("strategy") == "no_bias"]
    deployed = sum(p["cost"] for p in existing_bias)
    if deployed >= max_total:
        print(f"  Already deployed ${deployed:.0f} in bias trades (max ${max_total}), skipping")
        return

    remaining = max_total - deployed

    # Fetch active markets
    markets = []
    for offset in range(0, 400, 100):
        resp = client.get(f"{GAMMA}/markets", params={
            "active": "true", "closed": "false",
            "limit": 100, "offset": offset,
            "order": "volume24hr", "ascending": "false",
        })
        if resp.status_code != 200:
            break
        batch = resp.json()
        if not batch:
            break
        markets.extend(batch)
        time.sleep(0.2)

    signals = []

    for m in markets:
        q = m.get("question", "")
        vol24h = float(m.get("volume24hr", 0) or 0)
        if vol24h < 10000:
            continue

        category = classify_market(q)
        if not category:
            continue

        no_rate = CATEGORY_RATES.get(category, 0.5)
        if no_rate < 0.65:
            continue

        clob_ids = parse_clob_ids(m.get("clobTokenIds", "[]"))
        if len(clob_ids) < 2:
            continue

        # Skip if we already have this position
        existing_tokens = {p["token_id"] for p in portfolio["positions"]}
        if clob_ids[1] in existing_tokens:
            continue

        # Get NO token price
        no_bid, no_ask = get_price(client, clob_ids[1])
        if no_ask <= 0 or no_ask >= 0.95:
            continue

        spread = no_ask - no_bid if no_bid > 0 else None
        if spread and spread > 0.03:
            continue

        # Edge = historical NO rate - NO ask price
        edge = no_rate - no_ask
        if edge < 0.08:  # Minimum 8% edge
            continue

        # EV per unit
        ev = no_rate * (1.0 - no_ask) - (1.0 - no_rate) * no_ask

        signals.append({
            "question": q,
            "slug": m.get("slug", ""),
            "category": category,
            "no_rate": no_rate,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "spread": spread,
            "edge": edge,
            "ev": ev,
            "token_id": clob_ids[1],
            "volume_24h": vol24h,
        })
        time.sleep(0.1)

    # Sort by edge, pick best ones
    signals.sort(key=lambda s: -s["edge"])

    # Filter for realistic trades (NO price between 0.10 and 0.85)
    signals = [s for s in signals if 0.10 <= s["no_ask"] <= 0.85]

    print(f"  Found {len(signals)} signals with edge > 8%")

    trades_placed = 0
    total_spent = 0

    for s in signals[:15]:  # Max 15 positions
        if total_spent + max_per_trade > remaining:
            break
        if total_spent + max_per_trade > portfolio["cash"]:
            break

        size = min(max_per_trade, remaining - total_spent)
        shares = size / s["no_ask"]

        print(f"  BUY NO ${size:.0f} @ ${s['no_ask']:.3f} edge={s['edge']:.1%} | {s['question'][:55]}")

        portfolio["positions"].append({
            "strategy": "no_bias",
            "category": s["category"],
            "question": s["question"][:80],
            "token_id": s["token_id"],
            "side": "NO",
            "entry_price": s["no_ask"],
            "shares": round(shares, 4),
            "cost": round(size, 4),
            "edge": round(s["edge"], 4),
            "ev": round(s["ev"], 4),
            "entry_time": datetime.now(timezone.utc).isoformat(),
        })
        portfolio["trades"].append({
            "time": datetime.now(timezone.utc).isoformat(),
            "strategy": "no_bias",
            "action": "BUY NO",
            "question": s["question"][:60],
            "price": s["no_ask"],
            "shares": round(shares, 4),
            "cost": round(size, 4),
        })

        portfolio["cash"] -= size
        portfolio["total_invested"] += size
        total_spent += size
        trades_placed += 1

    print(f"\n  Placed {trades_placed} NO bias trades, spent ${total_spent:.2f}")
    print(f"  Cash remaining: ${portfolio['cash']:.2f}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Portfolio Mark-to-Market
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def mark_to_market(client, portfolio):
    """Update portfolio with current prices."""
    print("\n━━ Portfolio Mark-to-Market ━━")

    total_value = portfolio["cash"]
    total_pnl = 0

    by_strategy = {}

    for pos in portfolio["positions"]:
        bid, ask = get_price(client, pos["token_id"])
        mid = (bid + ask) / 2 if bid > 0 and ask > 0 else (bid or ask)

        # Use bid for liquidation value
        current_value = pos["shares"] * bid if bid > 0 else pos["shares"] * mid
        cost = pos["cost"]
        pnl = current_value - cost
        pnl_pct = (pnl / cost * 100) if cost > 0 else 0

        pos["current_bid"] = round(bid, 4)
        pos["current_ask"] = round(ask, 4)
        pos["current_value"] = round(current_value, 4)
        pos["unrealized_pnl"] = round(pnl, 4)
        pos["pnl_pct"] = round(pnl_pct, 2)

        total_value += current_value
        total_pnl += pnl

        strat = pos.get("strategy", "unknown")
        if strat not in by_strategy:
            by_strategy[strat] = {"count": 0, "cost": 0, "value": 0, "pnl": 0}
        by_strategy[strat]["count"] += 1
        by_strategy[strat]["cost"] += cost
        by_strategy[strat]["value"] += current_value
        by_strategy[strat]["pnl"] += pnl

        time.sleep(0.08)

    portfolio["last_mtm"] = datetime.now(timezone.utc).isoformat()
    portfolio["total_value"] = round(total_value, 2)
    portfolio["total_unrealized_pnl"] = round(total_pnl, 2)

    print(f"\n  {'Strategy':<15} {'#Pos':>5} {'Cost':>10} {'Value':>10} {'PnL':>10} {'PnL%':>7}")
    print(f"  {'-'*60}")
    for strat, s in sorted(by_strategy.items()):
        pnl_pct = (s["pnl"] / s["cost"] * 100) if s["cost"] > 0 else 0
        print(f"  {strat:<15} {s['count']:>5} ${s['cost']:>8.2f} ${s['value']:>8.2f} ${s['pnl']:>+8.2f} {pnl_pct:>+6.1f}%")

    print(f"\n  Cash: ${portfolio['cash']:.2f}")
    print(f"  Positions value: ${total_value - portfolio['cash']:.2f}")
    print(f"  Total portfolio: ${total_value:.2f}")
    print(f"  Total PnL: ${total_pnl:+.2f} ({total_pnl/INITIAL_BALANCE*100:+.1f}%)")

    # Show top/bottom positions
    positions_sorted = sorted(portfolio["positions"], key=lambda p: p.get("unrealized_pnl", 0))
    if len(positions_sorted) > 3:
        print(f"\n  Top 3 positions:")
        for p in positions_sorted[-3:][::-1]:
            print(f"    {p.get('pnl_pct', 0):>+6.1f}% ${p.get('unrealized_pnl', 0):>+6.2f} | {p['question'][:50]}")
        print(f"  Bottom 3 positions:")
        for p in positions_sorted[:3]:
            print(f"    {p.get('pnl_pct', 0):>+6.1f}% ${p.get('unrealized_pnl', 0):>+6.2f} | {p['question'][:50]}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    client = httpx.Client(timeout=20)
    now = datetime.now(timezone.utc)

    print(f"╔{'═'*68}╗")
    print(f"║  Strategy Execution Engine — {now.strftime('%Y-%m-%d %H:%M UTC'):<39}║")
    print(f"║  Mode: PAPER TRADING — ${INITIAL_BALANCE:,.0f} initial balance{' '*21}║")
    print(f"╚{'═'*68}╝")

    portfolio = load_portfolio()
    print(f"\nPortfolio loaded: ${portfolio['cash']:.2f} cash, {len(portfolio['positions'])} positions")

    # Execute strategies
    execute_arb(client, portfolio, max_capital=200)
    save_portfolio(portfolio)

    execute_no_bias(client, portfolio, max_per_trade=20, max_total=300)
    save_portfolio(portfolio)

    # Mark to market
    mark_to_market(client, portfolio)
    save_portfolio(portfolio)

    print(f"\nPortfolio saved to {PORTFOLIO_FILE}")
    client.close()


if __name__ == "__main__":
    main()
