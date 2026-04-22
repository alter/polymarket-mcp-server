#!/usr/bin/env python3
"""
Prediction Engine v6: "Arena Mirror"

Rather than running a slow-polling strategy, this mirrors the top-performing
strategy from the Arena (docker bot, 30-sec polling, 311 strategies).

- Reads arena_results.json from docker volume
- Finds top fee-free strategy with 100+ trades
- Reports its live equity/W-L to prediction_portfolio.json

Also keeps legacy "Structural NO" portfolio as a second tracker:
- check_structural_no_positions(): resolves remaining NO positions via CLOB
- tracks legacy performance until all positions resolve

Usage:
  python prediction_engine.py           # sync mirror + check legacy
  python prediction_engine.py --check   # just check resolutions
  python prediction_engine.py --mirror  # just sync arena mirror
"""
import json, os, sys, time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import httpx

CLOB = "https://clob.polymarket.com"
ARENA_RESULTS = Path("bot-data/arena_results.json")
PF_PATH = "bot-data/prediction_portfolio.json"
MIRROR_PATH = "bot-data/arena_mirror.json"
INITIAL = 1000.0

client = httpx.Client(timeout=20)


# ─── Legacy Structural NO portfolio (resolve-only) ──────────────────────────

def load_legacy():
    if os.path.exists(PF_PATH):
        return json.load(open(PF_PATH))
    return None


def save_legacy(pf):
    os.makedirs(os.path.dirname(PF_PATH), exist_ok=True)
    pf["last_update"] = datetime.now(timezone.utc).isoformat()
    with open(PF_PATH, "w") as f:
        json.dump(pf, f, indent=2)


def check_clob(cid):
    """Returns (is_closed, first_token_won, current_yes_price)."""
    try:
        r = client.get(f"{CLOB}/markets/{cid}")
        if r.status_code != 200:
            return False, None, None
        m = r.json()
        tokens = m.get("tokens", [])
        if not m.get("closed"):
            yp = None
            for t in tokens:
                if t.get("outcome") == "Yes":
                    yp = float(t.get("price", 0))
                    break
            if yp is None and tokens:
                yp = float(tokens[0].get("price", 0))
            return False, None, yp
        if not tokens:
            return True, None, None
        return True, tokens[0].get("winner", False), None
    except Exception:
        return False, None, None


def resolve_legacy(pf):
    """Check open legacy positions via CLOB and resolve."""
    if not pf or not pf.get("positions"):
        return 0
    resolved = 0
    for pos in list(pf["positions"]):
        cid = pos["cid"]
        is_closed, first_won, current_yes = check_clob(cid)
        time.sleep(0.1)

        if is_closed and first_won is not None:
            we_won = not first_won  # we always bet NO
            if we_won:
                pnl = pos["shares"] * 1.0 - pos["cost"]
                pf["total_won"] += 1
            else:
                pnl = -pos["cost"]
                pf["total_lost"] += 1
            pos["resolved"] = True
            pos["won"] = we_won
            pos["pnl"] = round(pnl, 2)
            pos["resolved_at"] = datetime.now(timezone.utc).isoformat()
            pf["cash"] += pos["cost"] + pnl
            pf["total_pnl"] += pnl
            pf["closed"].append(pos)
            pf["positions"].remove(pos)
            resolved += 1
            tag = "WIN" if we_won else "LOSS"
            print(f"  {tag}: NO @ {pos['entry']:.3f} pnl=${pnl:+.2f} | {pos['question'][:55]}")
        elif current_yes is not None:
            cp = 1.0 - current_yes
            pos["current_price"] = round(cp, 4)
            pos["current_value"] = round(pos["shares"] * cp, 2)
            pos["unrealized_pnl"] = round(pos["current_value"] - pos["cost"], 2)
    return resolved


# ─── Arena Mirror ───────────────────────────────────────────────────────────

def load_arena():
    if not ARENA_RESULTS.exists():
        return None
    try:
        return json.load(open(ARENA_RESULTS))
    except Exception as e:
        print(f"Arena load failed: {e}")
        return None


def mirror_arena():
    """Snapshot top arena strategies to arena_mirror.json."""
    arena = load_arena()
    if not arena:
        print("Arena results not available")
        return

    results = arena.get("results", [])
    active = [r for r in results if not r.get("retired")]
    profitable = [r for r in active if r.get("equity", 0) > INITIAL]

    # Top by equity among actives
    top_all = sorted(active, key=lambda r: -r.get("equity", 0))[:20]
    # Top fee-free
    top_free = sorted([r for r in active if r.get("params", {}).get("fee_free_only")],
                      key=lambda r: -r.get("equity", 0))[:10]
    # Top by indicator
    by_indicator = defaultdict(list)
    for r in active:
        ind = r.get("params", {}).get("indicator", "?")
        by_indicator[ind].append(r)
    ind_leaders = {}
    for ind, rs in by_indicator.items():
        best = max(rs, key=lambda r: r.get("equity", 0))
        ind_leaders[ind] = {
            "best_name": best.get("name"),
            "best_equity": best.get("equity"),
            "best_wr": round(best.get("wins", 0) /
                             max(best.get("wins", 0) + best.get("losses", 0), 1) * 100, 1),
            "count": len(rs),
            "profitable_count": sum(1 for r in rs if r.get("equity", 0) > INITIAL),
        }

    mirror = {
        "synced_at": datetime.now(timezone.utc).isoformat(),
        "arena_updated_at": arena.get("updated_at"),
        "total_strategies": arena.get("strategies"),
        "active_strategies": arena.get("active"),
        "profitable": len(profitable),
        "markets_tracked": arena.get("markets_tracked"),
        "top_equity": top_all[0]["equity"] if top_all else INITIAL,
        "top_20_overall": top_all,
        "top_10_fee_free": top_free,
        "indicator_leaders": ind_leaders,
    }

    os.makedirs(os.path.dirname(MIRROR_PATH), exist_ok=True)
    with open(MIRROR_PATH, "w") as f:
        json.dump(mirror, f, indent=2)

    # Print summary
    print(f"=== ARENA STATUS ({arena.get('updated_at', '?')[:19]}) ===")
    print(f"  Strategies: {arena.get('strategies')} total, "
          f"{arena.get('active')} active, {len(profitable)} profitable")
    print(f"  Markets tracked: {arena.get('markets_tracked')}")

    print(f"\n  TOP 10 overall:")
    for r in top_all[:10]:
        total = r.get("wins", 0) + r.get("losses", 0)
        wr = r.get("wins", 0) / max(total, 1) * 100
        print(f"    #{r['id']:<4} {r['name'][:50]:<50} "
              f"${r['equity']:>8.2f}  ({total:>4} trades, {wr:>4.0f}% WR)")

    print(f"\n  Best by indicator:")
    for ind, d in sorted(ind_leaders.items(), key=lambda x: -x[1]["best_equity"]):
        print(f"    {ind:<14} best=${d['best_equity']:>8.2f} "
              f"WR={d['best_wr']:>4.1f}% ({d['profitable_count']}/{d['count']} profitable)")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    mirror_arena()

    pf = load_legacy()
    if pf and pf.get("positions"):
        print(f"\n=== LEGACY STRUCTURAL NO PORTFOLIO ===")
        print(f"Checking {len(pf['positions'])} open positions via CLOB...")
        resolved = resolve_legacy(pf)
        save_legacy(pf)
        wr = pf["total_won"] / max(pf["total_won"] + pf["total_lost"], 1) * 100
        unrealized = sum(p.get("unrealized_pnl", 0) for p in pf["positions"])
        invested = sum(p.get("current_value", p["cost"]) for p in pf["positions"])
        total_val = pf["cash"] + invested
        print(f"Resolved: {resolved}")
        print(f"Record: {pf['total_won']}W / {pf['total_lost']}L ({wr:.0f}%)")
        print(f"Realized PnL: ${pf['total_pnl']:+.2f}")
        print(f"Open: {len(pf['positions'])}, Unrealized: ${unrealized:+.2f}")
        print(f"Total: ${total_val:,.2f} ({total_val/INITIAL*100-100:+.1f}%)")


if __name__ == "__main__":
    if "--check" in sys.argv:
        # Just check legacy resolutions
        pf = load_legacy()
        if pf and pf.get("positions"):
            resolved = resolve_legacy(pf)
            save_legacy(pf)
            print(f"Resolved {resolved} legacy positions")
        else:
            print("No legacy positions")
    elif "--mirror" in sys.argv:
        mirror_arena()
    else:
        main()
