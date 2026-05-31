#!/usr/bin/env python3
"""
Multi-candidate arbitrage scanner.

For mutually-exclusive events (only ONE candidate can win):
  Sum of YES prices SHOULD = 1.0 (in efficient market)
  If sum > 1.0: SELL all YES (or BUY NO) → guaranteed profit
  If sum < 1.0: BUY all YES → guaranteed profit IF list is complete

Examples:
  - "Will [name] win the 2028 US Presidential Election?" — many candidates
  - "Will [name] win the 2026 Masters?" — golf tournament
  - "Will [team] win the 2025-26 NBA Finals?" — basketball

Algorithm:
  1. Fetch active markets
  2. Group by event (e.g., "win 2028 US Presidential Election")
  3. Sum YES prices per group
  4. Flag groups where sum significantly differs from 1.0
"""
import asyncio, json, os, re
from collections import defaultdict
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
DATA = "bot-data"
RESULTS_FILE = os.path.join(DATA, "multi_candidate_arb.json")


# ─── Event extraction ───────────────────────────────────────────────────────

EVENT_PATTERNS = [
    # "Will <X> win the 2028 US Presidential Election" → event = "win 2028 US Presidential Election"
    (r'will\s+.+?\s+win\s+(.+?)\??$', "win"),
    (r'will\s+.+?\s+(elected|nominated)\s+(.+?)\??$', "elected"),
    (r'will\s+(.+?)\s+be\s+the\s+next\s+(.+?)\??$', "next"),
]


def extract_event(q):
    """Return canonical event string if question is multi-candidate, else None."""
    ql = q.lower().strip().rstrip('?')
    for pattern, _kind in EVENT_PATTERNS:
        m = re.match(pattern, ql)
        if m:
            event = m.groups()[-1].strip()  # last captured group
            return event
    return None


def get_yes_price(mkt):
    op = mkt.get("outcomePrices", "")
    if isinstance(op, str):
        try: op = json.loads(op)
        except: return None
    if op and len(op) >= 1:
        try: return float(op[0])
        except: pass
    return None


# ─── Scanner ────────────────────────────────────────────────────────────────

async def fetch_active_markets(client, total=1500):
    markets = []
    for offset in range(0, total, 100):
        try:
            r = await client.get(f"{GAMMA}/markets", params={
                "active": "true", "closed": "false",
                "limit": 100, "offset": offset,
                "order": "volume24hr", "ascending": "false",
            })
            if r.status_code != 200:
                break
            batch = r.json()
            if not batch:
                break
            markets.extend(batch)
        except Exception as e:
            print(f"  fetch err: {e}")
            break
    return markets


def find_groups(markets):
    by_event = defaultdict(list)
    for m in markets:
        q = m.get("question", "")
        event = extract_event(q)
        if not event:
            continue
        # Discard if it's a single binary (not "X" candidate but generic event)
        # Heuristic: event must be specific enough (e.g., "win 2028 US Presidential Election")
        if len(event) < 10:
            continue
        yp = get_yes_price(m)
        if yp is None or yp <= 0.001 or yp >= 0.999:
            continue
        cid = m.get("conditionId", "")
        by_event[event].append({
            "cid": cid, "q": q, "yes_p": yp,
            "fees_on": m.get("feesEnabled", False),
            "vol24": float(m.get("volume24hr", 0) or 0),
            "end": m.get("endDate", ""),
        })
    # Filter: groups with ≥ 2 candidates
    return {ev: ms for ev, ms in by_event.items() if len(ms) >= 2}


async def main():
    os.makedirs(DATA, exist_ok=True)
    client = httpx.AsyncClient(timeout=30.0)
    print(f"[{datetime.now():%H:%M:%S}] Fetching markets...")
    markets = await fetch_active_markets(client, total=1500)
    print(f"  Fetched {len(markets)} markets")

    groups = find_groups(markets)
    print(f"  Found {len(groups)} multi-candidate event groups")

    # For each group, compute sum YES
    summaries = []
    arb_candidates = []
    for event, ms in groups.items():
        n = len(ms)
        sum_yes = sum(m["yes_p"] for m in ms)
        # Edge: how far from 1.0 (within reason)
        edge_over = sum_yes - 1.0
        summaries.append({
            "event": event, "n_candidates": n,
            "sum_yes": round(sum_yes, 4),
            "edge_over_1": round(edge_over, 4),
            "candidates": sorted(ms, key=lambda m: -m["yes_p"]),
        })

    # Sort by absolute distance from 1.0
    summaries.sort(key=lambda s: -abs(s["edge_over_1"]))

    # Cherry-pick: large groups (≥ 5) where sum > 1.05 (real arb if list complete)
    arb_over = [s for s in summaries if s["edge_over_1"] > 0.05 and s["n_candidates"] >= 5]
    arb_under = [s for s in summaries if s["edge_over_1"] < -0.05 and s["n_candidates"] >= 5]

    output = {
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "n_markets_scanned": len(markets),
        "n_event_groups": len(groups),
        "n_arb_over_1": len(arb_over),
        "n_arb_under_1": len(arb_under),
        "all_groups": summaries,
    }
    with open(RESULTS_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"  Saved to {RESULTS_FILE}")

    # Display
    print(f"\n━━━ MULTI-CANDIDATE GROUPS BY |sum-1| ━━━")
    print(f"{'sum':>6} {'edge':>8} {'N':>4}  Event")
    print("─" * 100)
    for s in summaries[:25]:
        print(f"{s['sum_yes']:>5.3f}  {s['edge_over_1']:>+5.3f}  {s['n_candidates']:>3}   {s['event'][:80]}")

    if arb_over:
        print(f"\n━━━ ARBITRAGE: SUM > 1.05 (sell all YES) ━━━")
        for s in arb_over:
            print(f"\n{s['event'][:80]}")
            print(f"  sum_yes = {s['sum_yes']:.3f} (edge {s['edge_over_1']:+.3f}), {s['n_candidates']} candidates")
            for c in s["candidates"][:10]:
                print(f"    p={c['yes_p']:.3f}  fees={c['fees_on']}  vol24=${c['vol24']:>6,.0f}  | {c['q'][:55]}")
            # Approximate profit: if you SELL 1 share YES on each candidate at total sum_yes,
            # exactly 1 of them resolves YES (win 1 share). Net: collect $sum_yes, pay $1, profit = sum-1
            print(f"  → If list complete: sell all → cost $0, payout $1, collected $sum = "
                  f"profit ${s['edge_over_1']:.3f} per share-set")

    if arb_under:
        print(f"\n━━━ POTENTIAL ARB: SUM < 0.95 (buy all YES) ━━━")
        print(f"  ⚠ ONLY arbitrage if candidate list is COMPLETE!")
        print(f"  ⚠ If 'other' / unlisted winner possible → NOT arbitrage (Nobel Prize trap)")
        for s in arb_under[:10]:
            print(f"\n{s['event'][:80]}")
            print(f"  sum_yes = {s['sum_yes']:.3f} (edge {s['edge_over_1']:+.3f}), {s['n_candidates']} candidates")
            for c in s["candidates"][:5]:
                print(f"    p={c['yes_p']:.3f}  fees={c['fees_on']}  | {c['q'][:55]}")

    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
