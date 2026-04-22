#!/usr/bin/env python3
"""Comprehensive Polymarket analysis - Stage 1: Classification + CLOB resolution data."""

import json
import re
import time
import asyncio
import aiohttp
from collections import defaultdict, Counter
from pathlib import Path

DATA_DIR = Path("bot-data/graph_v2")
CACHE_FILE = Path("bot-data/clob_cache.json")

# Load data
with open(DATA_DIR / "markets.json") as f:
    markets = json.load(f)

with open(DATA_DIR / "snapshots.json") as f:
    snapshots = json.load(f)

# ============================================================
# PART 1: Market Structure Taxonomy
# ============================================================

def classify_market(m):
    q = m["q"].lower()
    s = m.get("s", "").lower()

    # Sports match winner: "Will [Team] win" with slug containing team abbreviations
    sports_patterns = [
        r"will .+ (win|beat) .+ on \d{4}",
        r"will .+ win on \d{4}-\d{2}-\d{2}",
        r"will .+ win .+ (game|match|series)",
        r"(nba|nfl|mlb|nhl|epl|la-liga|serie-a|bundesliga|ligue-1|ucl|mls|ufc|atp|wta)",
    ]

    over_under_patterns = [
        r"(over|under|o/u)\s*\d",
        r"(total|combined).*(over|under)",
        r"\d+\.5\s*(points|goals|runs|yards)",
    ]

    spread_patterns = [
        r"[-+]\d+\.5",
        r"spread",
        r"\([-+]\d+",
    ]

    price_target_patterns = [
        r"(bitcoin|btc|eth|ethereum|solana|sol|crypto|xrp).*(above|below|over|under|reach|\$\d)",
        r"(above|below|over|under|reach)\s*\$[\d,]+",
        r"\$\d+[,\d]*\s*(by|before|on)",
    ]

    election_patterns = [
        r"will .+ win .*(election|primary|nomination|presidency|governor|mayor|senate|congress)",
        r"(president|prime minister|chancellor) of",
        r"(next|win).*(president|pm|prime minister)",
    ]

    multi_candidate_patterns = [
        r"will .+ win .*(tournament|championship|cup|award|oscar|grammy|emmy|super bowl|world series)",
    ]

    # Check slug for sports leagues
    sports_slugs = ["nba", "nfl", "mlb", "nhl", "epl", "la-liga", "serie-a", "bundesliga",
                     "ligue-1", "ucl", "mls", "ufc", "atp", "wta", "wnba", "liga-mx",
                     "premier-league", "champions-league", "f1-", "nascar"]

    is_sports_slug = any(sl in s for sl in sports_slugs)

    # Spread
    for p in spread_patterns:
        if re.search(p, q) or re.search(p, s):
            return "spread"

    # Over/Under
    for p in over_under_patterns:
        if re.search(p, q) or re.search(p, s):
            return "over_under"

    # Sports match winner
    if is_sports_slug and ("win" in q or "beat" in q):
        return "match_winner"
    for p in sports_patterns:
        if re.search(p, q) or re.search(p, s):
            return "match_winner"

    # Price target (crypto/stocks)
    for p in price_target_patterns:
        if re.search(p, q) or re.search(p, s):
            return "price_target"

    # Election / political
    for p in election_patterns:
        if re.search(p, q) or re.search(p, s):
            return "election"

    # Multi-candidate (tournaments, awards)
    for p in multi_candidate_patterns:
        if re.search(p, q) or re.search(p, s):
            return "multi_candidate"

    # If it has a group and looks like "Will X win/happen"
    if m.get("g") and "win" in q:
        # Could be sports or competition
        if is_sports_slug:
            return "match_winner"
        return "multi_candidate"

    # Default: binary event
    return "binary_event"


# Classify all markets
for m in markets:
    m["type"] = classify_market(m)

type_counts = Counter(m["type"] for m in markets)
print("=" * 70)
print("MARKET STRUCTURE TAXONOMY")
print("=" * 70)
for t, c in type_counts.most_common():
    print(f"  {t:20s}: {c:5d} ({c/len(markets)*100:.1f}%)")
print(f"  {'TOTAL':20s}: {len(markets):5d}")

# Resolved markets by type
resolved = [m for m in markets if m["y"] <= 0.005 or m["y"] >= 0.995]
print(f"\nResolved markets (price near 0 or 1): {len(resolved)}")
for t in type_counts:
    r = [m for m in resolved if m["type"] == t]
    yes_won = [m for m in r if m["y"] >= 0.995]
    no_won = [m for m in r if m["y"] <= 0.005]
    if r:
        print(f"  {t:20s}: {len(r):4d} resolved | YES won: {len(yes_won):4d} ({len(yes_won)/len(r)*100:.0f}%) | NO won: {len(no_won):4d} ({len(no_won)/len(r)*100:.0f}%)")

# ============================================================
# PART 2: Fetch CLOB resolution data for resolved markets
# ============================================================

async def fetch_clob_data(session, cid, semaphore):
    """Fetch market data from CLOB API."""
    url = f"https://clob.polymarket.com/markets/{cid}"
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return cid, data
                else:
                    return cid, None
        except Exception as e:
            return cid, None

async def fetch_batch(cids):
    """Fetch CLOB data for a batch of condition IDs."""
    semaphore = asyncio.Semaphore(2)  # ~2 req/sec
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_clob_data(session, cid, semaphore) for cid in cids]
        results = {}
        for coro in asyncio.as_completed(tasks):
            cid, data = await coro
            if data:
                results[cid] = data
            # Rate limit
            await asyncio.sleep(0.5)
        return results

# Load cache or fetch
if CACHE_FILE.exists():
    with open(CACHE_FILE) as f:
        clob_cache = json.load(f)
    print(f"\nLoaded {len(clob_cache)} cached CLOB results")
else:
    clob_cache = {}

# We want at least 200 resolved markets from CLOB
needed_cids = []
for m in resolved:
    if m["c"] not in clob_cache:
        needed_cids.append(m["c"])
    if len(clob_cache) + len(needed_cids) >= 300:
        break

if needed_cids:
    print(f"\nFetching {len(needed_cids)} markets from CLOB API (rate limited ~2/sec)...")
    new_data = asyncio.run(fetch_batch(needed_cids))
    clob_cache.update(new_data)
    with open(CACHE_FILE, "w") as f:
        json.dump(clob_cache, f)
    print(f"Fetched {len(new_data)} new, total cached: {len(clob_cache)}")
else:
    print(f"Cache sufficient: {len(clob_cache)} entries")

# ============================================================
# PART 3: Validate resolution from CLOB vs price signal
# ============================================================

print("\n" + "=" * 70)
print("CLOB RESOLUTION VALIDATION")
print("=" * 70)

clob_resolved = 0
clob_yes_won = 0
clob_no_won = 0
mismatches = 0

for m in resolved:
    cid = m["c"]
    if cid not in clob_cache:
        continue
    clob = clob_cache[cid]
    if not clob.get("closed"):
        continue

    clob_resolved += 1
    tokens = clob.get("tokens", [])
    winner = None
    for t in tokens:
        if t.get("winner"):
            winner = t.get("outcome", "").upper()
            break

    price_says_yes = m["y"] >= 0.995
    clob_says_yes = winner == "YES"

    if winner:
        if clob_says_yes:
            clob_yes_won += 1
        else:
            clob_no_won += 1

        if price_says_yes != clob_says_yes:
            mismatches += 1

print(f"CLOB confirmed resolved: {clob_resolved}")
print(f"  YES won: {clob_yes_won} ({clob_yes_won/max(clob_resolved,1)*100:.0f}%)")
print(f"  NO won:  {clob_no_won} ({clob_no_won/max(clob_resolved,1)*100:.0f}%)")
print(f"  Price/CLOB mismatches: {mismatches}")

# Save classified markets for next stages
with open("bot-data/classified_markets.json", "w") as f:
    json.dump(markets, f)

print("\nStage 1 complete. Classified markets saved.")
