#!/usr/bin/env python3
"""Stage 2: Deep analysis - Favorite/longshot bias, time analysis, volume, groups."""

import json
import asyncio
import aiohttp
from collections import defaultdict, Counter
from pathlib import Path
import statistics

DATA_DIR = Path("bot-data/graph_v2")
CACHE_FILE = Path("bot-data/clob_cache.json")

# Load data
with open("bot-data/classified_markets.json") as f:
    markets = json.load(f)

with open(DATA_DIR / "snapshots.json") as f:
    snapshots = json.load(f)

with open(CACHE_FILE) as f:
    clob_cache = json.load(f)

# Build market lookup
market_by_cid = {m["c"]: m for m in markets}

# ============================================================
# First: fetch MORE resolved markets we need
# ============================================================

# Get all resolved markets that aren't cached yet
resolved = [m for m in markets if m["y"] <= 0.005 or m["y"] >= 0.995]
uncached = [m["c"] for m in resolved if m["c"] not in clob_cache]

async def fetch_clob_data(session, cid, semaphore):
    url = f"https://clob.polymarket.com/markets/{cid}"
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return cid, await resp.json()
                return cid, None
        except:
            return cid, None

async def fetch_batch(cids):
    semaphore = asyncio.Semaphore(2)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_clob_data(session, cid, semaphore) for cid in cids]
        results = {}
        for coro in asyncio.as_completed(tasks):
            cid, data = await coro
            if data:
                results[cid] = data
            await asyncio.sleep(0.5)
        return results

if uncached:
    # Fetch up to 500 more
    batch = uncached[:500]
    print(f"Fetching {len(batch)} more markets from CLOB API...")
    new_data = asyncio.run(fetch_batch(batch))
    clob_cache.update(new_data)
    with open(CACHE_FILE, "w") as f:
        json.dump(clob_cache, f)
    print(f"Fetched {len(new_data)}, total cached: {len(clob_cache)}")

# ============================================================
# Build resolved dataset with CLOB ground truth
# ============================================================

resolved_data = []
for m in markets:
    cid = m["c"]
    if cid not in clob_cache:
        continue
    clob = clob_cache[cid]
    if not clob.get("closed"):
        continue

    tokens = clob.get("tokens", [])
    winner = None
    for t in tokens:
        if t.get("winner"):
            winner = t.get("outcome", "").upper()
            break

    if winner is None:
        # No winner declared yet even though closed
        continue

    yes_won = winner == "YES"
    resolved_data.append({
        "cid": cid,
        "question": m["q"],
        "type": m["type"],
        "yes_price": m["y"],
        "volume": m.get("v", 0),
        "v24": m.get("v24", 0),
        "group": m.get("g"),
        "slug": m.get("s", ""),
        "yes_won": yes_won,
        "start_date": m.get("sd"),
        "end_date": m.get("ed"),
    })

print(f"\nTotal resolved with CLOB ground truth: {len(resolved_data)}")
print(f"  YES won: {sum(1 for r in resolved_data if r['yes_won'])}")
print(f"  NO won:  {sum(1 for r in resolved_data if not r['yes_won'])}")

# ============================================================
# ANALYSIS 1: Investigate mismatches (price vs CLOB)
# ============================================================

print("\n" + "=" * 70)
print("PRICE vs CLOB MISMATCHES")
print("=" * 70)

mismatches = []
for r in resolved_data:
    price_says_yes = r["yes_price"] >= 0.995
    if price_says_yes != r["yes_won"]:
        mismatches.append(r)

print(f"Total mismatches: {len(mismatches)}")
print(f"  Price says YES but NO won: {sum(1 for m in mismatches if m['yes_price'] >= 0.995)}")
print(f"  Price says NO but YES won: {sum(1 for m in mismatches if m['yes_price'] <= 0.005)}")
print("\nSample mismatches (price says YES, NO won):")
for m in mismatches[:5]:
    if m["yes_price"] >= 0.995:
        print(f"  {m['question'][:80]} | price={m['yes_price']:.3f} | YES_won={m['yes_won']} | type={m['type']}")
print("\nSample mismatches (price says NO, YES won):")
for m in mismatches:
    if m["yes_price"] <= 0.005:
        print(f"  {m['question'][:80]} | price={m['yes_price']:.4f} | YES_won={m['yes_won']} | type={m['type']}")
        if sum(1 for mm in mismatches if mm["yes_price"] <= 0.005 and mm is not m) < 5:
            continue
        break

# ============================================================
# ANALYSIS 2: Favorite-Longshot Bias
# ============================================================

print("\n" + "=" * 70)
print("FAVORITE-LONGSHOT BIAS ANALYSIS")
print("=" * 70)

# We need markets where we know the ORIGINAL price before resolution
# Current yes_price is post-resolution. We need snapshot data.
# For markets without snapshots, the current price IS the resolved price.
# So we should use snapshot data for the "pre-resolution" price.

# For now, let's look at markets with snapshots
markets_with_snapshots = []
for r in resolved_data:
    if r["cid"] in snapshots:
        snaps = snapshots[r["cid"]]
        if len(snaps) >= 2:
            # Get price from first snapshot (earliest available)
            # and from before the last few snapshots
            early_prices = [s[1] for s in snaps[:3]]
            early_price = statistics.mean(early_prices)

            # Price 1-2 snapshots before the end
            if len(snaps) >= 3:
                pre_resolution_prices = [s[1] for s in snaps[-4:-1]] if len(snaps) >= 4 else [s[1] for s in snaps[:-1]]
                pre_res_price = statistics.mean(pre_resolution_prices)
            else:
                pre_res_price = snaps[0][1]

            r["early_price"] = early_price
            r["pre_res_price"] = pre_res_price
            r["snapshots"] = snaps
            markets_with_snapshots.append(r)

print(f"Markets with snapshot data: {len(markets_with_snapshots)}")

# Bin by early price
bins = [
    ("Strong YES (>0.70)", 0.70, 1.01),
    ("Lean YES (0.55-0.70)", 0.55, 0.70),
    ("Coin flip (0.40-0.55)", 0.40, 0.55),
    ("Lean NO (0.30-0.40)", 0.30, 0.40),
    ("Longshot (<0.30)", 0.00, 0.30),
]

print(f"\n{'Price Range':<25s} {'Count':>6s} {'YES Won':>8s} {'YES %':>7s} {'Impl.Prob':>10s} {'Bias':>10s}")
print("-" * 70)

for label, lo, hi in bins:
    in_bin = [m for m in markets_with_snapshots if lo <= m["early_price"] < hi]
    if not in_bin:
        print(f"{label:<25s} {'0':>6s}")
        continue
    yes_won = sum(1 for m in in_bin if m["yes_won"])
    actual_rate = yes_won / len(in_bin)
    implied_prob = statistics.mean(m["early_price"] for m in in_bin)
    bias = actual_rate - implied_prob
    print(f"{label:<25s} {len(in_bin):>6d} {yes_won:>8d} {actual_rate*100:>6.1f}% {implied_prob*100:>9.1f}% {bias*100:>+9.1f}pp")

# Same analysis by market TYPE
print(f"\nFavorite-Longshot Bias by Market Type:")
print(f"{'Type':<20s} {'Count':>6s} {'Fav YES%':>9s} {'Fav Impl':>9s} {'Fav Bias':>9s} | {'Long YES%':>10s} {'Long Impl':>10s} {'Long Bias':>10s}")
print("-" * 100)

for mtype in ["binary_event", "match_winner", "price_target", "over_under", "multi_candidate", "election", "spread"]:
    typed = [m for m in markets_with_snapshots if m["type"] == mtype]
    favs = [m for m in typed if m["early_price"] >= 0.60]
    longs = [m for m in typed if m["early_price"] <= 0.40]

    fav_str = ""
    long_str = ""

    if len(favs) >= 3:
        fav_yes = sum(1 for m in favs if m["yes_won"]) / len(favs)
        fav_impl = statistics.mean(m["early_price"] for m in favs)
        fav_bias = fav_yes - fav_impl
        fav_str = f"{fav_yes*100:>8.0f}% {fav_impl*100:>8.0f}% {fav_bias*100:>+8.1f}pp"
    else:
        fav_str = f"{'n/a (n<3)':>28s}"

    if len(longs) >= 3:
        long_yes = sum(1 for m in longs if m["yes_won"]) / len(longs)
        long_impl = statistics.mean(m["early_price"] for m in longs)
        long_bias = long_yes - long_impl
        long_str = f"{long_yes*100:>9.0f}% {long_impl*100:>9.0f}% {long_bias*100:>+9.1f}pp"
    else:
        long_str = f"{'n/a (n<3)':>30s}"

    print(f"{mtype:<20s} {len(typed):>6d} {fav_str} | {long_str}")

# ============================================================
# ANALYSIS 3: Time-to-Resolution Analysis
# ============================================================

print("\n" + "=" * 70)
print("TIME-TO-RESOLUTION ANALYSIS")
print("=" * 70)

# Look at price trajectory in final hours before resolution
hours_before = [1, 2, 4, 8, 12, 24]

print(f"\nPrice convergence before resolution:")
print(f"{'Hours Before':>14s} {'Markets':>8s} {'Avg Price (YES won)':>20s} {'Avg Price (NO won)':>20s} {'Spread':>8s}")
print("-" * 75)

for m in markets_with_snapshots:
    snaps = m["snapshots"]
    if len(snaps) < 2:
        continue
    # Last snapshot timestamp
    last_ts = snaps[-1][0]
    m["price_at_hour"] = {}
    for s in snaps:
        hours_from_end = (last_ts - s[0]) / 3600
        for h in hours_before:
            if abs(hours_from_end - h) < 1.5:  # within 1.5 hours
                if h not in m["price_at_hour"]:
                    m["price_at_hour"][h] = s[1]

for h in hours_before:
    yes_prices = [m["price_at_hour"][h] for m in markets_with_snapshots if h in m.get("price_at_hour", {})]
    no_prices_yes = [m["price_at_hour"][h] for m in markets_with_snapshots if h in m.get("price_at_hour", {}) and m["yes_won"]]
    no_prices_no = [m["price_at_hour"][h] for m in markets_with_snapshots if h in m.get("price_at_hour", {}) and not m["yes_won"]]

    if no_prices_yes and no_prices_no:
        avg_yes = statistics.mean(no_prices_yes)
        avg_no = statistics.mean(no_prices_no)
        spread = avg_yes - avg_no
        print(f"{h:>12d}h {len(yes_prices):>8d} {avg_yes:>19.3f} {avg_no:>19.3f} {spread:>+7.3f}")

# Last-minute drift detection
print(f"\nLast-minute drift (price change in final snapshot):")
drift_data = []
for m in markets_with_snapshots:
    snaps = m["snapshots"]
    if len(snaps) < 3:
        continue
    final_price = snaps[-1][1]
    prev_price = snaps[-2][1]
    drift = final_price - prev_price
    drift_data.append({
        "drift": drift,
        "abs_drift": abs(drift),
        "yes_won": m["yes_won"],
        "type": m["type"],
        "question": m["question"],
    })

if drift_data:
    avg_abs_drift = statistics.mean(d["abs_drift"] for d in drift_data)
    big_drifts = [d for d in drift_data if d["abs_drift"] > 0.10]
    print(f"  Average absolute drift (last snapshot): {avg_abs_drift:.4f}")
    print(f"  Markets with >10% drift in last snapshot: {len(big_drifts)}/{len(drift_data)}")

    if big_drifts:
        print(f"\n  Big drifts (>10% change in last snapshot):")
        for d in sorted(big_drifts, key=lambda x: -x["abs_drift"])[:10]:
            direction = "UP" if d["drift"] > 0 else "DOWN"
            outcome = "YES won" if d["yes_won"] else "NO won"
            print(f"    {direction:>4s} {d['drift']:>+.3f} | {outcome} | {d['type']:15s} | {d['question'][:60]}")

# ============================================================
# ANALYSIS 4: Volume-Outcome Correlation
# ============================================================

print("\n" + "=" * 70)
print("VOLUME-OUTCOME CORRELATION")
print("=" * 70)

vol_data = [r for r in resolved_data if r["volume"] > 0]
vol_data.sort(key=lambda x: x["volume"])

# Quartiles
n = len(vol_data)
quartiles = [
    ("Q1 (lowest vol)", vol_data[:n//4]),
    ("Q2", vol_data[n//4:n//2]),
    ("Q3", vol_data[n//2:3*n//4]),
    ("Q4 (highest vol)", vol_data[3*n//4:]),
]

print(f"\n{'Quartile':<20s} {'Count':>6s} {'YES Won':>8s} {'YES %':>7s} {'Avg Volume':>15s}")
print("-" * 60)

for label, q in quartiles:
    if not q:
        continue
    yes = sum(1 for m in q if m["yes_won"])
    avg_vol = statistics.mean(m["volume"] for m in q)
    print(f"{label:<20s} {len(q):>6d} {yes:>8d} {yes/len(q)*100:>6.1f}% ${avg_vol:>13,.0f}")

# Volume spike before resolution
print(f"\nVolume spike before resolution (using snapshot volume data):")
vol_spike_data = []
for m in markets_with_snapshots:
    snaps = m["snapshots"]
    if len(snaps) < 4:
        continue
    # Compare last 2 snapshot volumes vs earlier average
    early_vols = [s[2] for s in snaps[:len(snaps)//2] if s[2] > 0]
    late_vols = [s[2] for s in snaps[-3:] if s[2] > 0]
    if early_vols and late_vols:
        early_avg = statistics.mean(early_vols)
        late_avg = statistics.mean(late_vols)
        if early_avg > 0:
            spike_ratio = late_avg / early_avg
            vol_spike_data.append({
                "ratio": spike_ratio,
                "yes_won": m["yes_won"],
                "type": m["type"],
            })

if vol_spike_data:
    avg_spike = statistics.mean(d["ratio"] for d in vol_spike_data)
    median_spike = statistics.median(d["ratio"] for d in vol_spike_data)
    print(f"  Average volume spike ratio (late/early): {avg_spike:.2f}x")
    print(f"  Median volume spike ratio: {median_spike:.2f}x")
    big_spikes = [d for d in vol_spike_data if d["ratio"] > 3.0]
    print(f"  Markets with >3x volume spike: {len(big_spikes)}/{len(vol_spike_data)}")

# ============================================================
# ANALYSIS 5: Group/Event Analysis (Multi-candidate overpricing)
# ============================================================

print("\n" + "=" * 70)
print("GROUP/EVENT ANALYSIS (Multi-candidate overpricing)")
print("=" * 70)

# Group markets by group_slug
groups = defaultdict(list)
for m in markets:
    g = m.get("g")
    if g:
        groups[g].append(m)

# Only groups with 2+ markets
multi_groups = {g: ms for g, ms in groups.items() if len(ms) >= 2}
print(f"Groups with 2+ markets: {len(multi_groups)}")

# Calculate sum of YES prices per group
overpriced = []
underpriced = []
fair = []

print(f"\n{'Sum Range':<20s} {'Count':>6s} {'Avg Sum':>8s} {'Avg N Markets':>14s}")
print("-" * 52)

sums = []
for g, ms in multi_groups.items():
    yes_sum = sum(m["y"] for m in ms)
    n_markets = len(ms)
    # Only count non-resolved for meaningful sum
    active = [m for m in ms if 0.005 < m["y"] < 0.995]
    if len(active) < 2:
        continue
    active_sum = sum(m["y"] for m in active)
    sums.append({
        "group": g,
        "sum": active_sum,
        "n": len(active),
        "markets": active,
        "total_sum_incl_resolved": yes_sum,
        "n_total": n_markets,
    })

if sums:
    over = [s for s in sums if s["sum"] > 1.05]
    under = [s for s in sums if s["sum"] < 0.95]
    fair_s = [s for s in sums if 0.95 <= s["sum"] <= 1.05]

    print(f"{'Overpriced (>1.05)':<20s} {len(over):>6d} {statistics.mean(s['sum'] for s in over) if over else 0:>7.3f} {statistics.mean(s['n'] for s in over) if over else 0:>13.1f}")
    print(f"{'Fair (0.95-1.05)':<20s} {len(fair_s):>6d} {statistics.mean(s['sum'] for s in fair_s) if fair_s else 0:>7.3f} {statistics.mean(s['n'] for s in fair_s) if fair_s else 0:>13.1f}")
    print(f"{'Underpriced (<0.95)':<20s} {len(under):>6d} {statistics.mean(s['sum'] for s in under) if under else 0:>7.3f} {statistics.mean(s['n'] for s in under) if under else 0:>13.1f}")

    print(f"\nMost overpriced groups (potential arb: sell YES on all):")
    over_sorted = sorted(sums, key=lambda x: -x["sum"])
    for s in over_sorted[:15]:
        vig = (s["sum"] - 1.0) * 100
        print(f"  {s['group'][:45]:<45s} sum={s['sum']:.3f} ({s['n']} mkts) vig={vig:>+.1f}%")
        # Show individual markets
        for mm in sorted(s["markets"], key=lambda x: -x["y"])[:5]:
            print(f"      {mm['q'][:60]:<60s} YES={mm['y']:.3f}")

    print(f"\nMost underpriced groups (potential arb: buy YES on all):")
    under_sorted = sorted(sums, key=lambda x: x["sum"])
    for s in under_sorted[:10]:
        discount = (1.0 - s["sum"]) * 100
        print(f"  {s['group'][:45]:<45s} sum={s['sum']:.3f} ({s['n']} mkts) discount={discount:>+.1f}%")

# ============================================================
# ANALYSIS 6: Contrarian NO - Corrected Strategy
# ============================================================

print("\n" + "=" * 70)
print("CORRARIAN NO STRATEGY - BY MARKET TYPE")
print("=" * 70)

print(f"\nIf we bet $20 NO on every resolved market, by type:")
print(f"{'Type':<20s} {'N':>5s} {'NO Won':>7s} {'NO %':>6s} {'PnL':>10s} {'PnL/trade':>10s}")
print("-" * 63)

total_pnl = 0
total_trades = 0

for mtype in ["binary_event", "match_winner", "price_target", "over_under", "multi_candidate", "election", "spread"]:
    typed = [r for r in resolved_data if r["type"] == mtype]
    if not typed:
        continue

    no_won = sum(1 for r in typed if not r["yes_won"])
    yes_won = sum(1 for r in typed if r["yes_won"])

    # PnL for $20 NO bet: if NO wins, profit = $20 * (1 - NO_price) / NO_price...
    # Simplified: $20 bet at market NO price. If NO wins, get $20 back. If YES wins, lose $20.
    # Actually: bet $20 at NO price. Cost = $20. Payout if NO wins = $20/no_price.
    # But we don't have the original NO price for each trade.
    # Simple model: we risk $20 per trade. Win $20 if NO wins, lose $20 if YES wins.
    pnl = (no_won - yes_won) * 20
    total_pnl += pnl
    total_trades += len(typed)

    print(f"{mtype:<20s} {len(typed):>5d} {no_won:>7d} {no_won/len(typed)*100:>5.0f}% ${pnl:>+9,d} ${pnl/len(typed):>+9.1f}")

print(f"{'TOTAL':<20s} {total_trades:>5d} {'-':>7s} {'-':>6s} ${total_pnl:>+9,d} ${total_pnl/max(total_trades,1):>+9.1f}")

print(f"\nKEY INSIGHT: Binary events have {sum(1 for r in resolved_data if r['type']=='binary_event' and not r['yes_won'])}/{sum(1 for r in resolved_data if r['type']=='binary_event')} NO wins.")
print(f"Match winners have {sum(1 for r in resolved_data if r['type']=='match_winner' and not r['yes_won'])}/{sum(1 for r in resolved_data if r['type']=='match_winner')} NO wins.")

# Strategy: ONLY bet NO on binary_event type
binary = [r for r in resolved_data if r["type"] == "binary_event"]
if binary:
    no_won = sum(1 for r in binary if not r["yes_won"])
    pnl = (no_won - (len(binary) - no_won)) * 20
    print(f"\nBinary-only NO strategy: {no_won}/{len(binary)} wins ({no_won/len(binary)*100:.0f}%), PnL = ${pnl:+,d} on {len(binary)} trades")

# Strategy: Bet NO on binary_event + election only
be_elec = [r for r in resolved_data if r["type"] in ("binary_event", "election")]
if be_elec:
    no_won = sum(1 for r in be_elec if not r["yes_won"])
    pnl = (no_won - (len(be_elec) - no_won)) * 20
    print(f"Binary+Election NO strategy: {no_won}/{len(be_elec)} wins ({no_won/len(be_elec)*100:.0f}%), PnL = ${pnl:+,d} on {len(be_elec)} trades")

# Exclude match_winner and over_under from NO strategy
exclude = {"match_winner", "over_under", "spread"}
filtered = [r for r in resolved_data if r["type"] not in exclude]
if filtered:
    no_won = sum(1 for r in filtered if not r["yes_won"])
    pnl = (no_won - (len(filtered) - no_won)) * 20
    print(f"Exclude sports NO strategy: {no_won}/{len(filtered)} wins ({no_won/len(filtered)*100:.0f}%), PnL = ${pnl:+,d} on {len(filtered)} trades")

print("\n" + "=" * 70)
print("SUMMARY OF FINDINGS")
print("=" * 70)
