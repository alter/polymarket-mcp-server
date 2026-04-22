#!/usr/bin/env python3
"""Stage 4: Validate findings, check for selection bias, deeper diagnostics."""

import json
import statistics
from collections import defaultdict, Counter
from pathlib import Path

DATA_DIR = Path("bot-data/graph_v2")
CACHE_FILE = Path("bot-data/clob_cache.json")

with open("bot-data/classified_markets.json") as f:
    markets = json.load(f)
with open(DATA_DIR / "snapshots.json") as f:
    snapshots = json.load(f)
with open(CACHE_FILE) as f:
    clob_cache = json.load(f)

# ============================================================
# CRITICAL CHECK: Are our snapshots representative?
# ============================================================

print("=" * 70)
print("DATA QUALITY & SELECTION BIAS CHECK")
print("=" * 70)

# What markets have snapshots?
with_snaps = [m for m in markets if m["c"] in snapshots]
without_snaps = [m for m in markets if m["c"] not in snapshots]
print(f"Markets with snapshots: {len(with_snaps)} / {len(markets)}")

# Are snapshot markets biased toward certain types?
print(f"\nMarket type distribution:")
print(f"{'Type':<20s} {'With Snap':>10s} {'Without':>10s} {'Snap %':>8s}")
for t in ["binary_event", "match_winner", "price_target", "over_under", "multi_candidate", "election", "spread"]:
    ws = sum(1 for m in with_snaps if m["type"] == t)
    wo = sum(1 for m in without_snaps if m["type"] == t)
    total = ws + wo
    print(f"  {t:<20s} {ws:>8d} {wo:>10d} {ws/max(total,1)*100:>7.1f}%")

# ============================================================
# CRITICAL: Check the "early_price" vs actual snapshot timeline
# ============================================================

print(f"\n{'='*70}")
print("SNAPSHOT TIMELINE ANALYSIS")
print("=" * 70)

# For resolved markets with snapshots, check:
# 1. How many snapshots before resolution?
# 2. What's the time span?
# 3. Is "early price" actually before meaningful trading?

resolved_with_snaps = []
for m in markets:
    cid = m["c"]
    if cid not in clob_cache or cid not in snapshots:
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
        continue

    snaps = snapshots[cid]
    if len(snaps) < 2:
        continue

    yes_won = winner == "YES"
    first_ts = snaps[0][0]
    last_ts = snaps[-1][0]
    duration_hours = (last_ts - first_ts) / 3600

    resolved_with_snaps.append({
        "cid": cid,
        "question": m["q"],
        "type": m["type"],
        "yes_won": yes_won,
        "n_snaps": len(snaps),
        "duration_hours": duration_hours,
        "first_price": snaps[0][1],
        "last_price": snaps[-1][1],
        "prices": [s[1] for s in snaps],
        "volumes": [s[2] for s in snaps],
        "trades": [s[3] for s in snaps],
        "group": m.get("g"),
    })

print(f"Resolved markets with snapshots: {len(resolved_with_snaps)}")

# Duration stats
durations = [m["duration_hours"] for m in resolved_with_snaps]
print(f"\nSnapshot duration (hours):")
print(f"  Min: {min(durations):.1f}h | Median: {statistics.median(durations):.1f}h | Max: {max(durations):.1f}h")
print(f"  Mean: {statistics.mean(durations):.1f}h")

n_snaps_list = [m["n_snaps"] for m in resolved_with_snaps]
print(f"\nNumber of snapshots per market:")
print(f"  Min: {min(n_snaps_list)} | Median: {statistics.median(n_snaps_list):.0f} | Max: {max(n_snaps_list)}")

# ============================================================
# CHECK: Is the NO bias real or an artifact of multi-outcome markets?
# ============================================================

print(f"\n{'='*70}")
print("NO BIAS DIAGNOSTIC: Multi-outcome vs True Binary")
print("=" * 70)

# In a multi-outcome event (e.g., "Will X win tournament?"), each
# individual market is a longshot. Of N candidates, only 1 wins YES.
# So N-1 markets resolve NO. This INFLATES the NO win rate.
#
# We need to check: are the "binary_event" markets actually independent
# events, or are they part of multi-outcome groups?

# Check how many binary_events have groups
binary_resolved = [m for m in resolved_with_snaps if m["type"] == "binary_event"]
binary_with_group = [m for m in binary_resolved if m["group"]]
binary_no_group = [m for m in binary_resolved if not m["group"]]

print(f"\nBinary events resolved with snapshots: {len(binary_resolved)}")
print(f"  With group: {len(binary_with_group)}")
print(f"  Without group: {len(binary_no_group)}")

# Group sizes for binary events
group_sizes = Counter()
for m in binary_resolved:
    g = m.get("group")
    if g:
        group_sizes[g] += 1

print(f"\nBinary event group sizes:")
for g, c in group_sizes.most_common(10):
    yes_won = sum(1 for m in binary_resolved if m.get("group") == g and m["yes_won"])
    print(f"  {g[:40]:<40s}: {c} markets, {yes_won} YES won")

# NO rate for binary events WITH and WITHOUT groups
if binary_with_group:
    no_rate_grouped = sum(1 for m in binary_with_group if not m["yes_won"]) / len(binary_with_group)
    print(f"\nNO win rate - binary with group: {no_rate_grouped*100:.0f}% (n={len(binary_with_group)})")
if binary_no_group:
    no_rate_ungrouped = sum(1 for m in binary_no_group if not m["yes_won"]) / len(binary_no_group)
    print(f"NO win rate - binary without group: {no_rate_ungrouped*100:.0f}% (n={len(binary_no_group)})")

# ============================================================
# MULTI-CANDIDATE CORRECTION
# ============================================================

print(f"\n{'='*70}")
print("MULTI-CANDIDATE CORRECTION")
print("=" * 70)

# For multi-candidate markets (elections, tournaments), the NO bias
# is STRUCTURAL: if 10 candidates, 9 resolve NO.
# The real question: is there edge in knowing WHICH one wins?

# Group multi_candidate by group
mc_groups = defaultdict(list)
for m in resolved_with_snaps:
    if m["type"] in ("multi_candidate", "election"):
        g = m.get("group", m["cid"])
        mc_groups[g].append(m)

print(f"Multi-candidate groups: {len(mc_groups)}")
for g, ms in sorted(mc_groups.items(), key=lambda x: -len(x[1]))[:10]:
    n = len(ms)
    yes_won = sum(1 for m in ms if m["yes_won"])
    print(f"  {g[:40]:<40s}: {n} candidates, {yes_won} winners")
    # Show which one won
    for m in ms:
        if m["yes_won"]:
            print(f"    WINNER: {m['question'][:60]} (first_price={m['first_price']:.3f})")

# ============================================================
# CORRECTED ANALYSIS: True independent binary events only
# ============================================================

print(f"\n{'='*70}")
print("CORRECTED: TRUE INDEPENDENT BINARY EVENTS")
print("=" * 70)

# Independent = no group, or group with only 1 market
# These are questions like "Will it rain?", "Will the bill pass?", etc.
# NOT "Will Team A win?" (which is part of A vs B)

truly_independent = []
for m in resolved_with_snaps:
    g = m.get("group")
    if m["type"] == "binary_event":
        if not g or group_sizes.get(g, 0) <= 1:
            truly_independent.append(m)

print(f"Truly independent binary events: {len(truly_independent)}")
if truly_independent:
    yes_won = sum(1 for m in truly_independent if m["yes_won"])
    no_won = len(truly_independent) - yes_won
    print(f"  YES won: {yes_won} ({yes_won/len(truly_independent)*100:.0f}%)")
    print(f"  NO won: {no_won} ({no_won/len(truly_independent)*100:.0f}%)")

    # By first_price bins
    bins = [
        ("Fav YES (>60%)", 0.60, 1.00),
        ("Toss-up (40-60%)", 0.40, 0.60),
        ("Fav NO (<40%)", 0.00, 0.40),
    ]

    print(f"\n  {'Bin':<25s} {'N':>5s} {'YES%':>7s} {'Implied':>8s} {'Bias':>8s}")
    for label, lo, hi in bins:
        in_bin = [m for m in truly_independent if lo <= m["first_price"] < hi]
        if len(in_bin) < 2:
            print(f"  {label:<25s} {len(in_bin):>5d} (too few)")
            continue
        yw = sum(1 for m in in_bin if m["yes_won"])
        actual = yw / len(in_bin)
        implied = statistics.mean(m["first_price"] for m in in_bin)
        bias = actual - implied
        print(f"  {label:<25s} {len(in_bin):>5d} {actual*100:>6.0f}% {implied*100:>7.0f}% {bias*100:>+7.1f}pp")

# Show some examples of truly independent binary events
print(f"\nSample truly independent binary events:")
for m in truly_independent[:15]:
    outcome = "YES" if m["yes_won"] else "NO"
    print(f"  [{outcome}] first={m['first_price']:.3f} last={m['last_price']:.3f} | {m['question'][:65]}")

# ============================================================
# THE REAL EDGE: What types of markets are TRULY exploitable?
# ============================================================

print(f"\n{'='*70}")
print("EDGE ANALYSIS: Where does contrarian NO actually work?")
print("=" * 70)

# Simulate on ALL 539 CLOB-resolved markets (not just snapshots)
# Use current local YES price as proxy (understanding it's post-resolution for many)

# Better: use only the 72 mismatches as "traded and lost" evidence
# These are markets where price showed YES=1.0 (strong favorite) but NO won
# This means: even EXTREME favorites can lose

# Count by type how often NO wins when price was between 0.3-0.7 (tradeable range)
# We need snapshot prices for this

print(f"\nNO win rate at different entry prices (snapshot data, all types):")
print(f"{'Entry YES Price':<25s} {'N':>5s} {'NO Won':>7s} {'NO %':>6s} {'E[NO bet]':>10s}")
print("-" * 58)

entry_bins = [
    ("YES 80-99%", 0.80, 0.99),
    ("YES 70-80%", 0.70, 0.80),
    ("YES 60-70%", 0.60, 0.70),
    ("YES 50-60%", 0.50, 0.60),
    ("YES 40-50%", 0.40, 0.50),
    ("YES 30-40%", 0.30, 0.40),
    ("YES 20-30%", 0.20, 0.30),
    ("YES 5-20%", 0.05, 0.20),
]

for label, lo, hi in entry_bins:
    in_bin = [m for m in resolved_with_snaps if lo <= m["first_price"] < hi]
    if len(in_bin) < 2:
        print(f"  {label:<23s} {len(in_bin):>5d} (too few)")
        continue

    no_won = sum(1 for m in in_bin if not m["yes_won"])
    no_rate = no_won / len(in_bin)

    # E[value] of $20 NO bet at entry price
    # Buy NO at (1 - first_price). If NO wins: get $1/token. If YES wins: get $0.
    # Cost per token = 1 - avg_yes_price
    avg_no_price = 1 - statistics.mean(m["first_price"] for m in in_bin)
    ev_per_dollar = no_rate / avg_no_price  # expected value per $1 wagered
    ev_20 = (ev_per_dollar - 1) * 20  # expected profit on $20 bet

    print(f"  {label:<23s} {len(in_bin):>5d} {no_won:>7d} {no_rate*100:>5.0f}% ${ev_20:>+9.1f}")

# ============================================================
# MATCH_WINNER deep dive
# ============================================================

print(f"\n{'='*70}")
print("MATCH WINNER DEEP DIVE")
print("=" * 70)

mw = [m for m in resolved_with_snaps if m["type"] == "match_winner"]
print(f"Match winner markets with snapshots: {len(mw)}")

# In match winner markets, each individual market is "Will Team A win?"
# In a 2-team match, there are 2 markets. One wins YES, one wins NO.
# So the structural NO rate should be 50%.
# If we see > 50% NO, it means underdogs are winning or there are draws.

mw_groups = defaultdict(list)
for m in mw:
    g = m.get("group", m["cid"])
    mw_groups[g].append(m)

print(f"Match winner groups: {len(mw_groups)}")

# For each match, check: did the favorite or underdog win?
fav_wins = 0
dog_wins = 0
draws = 0
matches = 0

for g, ms in mw_groups.items():
    if len(ms) < 2:
        continue
    matches += 1
    # Sort by first_price (highest = favorite)
    ms_sorted = sorted(ms, key=lambda x: -x["first_price"])
    fav = ms_sorted[0]
    dog = ms_sorted[-1]

    if fav["yes_won"]:
        fav_wins += 1
    elif dog["yes_won"]:
        dog_wins += 1
    else:
        draws += 1

    # Show if interesting
    if dog["yes_won"] and dog["first_price"] < 0.30:
        print(f"  UPSET: {dog['question'][:60]} (dog={dog['first_price']:.3f}, fav={fav['first_price']:.3f})")

print(f"\nMatches: {matches} | Fav won: {fav_wins} | Dog won: {dog_wins} | Neither/draw: {draws}")
if matches > 0:
    print(f"Favorite win rate: {fav_wins/matches*100:.0f}%")
    print(f"Underdog win rate: {dog_wins/matches*100:.0f}%")

# Average favorite price
fav_prices = []
for g, ms in mw_groups.items():
    if len(ms) >= 2:
        ms_sorted = sorted(ms, key=lambda x: -x["first_price"])
        fav_prices.append(ms_sorted[0]["first_price"])

if fav_prices:
    avg_fav_price = statistics.mean(fav_prices)
    print(f"Average favorite implied probability: {avg_fav_price*100:.0f}%")
    print(f"If favorites won at implied rate: {avg_fav_price*100:.0f}% vs actual {fav_wins/max(matches,1)*100:.0f}%")
    bias = fav_wins/max(matches,1) - avg_fav_price
    print(f"Favorite bias: {bias*100:+.1f}pp")
