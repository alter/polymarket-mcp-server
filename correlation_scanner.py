#!/usr/bin/env python3
"""
Cross-market correlation scanner for resolved markets.

Strategy: find pairs of markets with similar question text. If they should
correlate (e.g. mutex championship outcomes, related events), check whether
historical prices diverged. Identify divergence-arb candidates.

Output: bot-data/correlation_pairs.json
"""
import json, re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import sys
sys.path.insert(0, ".")
from mass_backtest import load_resolutions, load_ticks


META_FILE = Path("bot-data/gamma_market_meta.json")


def tokenize(s):
    s = re.sub(r"[^\w\s]", " ", s.lower())
    return set(t for t in s.split() if len(t) > 2)


def jaccard(a, b):
    if not a or not b:
        return 0
    return len(a & b) / len(a | b)


def main():
    print(f"[{datetime.now():%H:%M:%S}] Loading...")
    if not META_FILE.exists():
        print(f"  {META_FILE} missing")
        return
    meta = json.load(open(META_FILE))
    res = load_resolutions()

    # Build (mid, q, end) for each closed market with question
    items = []
    for mid, m in meta.items():
        if not m.get("closed"):
            continue
        q = m.get("q", "")
        if not q or len(q) < 10:
            continue
        items.append((str(mid), q, m.get("end", "")))
    print(f"  {len(items)} closed markets with questions")

    # Compute pairwise jaccard, find clusters
    tokens = {mid: tokenize(q) for mid, q, _ in items}
    pairs = []
    for i in range(len(items)):
        m1, q1, _ = items[i]
        for j in range(i+1, len(items)):
            m2, q2, _ = items[j]
            jc = jaccard(tokens[m1], tokens[m2])
            if jc >= 0.5:
                pairs.append({
                    "m1": m1, "m2": m2,
                    "q1": q1[:60], "q2": q2[:60],
                    "jaccard": round(jc, 3),
                })
    pairs.sort(key=lambda p: -p["jaccard"])
    print(f"  Pairs with jaccard>=0.5: {len(pairs)}")

    # For each pair, check if both have ticks AND resolution disagreement.
    # If they're mutex (one resolves YES, other NO) — confirmed mutex
    # If both YES — interesting (could indicate same event split)
    confirmed_mutex = 0
    both_yes = 0
    both_no = 0
    disagreed = 0
    interesting = []
    for p in pairs[:200]:
        m1 = p["m1"]; m2 = p["m2"]
        if m1 not in res or m2 not in res:
            continue
        y1 = res[m1]["yes_won"]
        y2 = res[m2]["yes_won"]
        if y1 and y2:
            both_yes += 1
            interesting.append({**p, "yes1": True, "yes2": True})
        elif not y1 and not y2:
            both_no += 1
        else:
            confirmed_mutex += 1
    print(f"\n  Of top 200 pairs:")
    print(f"    confirmed mutex (one Y, one N): {confirmed_mutex}")
    print(f"    both YES: {both_yes}")
    print(f"    both NO: {both_no}")

    print(f"\n━━━ Top 15 highest-similarity resolved pairs ━━━")
    for p in pairs[:15]:
        m1 = p["m1"]; m2 = p["m2"]
        y1 = res.get(m1, {}).get("yes_won", "?")
        y2 = res.get(m2, {}).get("yes_won", "?")
        print(f"  jc={p['jaccard']:.2f}  Y1={y1} Y2={y2}")
        print(f"    m1={m1}: {p['q1']}")
        print(f"    m2={m2}: {p['q2']}")

    # Save
    out = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "n_pairs": len(pairs),
        "confirmed_mutex": confirmed_mutex,
        "both_yes": both_yes,
        "both_no": both_no,
        "top_pairs": pairs[:50],
        "both_yes_pairs": interesting[:20],
    }
    with open("bot-data/correlation_pairs.json", "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved to bot-data/correlation_pairs.json")


if __name__ == "__main__":
    main()
