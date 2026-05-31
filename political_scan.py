#!/usr/bin/env python3
"""
Scan Polymarket for political markets (last 3 months).

Strategy:
- Query gamma API by political tags: politics, elections, trump, etc.
- Also keyword scan over active+recently-closed markets.
- Filter to created/closed within last 90 days.
- Output: bot-data/political_markets.json with full metadata.
"""
import json, time, re
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import httpx

GAMMA = "https://gamma-api.polymarket.com"

POLITICAL_TAGS = [
    "politics", "elections", "election", "trump", "biden", "harris",
    "us-elections", "world-elections", "geopolitics",
    "congress", "senate", "house", "supreme-court",
    "president", "presidential", "republican", "democrat",
    "europe-politics", "europe-elections", "uk-politics",
    "russia", "ukraine", "china", "israel", "iran",
]

POLITICAL_KEYWORDS = [
    "president", "senate", "congress", "election", "vote", "ballot",
    "trump", "biden", "harris", "vance", "musk", "putin", "xi",
    "supreme court", "scotus", "impeachment", "indict", "treaty",
    "ceasefire", "war", "invade", "russia", "ukraine", "iran",
    "israel", "palestine", "gaza", "hamas", "hezbollah",
    "republican", "democrat", "gop", "dem ",
    "tariff", "sanction", "executive order",
    "prime minister", "chancellor", "parliament",
    "nato", "un security",
]


def is_political(question, slug=""):
    q = (question + " " + slug).lower()
    return any(kw in q for kw in POLITICAL_KEYWORDS)


async def fetch_pages(client, params_base, max_pages=50):
    """Generic gamma /markets pagination."""
    all_items = []
    for page in range(max_pages):
        params = {**params_base, "limit": 100, "offset": page * 100}
        try:
            r = await client.get(f"{GAMMA}/markets", params=params, timeout=15.0)
            if r.status_code != 200:
                break
            batch = r.json()
        except Exception:
            break
        if not batch:
            break
        all_items.extend(batch)
        if len(batch) < 100:
            break
    return all_items


async def main():
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
    print(f"[{datetime.now():%H:%M:%S}] Scanning markets from {cutoff_date.date()}...")

    by_id = {}  # mid → market dict
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. Tag-based scan
        for tag in POLITICAL_TAGS:
            try:
                items = await fetch_pages(client, {"tag_slug": tag, "closed": "false"}, max_pages=10)
                items += await fetch_pages(client, {"tag_slug": tag, "closed": "true"}, max_pages=10)
                added = 0
                for m in items:
                    mid = str(m.get("id", ""))
                    if not mid or mid in by_id:
                        continue
                    by_id[mid] = m
                    added += 1
                print(f"  tag={tag}: {len(items)} hits, +{added} new (total {len(by_id)})")
            except Exception as e:
                print(f"  tag={tag} err: {e}")

        # 2. Broad scan via active+closed pagination, keyword filter
        print("  broad keyword scan...")
        for closed_flag in ["false", "true"]:
            items = await fetch_pages(client, {"closed": closed_flag}, max_pages=80)
            added = 0
            for m in items:
                mid = str(m.get("id", ""))
                if not mid or mid in by_id:
                    continue
                if is_political(m.get("question", ""), m.get("slug", "")):
                    by_id[mid] = m
                    added += 1
            print(f"  closed={closed_flag}: scanned {len(items)} markets, +{added} political")

    print(f"\n  Total unique political markets: {len(by_id)}")

    # Filter by created/end date in last 90 days
    political = []
    for m in by_id.values():
        # Parse end date and created date
        end = m.get("endDate", "")
        created = m.get("startDate", "") or m.get("createdAt", "")
        try:
            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None
        except Exception:
            end_dt = None
        try:
            created_dt = datetime.fromisoformat(created.replace("Z", "+00:00")) if created else None
        except Exception:
            created_dt = None
        # Keep if either created or ends within last 90 days, or still active
        if not m.get("closed"):
            political.append(m)
        elif end_dt and end_dt >= cutoff_date:
            political.append(m)
        elif created_dt and created_dt >= cutoff_date:
            political.append(m)

    print(f"  Filtered to last 90d (created or ending): {len(political)}")

    # Categorize
    cats = defaultdict(list)
    for m in political:
        q = m.get("question", "").lower()
        if any(k in q for k in ["election", "vote", "ballot", "president", "presidential"]):
            cat = "elections"
        elif any(k in q for k in ["russia", "ukraine", "iran", "israel", "gaza", "hamas", "war", "ceasefire", "invade"]):
            cat = "geopolitics"
        elif any(k in q for k in ["trump", "biden", "harris", "vance"]):
            cat = "us_political"
        elif any(k in q for k in ["scotus", "supreme court", "impeachment", "indict"]):
            cat = "us_legal"
        elif any(k in q for k in ["tariff", "sanction", "executive order"]):
            cat = "policy"
        elif any(k in q for k in ["china", "xi", "putin", "kim jong"]):
            cat = "world_leaders"
        else:
            cat = "other_political"
        cats[cat].append(m)

    print(f"\n━━━ Categories ━━━")
    for cat, ms in sorted(cats.items(), key=lambda x: -len(x[1])):
        active = sum(1 for m in ms if not m.get("closed"))
        closed = sum(1 for m in ms if m.get("closed"))
        total_vol = sum(float(m.get("volume", 0) or 0) for m in ms)
        total_liq = sum(float(m.get("liquidity", 0) or 0) for m in ms)
        print(f"  {cat:<20} total={len(ms):>4} (active {active}, closed {closed})  "
              f"vol=${total_vol/1e6:.1f}M  liq=${total_liq/1e3:.0f}K")

    # Save full data
    out = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "cutoff_date": cutoff_date.isoformat(),
        "n_total": len(political),
        "by_category": {cat: len(ms) for cat, ms in cats.items()},
        "markets": [
            {
                "id": m.get("id"),
                "question": m.get("question", ""),
                "slug": m.get("slug", ""),
                "category": next((c for c, ms in cats.items() if m in ms), "?"),
                "volume": float(m.get("volume", 0) or 0),
                "liquidity": float(m.get("liquidity", 0) or 0),
                "active": not m.get("closed"),
                "closed": bool(m.get("closed")),
                "endDate": m.get("endDate", ""),
                "startDate": m.get("startDate", ""),
                "outcomes": m.get("outcomes", ""),
                "outcomePrices": m.get("outcomePrices", ""),
                "conditionId": m.get("conditionId", ""),
                "tag_slugs": [t.get("slug", "") for t in m.get("tags", []) if isinstance(t, dict)],
            }
            for m in political
        ],
    }
    with open("bot-data/political_markets.json", "w") as f:
        json.dump(out, f, indent=1)
    print(f"\nSaved {len(political)} markets to bot-data/political_markets.json")

    # Top 15 by volume
    political.sort(key=lambda m: -float(m.get("volume", 0) or 0))
    print(f"\n━━━ Top 15 by volume ━━━")
    for m in political[:15]:
        v = float(m.get("volume", 0) or 0)
        cat = next((c for c, ms in cats.items() if m in ms), "?")
        end = m.get("endDate", "")[:10]
        st = "ACTIVE" if not m.get("closed") else "closed"
        print(f"  ${v/1e6:>5.1f}M [{cat:<14}] [{st:<6}] end={end}")
        print(f"    {m.get('question', '')[:90]}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
