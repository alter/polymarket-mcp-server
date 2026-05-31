#!/usr/bin/env python3
"""
Scan RESOLVED political markets in last 6 months (broader window for sample).
For each: get outcome (which side won) and metadata.
"""
import json, asyncio, re
from datetime import datetime, timezone, timedelta

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

# Strict regex
STRONG_POLITICAL = re.compile(
    r'\b(election|vote|ballot|president|senate|congress|impeach|indict|treaty|'
    r'ceasefire|invad|sanction|tariff|prime minister|chancellor|parliament|'
    r'NATO|UN security|supreme court|scotus|executive order|'
    r'trump|biden|harris|vance|musk|putin|xi jinping|netanyahu|zelensky|'
    r'republican\b|democrat|gop\b|head of state|prime minister|'
    r'russia[-\s]?ukraine|israel[-\s]?(palestin|hamas|gaza|hezbollah)|'
    r'north korea|south korea|kim jong)\b',
    re.IGNORECASE)

EXCLUDE = re.compile(
    r'\b(NHL|NBA|MLB|NFL|FIFA|World Cup|Stanley Cup|Champions League|Super Bowl|'
    r'tennis|tournament|playoffs?|finals|Masters|Wimbledon|UFC|MMA|fight night|'
    r'bitcoin|ethereum|crypto|altcoin|defi|launch|airdrop|halving|'
    r'oscar|grammy|emmy|movie|album|tour|concert|GTA|game\b|'
    r'temperature|hurricane|earthquake|stock|S&P|nasdaq|dow|'
    r'recipe|food|cooking|gambl)\b', re.IGNORECASE)


async def fetch_pages(client, params_base, max_pages=80):
    items = []
    for page in range(max_pages):
        params = {**params_base, "limit": 100, "offset": page * 100}
        try:
            r = await client.get(f"{GAMMA}/markets", params=params, timeout=20.0)
            if r.status_code != 200:
                break
            batch = r.json()
        except Exception as e:
            print(f"   err: {e}")
            break
        if not batch:
            break
        items.extend(batch)
        if len(batch) < 100:
            break
    return items


async def fetch_outcome_from_clob(client, cid):
    """Get yes_won from CLOB market."""
    if not cid:
        return None
    try:
        r = await client.get(f"{CLOB}/markets/{cid}", timeout=10.0)
        if r.status_code != 200:
            return None
        d = r.json()
        if not d.get("closed"):
            return None
        tokens = d.get("tokens", [])
        if not tokens:
            return None
        # tokens[0] = YES, tokens[1] = NO
        return bool(tokens[0].get("winner", False))
    except Exception:
        return None


async def main():
    cutoff = datetime.now(timezone.utc) - timedelta(days=180)
    print(f"Scanning resolved markets since {cutoff.date()}...")

    async with httpx.AsyncClient(timeout=30.0) as client:
        # closed=true
        items = await fetch_pages(client, {"closed": "true", "active": "false"}, max_pages=100)
        print(f"  Got {len(items)} closed markets total")

        # Filter to political + recent
        political = []
        for m in items:
            q = m.get("question", "")
            if EXCLUDE.search(q):
                continue
            if not STRONG_POLITICAL.search(q):
                continue
            end = m.get("endDate", "")
            try:
                end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None
            except Exception:
                end_dt = None
            if end_dt and end_dt < cutoff:
                continue
            political.append(m)
        print(f"  Filtered to political (last 180d): {len(political)}")

        # For each, fetch outcome via CLOB
        print(f"  Fetching outcomes from CLOB...")
        sem = asyncio.Semaphore(8)

        async def get_outcome(m):
            async with sem:
                cid = m.get("conditionId", "")
                outcome = await fetch_outcome_from_clob(client, cid)
                return m, outcome

        results = await asyncio.gather(*[get_outcome(m) for m in political])
        with_outcome = [(m, o) for m, o in results if o is not None]
        print(f"  With known outcome: {len(with_outcome)}")

    # Build clean records
    records = []
    for m, yes_won in with_outcome:
        end = m.get("endDate", "")
        records.append({
            "id": m.get("id"),
            "question": m.get("question", ""),
            "slug": m.get("slug", ""),
            "endDate": end,
            "startDate": m.get("startDate", "") or m.get("createdAt", ""),
            "yes_won": yes_won,
            "outcome": "YES" if yes_won else "NO",
            "volume": float(m.get("volume", 0) or 0),
            "liquidity": float(m.get("liquidity", 0) or 0),
            "conditionId": m.get("conditionId", ""),
        })

    records.sort(key=lambda r: -r["volume"])
    out = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "n_total": len(records),
        "markets": records,
    }
    with open("bot-data/political_resolved.json", "w") as f:
        json.dump(out, f, indent=1)
    print(f"\nSaved {len(records)} markets to bot-data/political_resolved.json")

    print(f"\n━━━ Top 25 resolved by volume ━━━")
    for r in records[:25]:
        end = r["endDate"][:10] if r["endDate"] else "?"
        print(f"  vol=${r['volume']/1e6:>5.2f}M  end={end}  → {r['outcome']}")
        print(f"    {r['question'][:90]}")


if __name__ == "__main__":
    asyncio.run(main())
