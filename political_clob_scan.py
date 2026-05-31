#!/usr/bin/env python3
"""
Scan CLOB endpoint for closed political markets — has full resolution data.
Pulls events with markets, filters by political keywords + last 180d.
"""
import json, asyncio, re
from datetime import datetime, timezone, timedelta
import httpx

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"

STRONG_POLITICAL = re.compile(
    r'\b(election|vote|ballot|president|senate|congress|impeach|indict|treaty|'
    r'ceasefire|invad|sanction|tariff|prime minister|chancellor|parliament|'
    r'NATO|UN security|supreme court|scotus|executive order|'
    r'trump|biden|harris|vance|musk|putin|xi jinping|netanyahu|zelensky|'
    r'republican\b|democrat|gop\b|head of state|prime minister|'
    r'russia[-\s]?ukraine|israel[-\s]?(palestin|hamas|gaza|hezbollah)|'
    r'north korea|south korea|kim jong)\b', re.IGNORECASE)

EXCLUDE = re.compile(
    r'\b(NHL|NBA|MLB|NFL|FIFA|World Cup|Stanley Cup|Champions League|Super Bowl|'
    r'tennis|tournament|playoffs?|finals|Masters|Wimbledon|UFC|MMA|fight night|'
    r'bitcoin|ethereum|crypto|altcoin|defi|launch|airdrop|halving|'
    r'oscar|grammy|emmy|movie|album|tour|concert|GTA|game\b|'
    r'temperature|hurricane|earthquake|stock|S&P|nasdaq|dow|'
    r'recipe|food|cooking|gambl|tweet|tweets)\b', re.IGNORECASE)


async def fetch_clob_markets(client, cursor=None, limit=500):
    """Paginated fetch of CLOB /markets — returns closed=True markets directly."""
    params = {"limit": limit}
    if cursor:
        params["next_cursor"] = cursor
    try:
        r = await client.get(f"{CLOB}/markets", params=params, timeout=20.0)
        if r.status_code != 200:
            return None, None
        d = r.json()
        return d.get("data", []), d.get("next_cursor", "")
    except Exception as e:
        print(f"  err: {e}")
        return None, None


async def main():
    cutoff = datetime.now(timezone.utc) - timedelta(days=180)
    print(f"Fetching CLOB markets, scanning for political resolved (since {cutoff.date()})...")

    political = []
    seen = set()
    async with httpx.AsyncClient(timeout=30.0) as client:
        cursor = ""
        for page in range(40):
            items, cursor = await fetch_clob_markets(client, cursor)
            if items is None:
                break
            new_political = 0
            for m in items:
                cid = m.get("condition_id", "")
                if cid in seen:
                    continue
                seen.add(cid)
                if not m.get("closed"):
                    continue
                q = m.get("question", "")
                if not q or EXCLUDE.search(q) or not STRONG_POLITICAL.search(q):
                    continue
                end = m.get("end_date_iso", "")
                try:
                    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None
                except Exception:
                    end_dt = None
                if end_dt and end_dt < cutoff:
                    continue
                tokens = m.get("tokens", [])
                if not tokens:
                    continue
                yes_won = any(t.get("winner") for t in tokens if t.get("outcome", "").lower() == "yes")
                no_won = any(t.get("winner") for t in tokens if t.get("outcome", "").lower() == "no")
                if not yes_won and not no_won:
                    continue
                political.append({
                    "id": cid,
                    "question": q,
                    "slug": m.get("market_slug", ""),
                    "end_date": end,
                    "yes_won": yes_won,
                    "outcome": "YES" if yes_won else "NO",
                    "rewards": m.get("rewards", {}),
                    "tags": m.get("tags", []),
                    "neg_risk": m.get("neg_risk", False),
                })
                new_political += 1
            print(f"  page {page+1}: scanned {len(items)} (cumul political {len(political)}, +{new_political} this page)")
            if not cursor or cursor == "LTE=":
                break

    print(f"\n  Found {len(political)} resolved political markets in last 180d")
    political.sort(key=lambda x: x.get("end_date") or "", reverse=True)

    out = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "n_total": len(political),
        "markets": political,
    }
    with open("bot-data/political_resolved_clob.json", "w") as f:
        json.dump(out, f, indent=1)

    print(f"\n━━━ Top 25 by recency ━━━")
    for r in political[:25]:
        end = r["end_date"][:10] if r["end_date"] else "?"
        print(f"  {end} → {r['outcome']}  {r['question'][:80]}")


if __name__ == "__main__":
    asyncio.run(main())
