#!/usr/bin/env python3
"""
Recover arena_ticks for May 13–22 gap using Polymarket CLOB prices-history API.

Sources:
  - arena_ticks.jsonl: read to find which market_ids had any activity during gap
  - Gamma API: /markets/{mid} → conditionId + YES token_id
  - CLOB API:  /prices-history?market={token_id}&interval=max&fidelity=1 → {t, p}

Output: bot-data/arena_ticks_recovered.jsonl
  same format as arena_ticks: {ts, market_id, mid, bid, ask, fees}
  bid=ask=p (no spread data available from history API)
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"

DATA_DIR    = "bot-data"
TICKS_FILE  = os.path.join(DATA_DIR, "arena_ticks.jsonl")
OUT_FILE    = os.path.join(DATA_DIR, "arena_ticks_recovered.jsonl")

GAP_START = datetime(2026, 5, 13, tzinfo=timezone.utc).timestamp()
GAP_END   = datetime(2026, 5, 23, tzinfo=timezone.utc).timestamp()

CONCURRENCY = 12
BATCH_SIZE  = 50   # flush every N markets


def collect_gap_market_ids():
    """Return set of market_ids that had ANY ticks during the gap window."""
    ids = set()
    file_size = os.path.getsize(TICKS_FILE)
    # Scan last 50MB — gap is near the end of the file
    offset = max(0, file_size - 50_000_000)
    with open(TICKS_FILE, "rb") as f:
        f.seek(offset)
        if offset > 0:
            f.readline()
        for line in f:
            try:
                t = json.loads(line)
                ts = datetime.fromisoformat(
                    t["ts"].replace("Z", "+00:00")
                ).timestamp()
                if GAP_START <= ts <= GAP_END:
                    ids.add(str(t["market_id"]))
            except Exception:
                pass
    return ids


async def fetch_token_id(client, sem, market_id):
    """Return (market_id, token_id) or None if can't resolve."""
    async with sem:
        try:
            r = await client.get(f"{GAMMA}/markets/{market_id}", timeout=10.0)
            if r.status_code != 200:
                return None
            m = r.json()
            tokens_raw = m.get("clobTokenIds", "[]")
            tokens = (
                json.loads(tokens_raw)
                if isinstance(tokens_raw, str)
                else (tokens_raw or [])
            )
            if not tokens:
                return None
            return (market_id, str(tokens[0]))
        except Exception:
            return None


async def fetch_price_history(client, sem, market_id, token_id):
    """Return list of {ts_iso, market_id, mid, bid, ask, fees} filtered to gap window."""
    async with sem:
        try:
            r = await client.get(
                f"{CLOB}/prices-history",
                params={"market": token_id, "interval": "max", "fidelity": "1"},
                timeout=15.0,
            )
            if r.status_code != 200:
                return []
            history = r.json().get("history", [])
        except Exception:
            return []

    out = []
    for item in history:
        t = item.get("t", 0)
        p = item.get("p")
        if p is None or not (GAP_START <= t <= GAP_END):
            continue
        out.append({
            "ts": datetime.fromtimestamp(t, tz=timezone.utc).isoformat(),
            "market_id": market_id,
            "mid": round(float(p), 6),
            "bid": round(float(p), 6),
            "ask": round(float(p), 6),
            "fees": False,
        })
    return out


async def main():
    print("=== arena_ticks recovery ===")
    print(f"Gap window: {datetime.fromtimestamp(GAP_START, tz=timezone.utc).date()} "
          f"→ {datetime.fromtimestamp(GAP_END, tz=timezone.utc).date()}")

    print("Step 1: collecting market_ids with gap-period ticks from arena_ticks.jsonl …")
    t0 = time.time()
    market_ids = collect_gap_market_ids()
    print(f"  found {len(market_ids)} market_ids in {time.time()-t0:.1f}s")

    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(timeout=15.0) as client:

        print("Step 2: resolving token_ids from Gamma API …")
        t0 = time.time()
        tasks = [fetch_token_id(client, sem, mid) for mid in market_ids]
        results = await asyncio.gather(*tasks)
        mid_to_token = {mid: tok for r in results if r for mid, tok in [r]}
        print(f"  resolved {len(mid_to_token)}/{len(market_ids)} token_ids "
              f"in {time.time()-t0:.1f}s")

        print("Step 3: fetching price history …")
        t0 = time.time()
        all_ticks = []
        tasks2 = [
            fetch_price_history(client, sem, mid, tok)
            for mid, tok in mid_to_token.items()
        ]
        chunk = 0
        for coro in asyncio.as_completed(tasks2):
            ticks = await coro
            all_ticks.extend(ticks)
            chunk += 1
            if chunk % 50 == 0:
                print(f"  {chunk}/{len(mid_to_token)} markets done, "
                      f"{len(all_ticks)} ticks so far …")
        print(f"  fetched {len(all_ticks)} total ticks in {time.time()-t0:.1f}s")

    print("Step 4: sorting and writing …")
    all_ticks.sort(key=lambda x: x["ts"])
    with open(OUT_FILE, "w") as f:
        for tick in all_ticks:
            f.write(json.dumps(tick, separators=(",", ":")) + "\n")
    print(f"Written {len(all_ticks)} ticks → {OUT_FILE}")

    # Quick stats
    if all_ticks:
        dates = {}
        for tick in all_ticks:
            d = tick["ts"][:10]
            dates[d] = dates.get(d, 0) + 1
        print("\nTicks per day:")
        for d, n in sorted(dates.items()):
            print(f"  {d}: {n}")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    asyncio.run(main())
