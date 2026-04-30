#!/usr/bin/env python3
"""
Orderbook depth collector — captures top-5 levels of /book?token_id=... CLOB
endpoint for all active markets, every CYCLE_SEC.

Output: bot-data/orderbook_snapshots.jsonl
Line format: {ts, market_id, cid, token_id, bids: [(p,s)*5], asks: [(p,s)*5]}

Each market sampled once per cycle; light load (~63 q/30s = 2 req/sec).
"""
import asyncio, gc, json, os, time
from datetime import datetime, timezone

import httpx

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
DATA = "data"
META_FILE = os.path.join(DATA, "gamma_market_meta.json")
CLOB_CACHE = os.path.join(DATA, "clob_cache.json")
TICKS_FILE = os.path.join(DATA, "arena_ticks.jsonl")
OUT_FILE = os.path.join(DATA, "orderbook_snapshots.jsonl")

CYCLE_SEC = 30
TOP_N_LEVELS = 5
CONCURRENCY = 8
RECENT_TICK_HOURS = 2   # only collect for markets active in last 2h


def recent_market_ids():
    """Read tail of arena_ticks.jsonl, return set of market_ids active in
    last RECENT_TICK_HOURS hours. Cheap: tail last ~5MB."""
    if not os.path.exists(TICKS_FILE):
        return set()
    cutoff_ts = time.time() - RECENT_TICK_HOURS * 3600
    out = set()
    try:
        size = os.path.getsize(TICKS_FILE)
        offset = max(0, size - 5_000_000)  # last 5MB
        with open(TICKS_FILE, "rb") as f:
            f.seek(offset)
            if offset > 0:
                f.readline()  # skip partial line
            for line in f:
                try:
                    t = json.loads(line)
                    ts = datetime.fromisoformat(t["ts"].replace("Z", "+00:00")).timestamp()
                    if ts >= cutoff_ts:
                        out.add(str(t["market_id"]))
                except Exception:
                    continue
    except Exception as e:
        print(f"recent_market_ids err: {e}")
    return out


async def fetch_one_market(client, sem, mid):
    """Fetch single market by id, return (mid, cid, yes_token) or None."""
    async with sem:
        try:
            r = await client.get(f"{GAMMA}/markets/{mid}", timeout=10.0)
            if r.status_code != 200:
                return None
            m = r.json()
        except Exception:
            return None
    if m.get("closed"):
        return None
    cid = m.get("conditionId", "")
    tokens_raw = m.get("clobTokenIds", "[]")
    if isinstance(tokens_raw, str):
        try:
            tokens = json.loads(tokens_raw)
        except Exception:
            return None
    else:
        tokens = tokens_raw
    if not cid or not tokens:
        return None
    return (str(mid), cid, tokens[0])


async def load_active_markets(client):
    """Fetch tokens for markets active in arena_ticks recently."""
    recent = recent_market_ids()
    print(f"  recent market_ids (last {RECENT_TICK_HOURS}h): {len(recent)}")
    if not recent:
        return []
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [fetch_one_market(client, sem, mid) for mid in recent]
    results = await asyncio.gather(*tasks)
    return [r for r in results if r is not None]


async def fetch_book(client, sem, market_id, cid, token_id):
    """Fetch one orderbook snapshot. Returns dict or None on error."""
    async with sem:
        try:
            r = await client.get(f"{CLOB}/book?token_id={token_id}", timeout=10.0)
            if r.status_code != 200:
                return None
            d = r.json()
        except Exception:
            return None

    bids = d.get("bids", [])
    asks = d.get("asks", [])
    # Polymarket /book returns bids ascending price (worst first), asks
    # descending (worst first). Best bid = last bid. Best ask = last ask.
    # Take last TOP_N levels.
    top_bids = bids[-TOP_N_LEVELS:] if bids else []
    top_asks = asks[-TOP_N_LEVELS:] if asks else []

    if not top_bids or not top_asks:
        return None

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "market_id": market_id,
        "cid": cid,
        "token_id": token_id,
        # Each level: [price, size]; bids[-1] = best bid, asks[-1] = best ask
        "bids": [[float(b["price"]), float(b["size"])] for b in top_bids],
        "asks": [[float(a["price"]), float(a["size"])] for a in top_asks],
    }


async def collect_cycle(client, markets, fout):
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [fetch_book(client, sem, mid, cid, tid) for mid, cid, tid in markets]
    results = await asyncio.gather(*tasks)
    n_ok = 0
    for r in results:
        if r is None:
            continue
        fout.write(json.dumps(r, separators=(",", ":")) + "\n")
        n_ok += 1
    fout.flush()
    return n_ok


async def main():
    os.makedirs(DATA, exist_ok=True)
    last_reload = 0
    markets = []
    async with httpx.AsyncClient(timeout=15.0) as client:
        with open(OUT_FILE, "a") as fout:
            print(f"OrderbookCollector starting, output={OUT_FILE}, "
                  f"cycle={CYCLE_SEC}s, top_n={TOP_N_LEVELS}")
            while True:
                # Reload market list every 10 minutes (new markets appear)
                now = time.time()
                if now - last_reload > 600:
                    markets = await load_active_markets(client)
                    last_reload = now
                    print(f"[{datetime.now():%H:%M:%S}] Tracking {len(markets)} active markets")

                if not markets:
                    print(f"[{datetime.now():%H:%M:%S}] No markets — sleeping 60s")
                    await asyncio.sleep(60)
                    continue

                t0 = time.time()
                try:
                    n_ok = await collect_cycle(client, markets, fout)
                except Exception as e:
                    print(f"Cycle error: {e}")
                    n_ok = 0
                dt = time.time() - t0
                print(f"[{datetime.now():%H:%M:%S}] Cycle: {n_ok}/{len(markets)} "
                      f"books in {dt:.1f}s")

                # GC every cycle to release per-cycle structures
                gc.collect()
                # Sleep to maintain CYCLE_SEC interval
                sleep_for = max(1.0, CYCLE_SEC - dt)
                await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
