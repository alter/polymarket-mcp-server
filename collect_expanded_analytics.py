#!/usr/bin/env python3
"""Collect expanded analytics data from Polymarket APIs for bias analysis."""

import json
import os
import time
from datetime import datetime, timezone

import httpx

GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
OUTPUT_DIR = "bot-data/analytics_v2"
DELAY = 0.2


def make_client() -> httpx.Client:
    return httpx.Client(timeout=20, headers={"Accept": "application/json"})


def fetch_paginated(client: httpx.Client, url: str, params: dict, target_count: int) -> list:
    """Fetch paginated results up to target_count."""
    results = []
    offset = 0
    limit = params.get("limit", 100)
    while len(results) < target_count:
        params["offset"] = offset
        try:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  Error at offset {offset}: {e}")
            break
        if not data:
            break
        results.extend(data)
        print(f"  Fetched {len(results)}/{target_count} ...")
        if len(data) < limit:
            break
        offset += limit
        time.sleep(DELAY)
    return results[:target_count]


def collect_resolved_markets(client: httpx.Client) -> list:
    """1. Collect 2000+ resolved markets."""
    print("\n=== Collecting resolved markets ===")
    params = {"closed": "true", "limit": 100, "order": "endDate", "ascending": "false"}
    markets = fetch_paginated(client, f"{GAMMA_API}/markets", params, 2100)
    print(f"  Total resolved markets collected: {len(markets)}")
    return markets


def enrich_resolution_data(markets: list) -> list:
    """2. For each resolved market, track which token resolved to 1.0."""
    print("\n=== Enriching resolution data ===")
    enriched = []
    for i, m in enumerate(markets):
        tokens = m.get("clobTokenIds") or m.get("clob_token_ids") or ""
        outcome_prices = m.get("outcomePrices") or m.get("outcome_prices") or ""
        outcomes = m.get("outcomes") or ""

        # Parse token IDs
        if isinstance(tokens, str):
            try:
                token_list = json.loads(tokens) if tokens.startswith("[") else [t.strip() for t in tokens.split(",") if t.strip()]
            except json.JSONDecodeError:
                token_list = []
        elif isinstance(tokens, list):
            token_list = tokens
        else:
            token_list = []

        # Parse outcome prices
        if isinstance(outcome_prices, str):
            try:
                price_list = json.loads(outcome_prices) if outcome_prices.startswith("[") else [p.strip() for p in outcome_prices.split(",") if p.strip()]
            except json.JSONDecodeError:
                price_list = []
        elif isinstance(outcome_prices, list):
            price_list = outcome_prices
        else:
            price_list = []

        # Parse outcomes
        if isinstance(outcomes, str):
            try:
                outcome_list = json.loads(outcomes) if outcomes.startswith("[") else [o.strip() for o in outcomes.split(",") if o.strip()]
            except json.JSONDecodeError:
                outcome_list = []
        elif isinstance(outcomes, list):
            outcome_list = outcomes
        else:
            outcome_list = []

        # Determine winning token
        winning_token = None
        winning_outcome = None
        for idx, price in enumerate(price_list):
            try:
                p = float(price)
            except (ValueError, TypeError):
                continue
            if p >= 0.99:
                if idx < len(token_list):
                    winning_token = token_list[idx]
                if idx < len(outcome_list):
                    winning_outcome = outcome_list[idx]
                break

        record = {
            "id": m.get("id"),
            "question": m.get("question"),
            "slug": m.get("slug"),
            "conditionId": m.get("conditionId") or m.get("condition_id"),
            "eventSlug": m.get("eventSlug") or m.get("event_slug"),
            "volume": m.get("volume"),
            "endDate": m.get("endDate") or m.get("end_date"),
            "closedTime": m.get("closedTime") or m.get("closed_time"),
            "token_ids": token_list,
            "outcome_prices": price_list,
            "outcomes": outcome_list,
            "winning_token": winning_token,
            "winning_outcome": winning_outcome,
        }
        enriched.append(record)

        if (i + 1) % 500 == 0:
            print(f"  Enriched {i + 1}/{len(markets)} markets")

    yes_wins = sum(1 for r in enriched if r["winning_outcome"] == "Yes")
    no_wins = sum(1 for r in enriched if r["winning_outcome"] == "No")
    unknown = len(enriched) - yes_wins - no_wins
    print(f"  Resolution stats: Yes={yes_wins}, No={no_wins}, Other/Unknown={unknown}")
    return enriched


def collect_trades_for_top_markets(client: httpx.Client, resolved_markets: list) -> dict:
    """3. Collect public trades from data-api for top 500 markets by volume."""
    print("\n=== Collecting trades for top markets ===")

    def safe_volume(m):
        try:
            return float(m.get("volume") or 0)
        except (ValueError, TypeError):
            return 0

    sorted_markets = sorted(resolved_markets, key=safe_volume, reverse=True)
    top_markets = sorted_markets[:500]

    trades_by_market = {}
    for i, m in enumerate(top_markets):
        condition_id = m.get("conditionId") or m.get("condition_id") or m.get("id")
        if not condition_id:
            continue

        # Try multiple ID fields that data-api might accept
        market_id = condition_id
        try:
            resp = client.get(f"{DATA_API}/trades", params={"market": market_id, "limit": 50})
            resp.raise_for_status()
            data = resp.json()
            if data:
                trades_by_market[market_id] = {
                    "question": m.get("question"),
                    "volume": m.get("volume"),
                    "trades": data,
                }
        except Exception as e:
            if (i + 1) % 100 == 0:
                print(f"  Trade fetch error for {market_id}: {e}")

        if (i + 1) % 50 == 0:
            print(f"  Fetched trades for {i + 1}/500 markets ({len(trades_by_market)} with data)")
        time.sleep(DELAY)

    print(f"  Total markets with trade data: {len(trades_by_market)}")
    return trades_by_market


def collect_active_markets_with_prices(client: httpx.Client) -> list:
    """4. Collect active markets with current prices and midpoints."""
    print("\n=== Collecting active markets ===")
    params = {
        "active": "true",
        "closed": "false",
        "limit": 100,
        "order": "volume24hr",
        "ascending": "false",
    }
    markets = fetch_paginated(client, f"{GAMMA_API}/markets", params, 500)
    print(f"  Active markets collected: {len(markets)}")

    print("  Fetching CLOB prices for active markets...")
    enriched = []
    for i, m in enumerate(markets):
        tokens = m.get("clobTokenIds") or m.get("clob_token_ids") or ""
        if isinstance(tokens, str):
            try:
                token_list = json.loads(tokens) if tokens.startswith("[") else [t.strip() for t in tokens.split(",") if t.strip()]
            except json.JSONDecodeError:
                token_list = []
        elif isinstance(tokens, list):
            token_list = tokens
        else:
            token_list = []

        outcomes = m.get("outcomes") or ""
        if isinstance(outcomes, str):
            try:
                outcome_list = json.loads(outcomes) if outcomes.startswith("[") else [o.strip() for o in outcomes.split(",") if o.strip()]
            except json.JSONDecodeError:
                outcome_list = []
        elif isinstance(outcomes, list):
            outcome_list = outcomes
        else:
            outcome_list = []

        prices = {}
        for idx, token_id in enumerate(token_list):
            if not token_id:
                continue
            outcome_name = outcome_list[idx] if idx < len(outcome_list) else f"token_{idx}"
            try:
                buy_resp = client.get(f"{CLOB_API}/price", params={"token_id": token_id, "side": "buy"})
                buy_resp.raise_for_status()
                buy_price = buy_resp.json().get("price")

                sell_resp = client.get(f"{CLOB_API}/price", params={"token_id": token_id, "side": "sell"})
                sell_resp.raise_for_status()
                sell_price = sell_resp.json().get("price")

                try:
                    bp = float(buy_price) if buy_price else None
                    sp = float(sell_price) if sell_price else None
                    mid = round((bp + sp) / 2, 6) if bp is not None and sp is not None else None
                    spread = round(sp - bp, 6) if bp is not None and sp is not None else None
                except (ValueError, TypeError):
                    bp, sp, mid, spread = None, None, None, None

                prices[outcome_name] = {
                    "token_id": token_id,
                    "buy": buy_price,
                    "sell": sell_price,
                    "midpoint": mid,
                    "spread": spread,
                }
                time.sleep(DELAY)
            except Exception as e:
                prices[outcome_name] = {"token_id": token_id, "error": str(e)}
                time.sleep(DELAY)

        # Check if probabilities sum to ~100%
        midpoints = [p.get("midpoint") for p in prices.values() if p.get("midpoint") is not None]
        prob_sum = round(sum(midpoints), 4) if midpoints else None

        enriched.append({
            "id": m.get("id"),
            "question": m.get("question"),
            "slug": m.get("slug"),
            "eventSlug": m.get("eventSlug") or m.get("event_slug"),
            "volume": m.get("volume"),
            "volume24hr": m.get("volume24hr") or m.get("volume_24hr"),
            "liquidity": m.get("liquidity"),
            "outcomes": outcome_list,
            "token_ids": token_list,
            "prices": prices,
            "probability_sum": prob_sum,
            "snapshot_time": datetime.now(timezone.utc).isoformat(),
        })

        if (i + 1) % 25 == 0:
            print(f"  Priced {i + 1}/{len(markets)} active markets")

    return enriched


def collect_events(client: httpx.Client) -> list:
    """5. Collect event-level data for multi-outcome analysis."""
    print("\n=== Collecting events ===")
    params = {"closed": "false", "limit": 50}
    try:
        resp = client.get(f"{GAMMA_API}/events", params=params)
        resp.raise_for_status()
        events = resp.json()
    except Exception as e:
        print(f"  Error fetching events: {e}")
        events = []

    print(f"  Events collected: {len(events)}")

    enriched_events = []
    for ev in events:
        markets = ev.get("markets", [])
        market_summaries = []
        for m in markets:
            outcome_prices = m.get("outcomePrices") or m.get("outcome_prices") or ""
            if isinstance(outcome_prices, str):
                try:
                    price_list = json.loads(outcome_prices) if outcome_prices.startswith("[") else []
                except json.JSONDecodeError:
                    price_list = []
            elif isinstance(outcome_prices, list):
                price_list = outcome_prices
            else:
                price_list = []

            outcomes = m.get("outcomes") or ""
            if isinstance(outcomes, str):
                try:
                    outcome_list = json.loads(outcomes) if outcomes.startswith("[") else []
                except json.JSONDecodeError:
                    outcome_list = []
            elif isinstance(outcomes, list):
                outcome_list = outcomes
            else:
                outcome_list = []

            market_summaries.append({
                "id": m.get("id"),
                "question": m.get("question"),
                "outcomes": outcome_list,
                "outcome_prices": price_list,
                "volume": m.get("volume"),
                "active": m.get("active"),
                "closed": m.get("closed"),
            })

        # For multi-outcome events, sum the "Yes" prices across markets
        yes_prices = []
        for ms in market_summaries:
            if ms["outcome_prices"] and len(ms["outcome_prices"]) > 0:
                try:
                    yes_prices.append(float(ms["outcome_prices"][0]))
                except (ValueError, TypeError, IndexError):
                    pass

        enriched_events.append({
            "id": ev.get("id"),
            "slug": ev.get("slug"),
            "title": ev.get("title"),
            "description": ev.get("description"),
            "num_markets": len(markets),
            "markets": market_summaries,
            "yes_price_sum": round(sum(yes_prices), 4) if yes_prices else None,
            "is_multi_outcome": len(markets) > 1,
        })

    multi = sum(1 for e in enriched_events if e["is_multi_outcome"])
    print(f"  Multi-outcome events: {multi}/{len(enriched_events)}")
    return enriched_events


def save_json(data, filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"  Saved {path} ({size_mb:.1f} MB)")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start = time.time()
    print(f"Starting expanded analytics collection at {datetime.now(timezone.utc).isoformat()}")

    client = make_client()

    # 1. Resolved markets
    resolved_raw = collect_resolved_markets(client)
    save_json(resolved_raw, "resolved_markets_raw.json")

    # 2. Enrich with resolution data
    resolved_enriched = enrich_resolution_data(resolved_raw)
    save_json(resolved_enriched, "resolved_markets_enriched.json")

    # 3. Trades for top markets
    trades = collect_trades_for_top_markets(client, resolved_enriched)
    save_json(trades, "trades_top500.json")

    # 4. Active markets with prices
    active_priced = collect_active_markets_with_prices(client)
    save_json(active_priced, "active_markets_priced.json")

    # 5. Events
    events = collect_events(client)
    save_json(events, "events_multi_outcome.json")

    # Summary
    elapsed = time.time() - start
    summary = {
        "collection_time": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "resolved_markets": len(resolved_enriched),
        "resolved_yes_wins": sum(1 for r in resolved_enriched if r["winning_outcome"] == "Yes"),
        "resolved_no_wins": sum(1 for r in resolved_enriched if r["winning_outcome"] == "No"),
        "trades_markets_count": len(trades),
        "trades_total": sum(len(v["trades"]) for v in trades.values()),
        "active_markets_priced": len(active_priced),
        "events_count": len(events),
        "multi_outcome_events": sum(1 for e in events if e["is_multi_outcome"]),
    }
    save_json(summary, "collection_summary.json")

    print(f"\n=== Done in {elapsed:.0f}s ===")
    print(json.dumps(summary, indent=2))

    client.close()


if __name__ == "__main__":
    main()
