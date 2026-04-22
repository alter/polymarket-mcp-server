#!/usr/bin/env python3
"""
Cross-platform prediction market price comparison.

Compares Polymarket prices with Kalshi, Metaculus, PredictIt, and Manifold Markets
to find pricing discrepancies using fuzzy question matching.
"""

import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com/markets"
KALSHI_URL = "https://api.elections.kalshi.com/trade-api/v2/markets?limit=100&status=open"
METACULUS_URL = (
    "https://www.metaculus.com/api2/questions/"
    "?status=open&type=binary&limit=100&order_by=-activity"
)
PREDICTIT_URL = "https://www.predictit.org/api/marketdata/all"
MANIFOLD_URL = "https://api.manifold.markets/v0/search-markets?term=&sort=liquidity&limit=100"

PRICE_DIFF_THRESHOLD = 0.05  # 5%

STOP_WORDS = {"will", "the", "a", "an", "in", "on", "by", "of", "to", "be", "is", "are",
              "was", "were", "for", "and", "or", "not", "at", "it", "its", "do", "does",
              "did", "has", "have", "had", "this", "that", "with", "from", "as", "but",
              "if", "than", "so", "can", "could", "would", "should", "may", "might",
              "what", "which", "who", "whom", "how", "when", "where", "there", "their",
              "they", "he", "she", "his", "her", "we", "us", "our", "you", "your",
              "about", "before", "after", "during", "between", "into", "through",
              "over", "under", "up", "down", "out", "any", "all", "each", "every",
              "both", "few", "more", "most", "other", "some", "such", "no", "nor",
              "too", "very", "just", "also", "then", "next", "new", "old", "first",
              "last", "long", "great", "little", "own", "same", "big", "high", "low",
              "small", "large", "early", "late", "young", "end", "yes", "market",
              "question", "predict", "prediction", "chance", "probability", "percent",
              "whether", "happen", "occurring", "occur", "event", "outcome", "result",
              "become", "get", "make", "take", "go", "come"}

OUTPUT_DIR = Path(__file__).parent / "bot-data"
OUTPUT_FILE = OUTPUT_DIR / "cross_platform_comparison.json"

TIMEOUT = httpx.Timeout(20.0, connect=10.0)
HEADERS = {"User-Agent": "CrossPlatformCompare/1.0"}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class NormalizedMarket:
    platform: str
    title: str
    url: str
    probability: Optional[float]  # 0-1
    raw_id: str = ""
    keywords: set = field(default_factory=set, repr=False)


@dataclass
class MatchResult:
    polymarket: dict
    other: dict
    similarity: float
    price_diff: float  # absolute difference in probability
    flagged: bool


# ---------------------------------------------------------------------------
# Text normalization & similarity
# ---------------------------------------------------------------------------

def normalize_text(text: str) -> set[str]:
    """Lowercase, strip punctuation, remove stop words, return word set."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    words = text.split()
    return {w for w in words if w not in STOP_WORDS and len(w) > 1}


def jaccard_similarity(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    intersection = a & b
    union = a | b
    return len(intersection) / len(union)


# ---------------------------------------------------------------------------
# Platform fetchers
# ---------------------------------------------------------------------------

async def fetch_polymarket(client: httpx.AsyncClient) -> list[NormalizedMarket]:
    """Fetch active binary markets from Polymarket Gamma API."""
    markets = []
    try:
        resp = await client.get(
            POLYMARKET_GAMMA_URL,
            params={"limit": 100, "active": "true", "closed": "false"},
        )
        resp.raise_for_status()
        data = resp.json()
        for m in data:
            title = m.get("question") or m.get("title", "")
            # Polymarket uses outcomePrices as JSON string "[\"0.65\",\"0.35\"]"
            prob = None
            prices_raw = m.get("outcomePrices")
            if prices_raw:
                try:
                    prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                    if prices and len(prices) >= 1:
                        prob = float(prices[0])
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass
            if prob is None:
                # Try bestAsk/bestBid or clobTokenIds-based pricing
                best_ask = m.get("bestAsk")
                if best_ask is not None:
                    try:
                        prob = float(best_ask)
                    except (ValueError, TypeError):
                        pass
            if title and prob is not None:
                slug = m.get("slug", m.get("conditionId", ""))
                url = f"https://polymarket.com/event/{slug}" if slug else ""
                nm = NormalizedMarket(
                    platform="Polymarket",
                    title=title,
                    url=url,
                    probability=prob,
                    raw_id=str(m.get("conditionId", "")),
                    keywords=normalize_text(title),
                )
                markets.append(nm)
        print(f"  Polymarket: fetched {len(markets)} markets")
    except Exception as e:
        print(f"  Polymarket: ERROR - {e}")
    return markets


async def fetch_kalshi(client: httpx.AsyncClient) -> list[NormalizedMarket]:
    """Fetch active markets from Kalshi public elections API."""
    markets = []
    try:
        resp = await client.get(KALSHI_URL)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("markets", data) if isinstance(data, dict) else data
        if not isinstance(items, list):
            items = []
        for m in items:
            title = m.get("title") or m.get("subtitle") or ""
            prob = None
            # Kalshi uses yes_ask / yes_bid / last_price (cents 0-100 or 0-1)
            for key in ("last_price", "yes_ask", "yes_bid", "close_time_value"):
                val = m.get(key)
                if val is not None:
                    try:
                        p = float(val)
                        prob = p / 100.0 if p > 1.0 else p
                        break
                    except (ValueError, TypeError):
                        continue
            if title and prob is not None and 0 <= prob <= 1:
                ticker = m.get("ticker", "")
                url = f"https://kalshi.com/markets/{ticker}" if ticker else ""
                nm = NormalizedMarket(
                    platform="Kalshi",
                    title=title,
                    url=url,
                    probability=prob,
                    raw_id=ticker,
                    keywords=normalize_text(title),
                )
                markets.append(nm)
        print(f"  Kalshi: fetched {len(markets)} markets")
    except Exception as e:
        print(f"  Kalshi: ERROR - {e}")
    return markets


async def fetch_metaculus(client: httpx.AsyncClient) -> list[NormalizedMarket]:
    """Fetch open binary questions from Metaculus."""
    markets = []
    try:
        resp = await client.get(METACULUS_URL)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("results", data) if isinstance(data, dict) else data
        if not isinstance(items, list):
            items = []
        for q in items:
            title = q.get("title") or q.get("title_short") or ""
            prob = None
            # Metaculus community prediction
            cp = q.get("community_prediction")
            if isinstance(cp, dict):
                prob = cp.get("full", {}).get("q2") if isinstance(cp.get("full"), dict) else None
                if prob is None:
                    prob = cp.get("q2")
            if prob is None:
                # Try 'prediction_count' based aggregation fields
                for key in ("my_predictions", "metaculus_prediction"):
                    mp = q.get(key)
                    if isinstance(mp, dict):
                        prob = mp.get("full", {}).get("q2") if isinstance(mp.get("full"), dict) else None
                        if prob is not None:
                            break
            if prob is not None:
                try:
                    prob = float(prob)
                except (ValueError, TypeError):
                    prob = None
            if title and prob is not None and 0 <= prob <= 1:
                qid = q.get("id", "")
                url = f"https://www.metaculus.com/questions/{qid}/" if qid else ""
                nm = NormalizedMarket(
                    platform="Metaculus",
                    title=title,
                    url=url,
                    probability=prob,
                    raw_id=str(qid),
                    keywords=normalize_text(title),
                )
                markets.append(nm)
        print(f"  Metaculus: fetched {len(markets)} markets")
    except Exception as e:
        print(f"  Metaculus: ERROR - {e}")
    return markets


async def fetch_predictit(client: httpx.AsyncClient) -> list[NormalizedMarket]:
    """Fetch all PredictIt markets."""
    markets = []
    try:
        resp = await client.get(PREDICTIT_URL)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("markets", []) if isinstance(data, dict) else []
        for m in items:
            mkt_name = m.get("name", "")
            contracts = m.get("contracts", [])
            for c in contracts:
                title = c.get("name") or c.get("shortName") or mkt_name
                prob = None
                last = c.get("lastTradePrice")
                if last is not None:
                    try:
                        prob = float(last)
                    except (ValueError, TypeError):
                        pass
                if prob is None:
                    best_yes = c.get("bestBuyYesCost")
                    if best_yes is not None:
                        try:
                            prob = float(best_yes)
                        except (ValueError, TypeError):
                            pass
                if title and prob is not None and 0 <= prob <= 1:
                    cid = c.get("id", "")
                    mid = m.get("id", "")
                    url = f"https://www.predictit.org/markets/detail/{mid}" if mid else ""
                    nm = NormalizedMarket(
                        platform="PredictIt",
                        title=f"{mkt_name}: {title}" if mkt_name != title else title,
                        url=url,
                        probability=prob,
                        raw_id=str(cid),
                        keywords=normalize_text(f"{mkt_name} {title}"),
                    )
                    markets.append(nm)
        print(f"  PredictIt: fetched {len(markets)} markets")
    except Exception as e:
        print(f"  PredictIt: ERROR - {e}")
    return markets


async def fetch_manifold(client: httpx.AsyncClient) -> list[NormalizedMarket]:
    """Fetch top-liquidity markets from Manifold Markets."""
    markets = []
    try:
        resp = await client.get(MANIFOLD_URL)
        resp.raise_for_status()
        data = resp.json()
        items = data if isinstance(data, list) else data.get("results", [])
        for m in items:
            title = m.get("question") or ""
            prob = m.get("probability")
            if prob is None:
                prob = m.get("prob")
            if prob is not None:
                try:
                    prob = float(prob)
                except (ValueError, TypeError):
                    prob = None
            if title and prob is not None and 0 <= prob <= 1:
                slug = m.get("slug", "")
                creator = m.get("creatorUsername", "")
                url = f"https://manifold.markets/{creator}/{slug}" if slug and creator else ""
                nm = NormalizedMarket(
                    platform="Manifold",
                    title=title,
                    url=url,
                    probability=prob,
                    raw_id=m.get("id", ""),
                    keywords=normalize_text(title),
                )
                markets.append(nm)
        print(f"  Manifold: fetched {len(markets)} markets")
    except Exception as e:
        print(f"  Manifold: ERROR - {e}")
    return markets


# ---------------------------------------------------------------------------
# Matching engine
# ---------------------------------------------------------------------------

def find_matches(
    poly_markets: list[NormalizedMarket],
    other_markets: list[NormalizedMarket],
    min_similarity: float = 0.35,
) -> list[MatchResult]:
    """Find best matches between Polymarket and other platform markets."""
    results: list[MatchResult] = []
    used_other: set[int] = set()

    for pm in poly_markets:
        best_sim = 0.0
        best_idx = -1
        for i, om in enumerate(other_markets):
            if i in used_other:
                continue
            sim = jaccard_similarity(pm.keywords, om.keywords)
            if sim > best_sim:
                best_sim = sim
                best_idx = i
        if best_sim >= min_similarity and best_idx >= 0:
            used_other.add(best_idx)
            om = other_markets[best_idx]
            diff = abs(pm.probability - om.probability)
            results.append(MatchResult(
                polymarket={
                    "title": pm.title,
                    "probability": round(pm.probability, 4),
                    "url": pm.url,
                    "platform": pm.platform,
                },
                other={
                    "title": om.title,
                    "probability": round(om.probability, 4),
                    "url": om.url,
                    "platform": om.platform,
                },
                similarity=round(best_sim, 4),
                price_diff=round(diff, 4),
                flagged=diff > PRICE_DIFF_THRESHOLD,
            ))

    results.sort(key=lambda r: r.price_diff, reverse=True)
    return results


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def print_table(matches: list[MatchResult]) -> None:
    """Print a formatted table of matched markets."""
    if not matches:
        print("\nNo matches found across platforms.")
        return

    flagged = [m for m in matches if m.flagged]
    print(f"\n{'='*110}")
    print(f"  CROSS-PLATFORM PRICE COMPARISON  |  Total matches: {len(matches)}  |  "
          f"Flagged (>{PRICE_DIFF_THRESHOLD*100:.0f}% diff): {len(flagged)}")
    print(f"{'='*110}")

    hdr = f"{'Platform':<12} {'Poly %':>7} {'Other %':>7} {'Diff':>7} {'Sim':>5} {'Flag':>5}  {'Polymarket Question'}"
    print(hdr)
    print(f"{'-'*110}")

    for m in matches:
        flag_str = " <<<" if m.flagged else ""
        poly_pct = f"{m.polymarket['probability']*100:.1f}%"
        other_pct = f"{m.other['probability']*100:.1f}%"
        diff_pct = f"{m.price_diff*100:.1f}%"
        sim_pct = f"{m.similarity:.2f}"
        poly_title = m.polymarket["title"][:55]
        other_plat = m.other["platform"]
        print(f"{other_plat:<12} {poly_pct:>7} {other_pct:>7} {diff_pct:>7} {sim_pct:>5} {flag_str:>5}  {poly_title}")
        other_title = m.other["title"][:70]
        print(f"{'':>12} {'':>7} {'':>7} {'':>7} {'':>5} {'':>5}  -> {other_title}")

    print(f"{'='*110}")

    if flagged:
        print(f"\nFLAGGED OPPORTUNITIES (price diff > {PRICE_DIFF_THRESHOLD*100:.0f}%):\n")
        for i, m in enumerate(flagged, 1):
            print(f"  {i}. [{m.other['platform']}] diff={m.price_diff*100:.1f}%  sim={m.similarity:.2f}")
            print(f"     Poly:  {m.polymarket['title'][:80]}  ({m.polymarket['probability']*100:.1f}%)")
            print(f"     Other: {m.other['title'][:80]}  ({m.other['probability']*100:.1f}%)")
            print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    start = time.time()
    print("Cross-Platform Prediction Market Comparison")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"Price diff threshold: {PRICE_DIFF_THRESHOLD*100:.0f}%\n")
    print("Fetching markets from all platforms...")

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        # Fetch all platforms concurrently
        poly_task = fetch_polymarket(client)
        kalshi_task = fetch_kalshi(client)
        metaculus_task = fetch_metaculus(client)
        predictit_task = fetch_predictit(client)
        manifold_task = fetch_manifold(client)

        poly, kalshi, metaculus, predictit, manifold = await asyncio.gather(
            poly_task, kalshi_task, metaculus_task, predictit_task, manifold_task
        )

    # Combine all non-Polymarket markets
    all_other: list[NormalizedMarket] = kalshi + metaculus + predictit + manifold
    print(f"\nTotal: {len(poly)} Polymarket markets, {len(all_other)} other-platform markets")

    # Find matches per platform for better results
    all_matches: list[MatchResult] = []
    for platform_name, platform_markets in [
        ("Kalshi", kalshi),
        ("Metaculus", metaculus),
        ("PredictIt", predictit),
        ("Manifold", manifold),
    ]:
        if platform_markets:
            matches = find_matches(poly, platform_markets)
            print(f"  Matched with {platform_name}: {len(matches)} pairs "
                  f"({sum(1 for m in matches if m.flagged)} flagged)")
            all_matches.extend(matches)

    all_matches.sort(key=lambda r: r.price_diff, reverse=True)

    # Print table
    print_table(all_matches)

    # Save results
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": {
            "price_diff_threshold": PRICE_DIFF_THRESHOLD,
            "min_similarity": 0.35,
        },
        "summary": {
            "polymarket_count": len(poly),
            "kalshi_count": len(kalshi),
            "metaculus_count": len(metaculus),
            "predictit_count": len(predictit),
            "manifold_count": len(manifold),
            "total_matches": len(all_matches),
            "flagged_count": sum(1 for m in all_matches if m.flagged),
        },
        "matches": [asdict(m) for m in all_matches],
    }
    OUTPUT_FILE.write_text(json.dumps(output, indent=2))
    elapsed = time.time() - start
    print(f"\nResults saved to {OUTPUT_FILE}")
    print(f"Completed in {elapsed:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
