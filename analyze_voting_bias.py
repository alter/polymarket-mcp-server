#!/usr/bin/env python3
"""
Polymarket Binary Market Bias Analysis

Analyzes resolved binary markets to determine if there's a systematic
preference for "Yes" vs "No" outcomes. Includes per-category breakdown
and volume-weighted statistics with chi-squared significance testing.

Usage:
    python analyze_voting_bias.py --max-markets 1000    # quick test
    python analyze_voting_bias.py                        # full run
    python analyze_voting_bias.py --output-json results.json --min-volume 1000
"""

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

import httpx

try:
    from scipy.stats import chisquare
except ImportError:
    print("scipy is required: pip install scipy")
    sys.exit(1)

GAMMA_API_URL = "https://gamma-api.polymarket.com"
PAGE_LIMIT = 100


@dataclass
class ResolvedMarket:
    question: str
    outcomes: list[str]
    winner_index: int
    winner_name: str
    category: str
    volume: float
    end_date: str


def determine_winner(market: dict) -> Optional[tuple[int, str]]:
    """Determine the winner of a binary market from outcomePrices.

    Returns (winner_index, winner_name) or None if no clear winner.
    """
    prices_raw = market.get("outcomePrices")
    if not prices_raw:
        return None

    if isinstance(prices_raw, str):
        try:
            prices = json.loads(prices_raw)
        except (json.JSONDecodeError, TypeError):
            return None
    else:
        prices = prices_raw

    if not prices or len(prices) < 2:
        return None

    outcomes_raw = market.get("outcomes")
    if isinstance(outcomes_raw, str):
        try:
            outcomes = json.loads(outcomes_raw)
        except (json.JSONDecodeError, TypeError):
            outcomes = ["Yes", "No"]
    elif isinstance(outcomes_raw, list):
        outcomes = outcomes_raw
    else:
        outcomes = ["Yes", "No"]

    if len(outcomes) < 2:
        return None

    try:
        p0 = float(prices[0])
        p1 = float(prices[1])
    except (ValueError, TypeError):
        return None

    if p0 >= 0.9:
        return (0, outcomes[0])
    elif p1 >= 0.9:
        return (1, outcomes[1])

    return None


async def fetch_all_resolved(
    client: httpx.AsyncClient,
    delay: float = 0.1,
    max_markets: Optional[int] = None,
) -> tuple[list[ResolvedMarket], dict]:
    """Fetch all closed binary markets with clear winners via pagination."""
    markets: list[ResolvedMarket] = []
    stats = {
        "total_fetched": 0,
        "total_binary": 0,
        "total_with_winner": 0,
        "total_skipped_no_winner": 0,
        "total_non_binary": 0,
        "pages_fetched": 0,
    }
    offset = 0

    while True:
        if max_markets and stats["total_fetched"] >= max_markets:
            break

        try:
            response = await client.get(
                f"{GAMMA_API_URL}/markets",
                params={
                    "closed": "true",
                    "limit": PAGE_LIMIT,
                    "offset": offset,
                },
            )
            response.raise_for_status()
            page = response.json()
        except httpx.HTTPError as e:
            print(f"\nHTTP error at offset {offset}: {e}")
            break
        except Exception as e:
            print(f"\nUnexpected error at offset {offset}: {e}")
            break

        if not page:
            break

        stats["pages_fetched"] += 1
        stats["total_fetched"] += len(page)

        for m in page:
            # Filter: binary markets only (exactly 2 outcomes)
            outcomes_raw = m.get("outcomes")
            if isinstance(outcomes_raw, str):
                try:
                    outcomes = json.loads(outcomes_raw)
                except (json.JSONDecodeError, TypeError):
                    stats["total_non_binary"] += 1
                    continue
            elif isinstance(outcomes_raw, list):
                outcomes = outcomes_raw
            else:
                stats["total_non_binary"] += 1
                continue

            if len(outcomes) != 2:
                stats["total_non_binary"] += 1
                continue

            stats["total_binary"] += 1

            winner = determine_winner(m)
            if winner is None:
                stats["total_skipped_no_winner"] += 1
                continue

            stats["total_with_winner"] += 1
            winner_index, winner_name = winner

            volume = 0.0
            for vol_key in ("volume", "volumeNum"):
                val = m.get(vol_key)
                if val is not None:
                    try:
                        volume = float(val)
                        break
                    except (ValueError, TypeError):
                        pass

            markets.append(
                ResolvedMarket(
                    question=m.get("question", ""),
                    outcomes=outcomes,
                    winner_index=winner_index,
                    winner_name=winner_name,
                    category=m.get("category", "Unknown") or "Unknown",
                    volume=volume,
                    end_date=m.get("endDate", ""),
                )
            )

        if max_markets and stats["total_fetched"] >= max_markets:
            break

        if len(page) < PAGE_LIMIT:
            break

        offset += PAGE_LIMIT

        if stats["pages_fetched"] % 10 == 0:
            print(
                f"  ... fetched {stats['total_fetched']} markets, "
                f"{stats['total_with_winner']} with clear winner "
                f"(page {stats['pages_fetched']})"
            )

        await asyncio.sleep(delay)

    return markets, stats


def compute_statistics(markets: list[ResolvedMarket], min_volume: float = 0) -> dict:
    """Compute overall, per-category, and volume-weighted bias statistics."""
    filtered = [m for m in markets if m.volume >= min_volume]

    if not filtered:
        return {"error": "No markets match the filters"}

    # --- Overall ---
    total = len(filtered)
    # winner_index == 0 typically means the first outcome (usually "Yes") won
    first_outcome_wins = sum(1 for m in filtered if m.winner_index == 0)
    second_outcome_wins = total - first_outcome_wins

    first_pct = first_outcome_wins / total * 100
    second_pct = second_outcome_wins / total * 100

    chi2, p_value = chisquare([first_outcome_wins, second_outcome_wins])

    overall = {
        "total": total,
        "first_outcome_wins": first_outcome_wins,
        "second_outcome_wins": second_outcome_wins,
        "first_outcome_pct": round(first_pct, 2),
        "second_outcome_pct": round(second_pct, 2),
        "chi_squared": round(float(chi2), 4),
        "p_value": round(float(p_value), 6),
        "significant": float(p_value) < 0.05,
    }

    # --- Volume-weighted ---
    total_volume = sum(m.volume for m in filtered)
    if total_volume > 0:
        first_volume = sum(m.volume for m in filtered if m.winner_index == 0)
        second_volume = total_volume - first_volume
        vw_first_pct = first_volume / total_volume * 100
        vw_second_pct = second_volume / total_volume * 100
    else:
        vw_first_pct = 0.0
        vw_second_pct = 0.0

    volume_weighted = {
        "total_volume": round(total_volume, 2),
        "first_outcome_volume": round(
            sum(m.volume for m in filtered if m.winner_index == 0), 2
        ),
        "second_outcome_volume": round(
            sum(m.volume for m in filtered if m.winner_index == 1), 2
        ),
        "first_outcome_pct": round(vw_first_pct, 2),
        "second_outcome_pct": round(vw_second_pct, 2),
    }

    # --- By category ---
    from collections import defaultdict

    cat_markets: dict[str, list[ResolvedMarket]] = defaultdict(list)
    for m in filtered:
        cat_markets[m.category].append(m)

    by_category = {}
    for cat, cat_list in sorted(cat_markets.items(), key=lambda x: -len(x[1])):
        n = len(cat_list)
        if n < 30:
            continue
        first_wins = sum(1 for m in cat_list if m.winner_index == 0)
        second_wins = n - first_wins
        cat_chi2, cat_p = chisquare([first_wins, second_wins])

        cat_volume = sum(m.volume for m in cat_list)
        by_category[cat] = {
            "total": n,
            "first_outcome_wins": first_wins,
            "second_outcome_wins": second_wins,
            "first_outcome_pct": round(first_wins / n * 100, 2),
            "second_outcome_pct": round(second_wins / n * 100, 2),
            "chi_squared": round(float(cat_chi2), 4),
            "p_value": round(float(cat_p), 6),
            "significant": float(cat_p) < 0.05,
            "total_volume": round(cat_volume, 2),
        }

    # --- Common outcome names ---
    from collections import Counter

    first_names = Counter(m.outcomes[0] for m in filtered)
    second_names = Counter(m.outcomes[1] for m in filtered)

    return {
        "overall": overall,
        "volume_weighted": volume_weighted,
        "by_category": by_category,
        "common_first_outcome_names": first_names.most_common(10),
        "common_second_outcome_names": second_names.most_common(10),
        "min_volume_filter": min_volume,
    }


def print_report(stats: dict, fetch_stats: dict) -> None:
    """Print a formatted report to the terminal."""
    print("\n" + "=" * 80)
    print("POLYMARKET BINARY MARKET BIAS ANALYSIS")
    print("=" * 80)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch stats
    print(f"\nData Collection:")
    print(f"  Pages fetched:         {fetch_stats['pages_fetched']}")
    print(f"  Total markets fetched: {fetch_stats['total_fetched']}")
    print(f"  Binary markets:        {fetch_stats['total_binary']}")
    print(f"  With clear winner:     {fetch_stats['total_with_winner']}")
    print(f"  No clear winner:       {fetch_stats['total_skipped_no_winner']}")
    print(f"  Non-binary (skipped):  {fetch_stats['total_non_binary']}")

    if "error" in stats:
        print(f"\n{stats['error']}")
        return

    # Overall
    o = stats["overall"]
    min_vol = stats["min_volume_filter"]
    if min_vol > 0:
        print(f"  Min volume filter:     ${min_vol:,.0f}")
    print(f"  Markets analyzed:      {o['total']}")

    # Common outcome names
    print(f"\nMost common outcome names:")
    print(f"  First outcome (index 0): ", end="")
    print(", ".join(f"{name} ({cnt})" for name, cnt in stats["common_first_outcome_names"][:5]))
    print(f"  Second outcome (index 1): ", end="")
    print(", ".join(f"{name} ({cnt})" for name, cnt in stats["common_second_outcome_names"][:5]))

    print(f"\n{'─' * 80}")
    print("OVERALL RESULTS")
    print(f"{'─' * 80}")
    print(f"  First outcome wins:  {o['first_outcome_wins']:>7,}  ({o['first_outcome_pct']:.2f}%)")
    print(f"  Second outcome wins: {o['second_outcome_wins']:>7,}  ({o['second_outcome_pct']:.2f}%)")
    print(f"  Chi-squared:         {o['chi_squared']:.4f}")
    print(f"  p-value:             {o['p_value']:.6f}")
    sig = "YES (p < 0.05)" if o["significant"] else "NO (p >= 0.05)"
    print(f"  Statistically significant: {sig}")

    # Volume-weighted
    vw = stats["volume_weighted"]
    print(f"\n{'─' * 80}")
    print("VOLUME-WEIGHTED RESULTS")
    print(f"{'─' * 80}")
    print(f"  Total volume:            ${vw['total_volume']:>15,.2f}")
    print(f"  First outcome volume:    ${vw['first_outcome_volume']:>15,.2f}  ({vw['first_outcome_pct']:.2f}%)")
    print(f"  Second outcome volume:   ${vw['second_outcome_volume']:>15,.2f}  ({vw['second_outcome_pct']:.2f}%)")

    # By category
    by_cat = stats["by_category"]
    if by_cat:
        print(f"\n{'─' * 80}")
        print("BY CATEGORY (n >= 30)")
        print(f"{'─' * 80}")
        header = f"  {'Category':<30} {'N':>6} {'1st%':>7} {'2nd%':>7} {'chi2':>8} {'p-val':>10} {'Sig':>4}"
        print(header)
        print(f"  {'─' * 76}")
        for cat, cs in by_cat.items():
            sig_mark = " *" if cs["significant"] else ""
            print(
                f"  {cat:<30} {cs['total']:>6,} {cs['first_outcome_pct']:>6.1f}% "
                f"{cs['second_outcome_pct']:>6.1f}% {cs['chi_squared']:>8.2f} "
                f"{cs['p_value']:>10.6f}{sig_mark}"
            )

    print(f"\n{'=' * 80}")
    print("* = statistically significant at p < 0.05")
    print("First outcome = index 0 (typically 'Yes'); Second = index 1 (typically 'No')")
    print("=" * 80)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze Yes/No bias in resolved Polymarket binary markets"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Delay between API requests in seconds (default: 0.1)",
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Max markets to fetch (default: all)",
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=100,
        help="Minimum trading volume to include a market (default: 100)",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Save results to JSON file",
    )
    args = parser.parse_args()

    limit_msg = f" (limit: {args.max_markets})" if args.max_markets else " (all)"
    print(f"Fetching closed binary markets{limit_msg}...")
    print(f"  Delay between requests: {args.delay}s")
    print(f"  Min volume filter: ${args.min_volume:,.0f}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        markets, fetch_stats = await fetch_all_resolved(
            client, delay=args.delay, max_markets=args.max_markets
        )

    print(f"\nCollected {len(markets)} resolved binary markets with clear winners.")
    print("Computing statistics...")

    stats = compute_statistics(markets, min_volume=args.min_volume)
    print_report(stats, fetch_stats)

    if args.output_json:
        output = {
            "generated_at": datetime.now().isoformat(),
            "fetch_stats": fetch_stats,
            "statistics": stats,
            "markets": [asdict(m) for m in markets],
        }
        # Convert Counter tuples to dicts for JSON serialization
        if "common_first_outcome_names" in output["statistics"]:
            output["statistics"]["common_first_outcome_names"] = [
                {"name": n, "count": c}
                for n, c in output["statistics"]["common_first_outcome_names"]
            ]
        if "common_second_outcome_names" in output["statistics"]:
            output["statistics"]["common_second_outcome_names"] = [
                {"name": n, "count": c}
                for n, c in output["statistics"]["common_second_outcome_names"]
            ]

        with open(args.output_json, "w") as f:
            json.dump(output, f, indent=2, default=str)
        print(f"\nResults saved to {args.output_json}")


if __name__ == "__main__":
    asyncio.run(main())
