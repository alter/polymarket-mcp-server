#!/usr/bin/env python3
"""
Polymarket Saved Results Analyzer

Reads results.json from a full bias analysis run and searches for
exploitable patterns across multiple slices: category, volume tier,
outcome naming, time periods, etc.

Usage:
    python analyze_saved_results.py results.json
    python analyze_saved_results.py results.json --top 20
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

try:
    from scipy.stats import chisquare, binom_test  # noqa: F401
except ImportError:
    # binom_test deprecated in newer scipy, use binomtest
    pass

from scipy.stats import chisquare

try:
    from scipy.stats import binomtest
except ImportError:
    from scipy.stats import binom_test as binomtest


@dataclass
class Market:
    question: str
    outcomes: list[str]
    winner_index: int
    winner_name: str
    category: str
    volume: float
    end_date: str


def load_markets(path: str) -> list[Market]:
    with open(path) as f:
        data = json.load(f)
    return [Market(**m) for m in data["markets"]]


def bias_stats(markets: list[Market], label: str = "") -> Optional[dict]:
    """Compute bias stats for a list of markets. Returns None if n < 10."""
    n = len(markets)
    if n < 10:
        return None
    first_wins = sum(1 for m in markets if m.winner_index == 0)
    second_wins = n - first_wins
    first_pct = first_wins / n * 100

    chi2, p = chisquare([first_wins, second_wins])

    total_vol = sum(m.volume for m in markets)
    first_vol = sum(m.volume for m in markets if m.winner_index == 0)
    vw_first_pct = (first_vol / total_vol * 100) if total_vol > 0 else 50.0

    return {
        "label": label,
        "n": n,
        "first_wins": first_wins,
        "second_wins": second_wins,
        "first_pct": round(first_pct, 2),
        "chi2": round(float(chi2), 4),
        "p_value": round(float(p), 8),
        "significant": float(p) < 0.05,
        "total_volume": round(total_vol, 2),
        "vw_first_pct": round(vw_first_pct, 2),
    }


def edge_estimate(first_pct: float, n: int) -> dict:
    """Estimate theoretical edge from betting on the biased outcome.

    If first_pct < 50, the edge is betting on second outcome (typically "No").
    If first_pct > 50, the edge is betting on first outcome.
    Assumes fair odds (market price ~0.50) for simplicity.
    """
    deviation = abs(first_pct - 50)
    # At fair odds (0.50), expected value per $1 bet:
    # EV = win_rate * 1.0 - (1 - win_rate) * 1.0 = 2*win_rate - 1
    win_rate = max(first_pct, 100 - first_pct) / 100
    ev_per_dollar = 2 * win_rate - 1
    bet_on = "first (index 0)" if first_pct > 50 else "second (index 1)"

    # Kelly criterion: f* = (bp - q) / b where b=1 (even money), p=win_rate, q=1-p
    kelly_fraction = 2 * win_rate - 1  # simplified for even money

    # Confidence interval (approximate)
    import math
    se = math.sqrt(win_rate * (1 - win_rate) / n) if n > 0 else 0
    ci_low = round((win_rate - 1.96 * se) * 100, 2)
    ci_high = round((win_rate + 1.96 * se) * 100, 2)

    return {
        "deviation_from_50": round(deviation, 2),
        "bet_on": bet_on,
        "win_rate": round(win_rate * 100, 2),
        "ev_per_dollar": round(ev_per_dollar, 4),
        "kelly_fraction": round(kelly_fraction, 4),
        "win_rate_95ci": f"{ci_low}%-{ci_high}%",
    }


def print_section(title: str) -> None:
    print(f"\n{'─' * 80}")
    print(f"  {title}")
    print(f"{'─' * 80}")


def print_bias_row(s: dict, show_edge: bool = True) -> None:
    sig = " ***" if s["p_value"] < 0.001 else (" **" if s["p_value"] < 0.01 else (" *" if s["significant"] else ""))
    print(
        f"  {s['label']:<35} n={s['n']:>6,}  "
        f"1st={s['first_pct']:>5.1f}%  "
        f"p={s['p_value']:<10.6f}{sig}"
    )
    if show_edge and s["significant"]:
        e = edge_estimate(s["first_pct"], s["n"])
        print(
            f"  {'':35} Edge: bet {e['bet_on']}, "
            f"WR={e['win_rate']:.1f}% [{e['win_rate_95ci']}], "
            f"EV=${e['ev_per_dollar']:.4f}/$ "
            f"Kelly={e['kelly_fraction']:.2%}"
        )


def analyze_by_volume_tier(markets: list[Market]) -> list[dict]:
    tiers = [
        ("$0-100", 0, 100),
        ("$100-1K", 100, 1_000),
        ("$1K-10K", 1_000, 10_000),
        ("$10K-100K", 10_000, 100_000),
        ("$100K-1M", 100_000, 1_000_000),
        ("$1M+", 1_000_000, float("inf")),
    ]
    results = []
    for label, lo, hi in tiers:
        subset = [m for m in markets if lo <= m.volume < hi]
        s = bias_stats(subset, label)
        if s:
            results.append(s)
    return results


def analyze_by_category(markets: list[Market], min_n: int = 30) -> list[dict]:
    cats: dict[str, list[Market]] = defaultdict(list)
    for m in markets:
        cats[m.category].append(m)
    results = []
    for cat, ms in sorted(cats.items(), key=lambda x: -len(x[1])):
        s = bias_stats(ms, cat)
        if s and s["n"] >= min_n:
            results.append(s)
    return results


def analyze_by_outcome_name(markets: list[Market], min_n: int = 30) -> list[dict]:
    """Group by what the first outcome is named (Yes, team name, etc.)."""
    groups: dict[str, list[Market]] = defaultdict(list)
    for m in markets:
        name = m.outcomes[0] if m.outcomes else "Unknown"
        groups[name].append(m)
    results = []
    for name, ms in sorted(groups.items(), key=lambda x: -len(x[1])):
        s = bias_stats(ms, name)
        if s and s["n"] >= min_n:
            results.append(s)
    return results


def analyze_by_year(markets: list[Market]) -> list[dict]:
    years: dict[str, list[Market]] = defaultdict(list)
    for m in markets:
        if m.end_date:
            try:
                y = m.end_date[:4]
                if y.isdigit():
                    years[y].append(m)
            except (IndexError, ValueError):
                pass
    results = []
    for y, ms in sorted(years.items()):
        s = bias_stats(ms, y)
        if s:
            results.append(s)
    return results


def analyze_by_quarter(markets: list[Market]) -> list[dict]:
    quarters: dict[str, list[Market]] = defaultdict(list)
    for m in markets:
        if m.end_date and len(m.end_date) >= 7:
            try:
                y = m.end_date[:4]
                mo = int(m.end_date[5:7])
                q = (mo - 1) // 3 + 1
                quarters[f"{y}-Q{q}"].append(m)
            except (IndexError, ValueError):
                pass
    results = []
    for q, ms in sorted(quarters.items()):
        s = bias_stats(ms, q)
        if s:
            results.append(s)
    return results


def analyze_yes_no_only(markets: list[Market]) -> list[dict]:
    """Analyze only markets where outcomes are literally Yes/No."""
    yes_no = [
        m for m in markets
        if len(m.outcomes) == 2
        and m.outcomes[0].strip().lower() == "yes"
        and m.outcomes[1].strip().lower() == "no"
    ]
    yes_no_ids = set(id(m) for m in yes_no)
    non_yes_no = [m for m in markets if id(m) not in yes_no_ids]

    results = []
    s = bias_stats(yes_no, "Yes/No markets")
    if s:
        results.append(s)
    s = bias_stats(non_yes_no, "Non-Yes/No markets")
    if s:
        results.append(s)
    return results


def analyze_question_keywords(markets: list[Market], min_n: int = 50) -> list[dict]:
    """Look for keywords in questions that correlate with bias."""
    keywords = [
        "will", "before", "after", "above", "below", "over", "under",
        "more", "less", "win", "reach", "hit", "break", "exceed",
        "pass", "drop", "fall", "rise", "beat", "by", "between",
    ]
    results = []
    for kw in keywords:
        subset = [m for m in markets if kw.lower() in m.question.lower().split()]
        s = bias_stats(subset, f'"{kw}" in question')
        if s and s["n"] >= min_n:
            results.append(s)
    return results


def analyze_category_x_volume(
    markets: list[Market], top_categories: int = 10
) -> list[dict]:
    """Cross-slice: category x volume tier for highest-signal combos."""
    cats = Counter(m.category for m in markets)
    top_cats = [c for c, _ in cats.most_common(top_categories)]

    vol_tiers = [
        ("low(<1K)", 0, 1_000),
        ("mid(1K-100K)", 1_000, 100_000),
        ("high(100K+)", 100_000, float("inf")),
    ]

    results = []
    for cat in top_cats:
        for tier_label, lo, hi in vol_tiers:
            subset = [
                m for m in markets
                if m.category == cat and lo <= m.volume < hi
            ]
            label = f"{cat} / {tier_label}"
            s = bias_stats(subset, label)
            if s and s["n"] >= 30 and s["significant"]:
                results.append(s)
    return results


def find_best_edges(all_slices: list[dict], top: int = 20) -> list[dict]:
    """Find the most exploitable slices sorted by EV, filtered by significance."""
    significant = [s for s in all_slices if s["significant"] and s["n"] >= 30]
    for s in significant:
        e = edge_estimate(s["first_pct"], s["n"])
        s["edge"] = e
    significant.sort(key=lambda x: -x["edge"]["ev_per_dollar"])
    return significant[:top]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze saved Polymarket bias results for trading edges"
    )
    parser.add_argument("input", help="Path to results.json from analyze_voting_bias.py")
    parser.add_argument(
        "--top", type=int, default=20, help="Show top N edges (default: 20)"
    )
    args = parser.parse_args()

    print(f"Loading data from {args.input}...")
    markets = load_markets(args.input)
    print(f"Loaded {len(markets):,} resolved markets")

    print("\n" + "=" * 80)
    print("POLYMARKET BIAS ANALYSIS — TRADING EDGE FINDER")
    print("=" * 80)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Markets: {len(markets):,}")

    all_slices: list[dict] = []

    # 1. Overall
    print_section("OVERALL")
    s = bias_stats(markets, "ALL MARKETS")
    if s:
        print_bias_row(s)
        all_slices.append(s)

    # 2. Yes/No vs other naming
    print_section("YES/NO vs OTHER OUTCOME NAMES")
    for s in analyze_yes_no_only(markets):
        print_bias_row(s)
        all_slices.append(s)

    # 3. By volume tier
    print_section("BY VOLUME TIER")
    for s in analyze_by_volume_tier(markets):
        print_bias_row(s)
        all_slices.append(s)

    # 4. By category
    print_section("BY CATEGORY (n >= 30)")
    for s in analyze_by_category(markets, min_n=30):
        print_bias_row(s)
        all_slices.append(s)

    # 5. By year
    print_section("BY YEAR")
    for s in analyze_by_year(markets):
        print_bias_row(s)
        all_slices.append(s)

    # 6. By quarter
    print_section("BY QUARTER")
    for s in analyze_by_quarter(markets):
        print_bias_row(s)
        all_slices.append(s)

    # 7. By outcome name
    print_section("BY FIRST OUTCOME NAME (n >= 30)")
    for s in analyze_by_outcome_name(markets, min_n=30):
        print_bias_row(s, show_edge=False)
        all_slices.append(s)

    # 8. By question keywords
    print_section("BY QUESTION KEYWORD (n >= 50)")
    for s in analyze_question_keywords(markets, min_n=50):
        print_bias_row(s)
        all_slices.append(s)

    # 9. Category x Volume cross-slices (only significant)
    print_section("CATEGORY x VOLUME (significant only)")
    cross = analyze_category_x_volume(markets)
    for s in cross:
        print_bias_row(s)
        all_slices.append(s)

    # 10. TOP EDGES
    print(f"\n{'=' * 80}")
    print(f"  TOP {args.top} EXPLOITABLE EDGES (by EV per dollar at fair odds)")
    print(f"{'=' * 80}")
    print(
        f"  {'Slice':<35} {'N':>6}  {'WR':>6}  {'EV/$':>7}  "
        f"{'Kelly':>7}  {'95% CI':>15}  {'p-val':>10}"
    )
    print(f"  {'─' * 93}")

    best = find_best_edges(all_slices, top=args.top)
    for s in best:
        e = s["edge"]
        sig = "***" if s["p_value"] < 0.001 else ("**" if s["p_value"] < 0.01 else "*")
        print(
            f"  {s['label']:<35} {s['n']:>6,}  "
            f"{e['win_rate']:>5.1f}%  "
            f"${e['ev_per_dollar']:.4f}  "
            f"{e['kelly_fraction']:>6.2%}  "
            f"{e['win_rate_95ci']:>15}  "
            f"{s['p_value']:<10.6f} {sig}"
        )

    print(f"\n{'=' * 80}")
    print("INTERPRETATION NOTES:")
    print("─" * 80)
    print("  - EV/$ = expected value per dollar at FAIR ODDS (0.50). Real edge is lower")
    print("    because market prices already partially reflect bias.")
    print("  - Kelly = optimal bet fraction of bankroll at fair odds.")
    print("  - Practical strategy: bet small fixed amounts ($1-5) on the biased outcome")
    print("    in the identified market slices, only when market price is near 0.50.")
    print("  - The edge is strongest when market price DOESN'T reflect the bias.")
    print("  - Volume-weighted bias != count-based bias. A few large 'No' markets")
    print("    can skew volume stats even if count is ~50/50.")
    print("  - Multiple testing caveat: with many slices, some will be significant")
    print("    by chance. Prefer large N, low p-value, and consistent patterns.")
    print("=" * 80)


if __name__ == "__main__":
    main()
