#!/usr/bin/env python3
"""
Sport markets analysis — does each category have specific bias?

For each market in our resolved set, classify by sport, check:
- Resolution YES/NO rate per category
- Best mass_backtest indicator for category
- Per-category top variants

Goal: find sport-specific patterns invisible in aggregate.
"""
import json, re
from collections import defaultdict
import sys
sys.path.insert(0, ".")
from mass_backtest import load_resolutions, load_ticks


CATEGORIES = {
    "NHL": re.compile(r'\bNHL\b|Stanley\s*Cup|hockey', re.I),
    "NBA": re.compile(r'\bNBA\b|basketball|NBA Finals', re.I),
    "NFL": re.compile(r'\bNFL\b|Super\s*Bowl|football\s+(?:season|league)', re.I),
    "MLB": re.compile(r'\bMLB\b|baseball|World\s*Series', re.I),
    "Soccer-EU": re.compile(r'Premier\s*League|La\s*Liga|Bundesliga|Serie\s*A|Ligue\s*1|Champions\s*League|UEFA', re.I),
    "Soccer-WC": re.compile(r'World\s*Cup|FIFA', re.I),
    "Tennis": re.compile(r'\btennis\b|Wimbledon|US Open|French Open|Australian Open|Madrid Open|Roland Garros', re.I),
    "Golf": re.compile(r'\bgolf\b|Masters|PGA|tournament', re.I),
    "Crypto": re.compile(r'bitcoin|ethereum|crypto|btc|eth\b', re.I),
    "Politics-US": re.compile(r'trump|biden|harris|vance|republican|democrat|congress|senate|impeach', re.I),
    "Geopolitics": re.compile(r'russia|ukraine|israel|iran|china|north korea|nato|putin|xi|netanyahu', re.I),
    "Pop-Culture": re.compile(r'oscar|grammy|emmy|movie|album|tour|concert|tv|netflix|streaming', re.I),
    "Weather": re.compile(r'temperature|hurricane|storm|earthquake|tornado', re.I),
}


def categorize(question):
    for cat, pat in CATEGORIES.items():
        if pat.search(question):
            return cat
    return "Other"


def main():
    res = load_resolutions()
    ticks = load_ticks()
    print(f"Resolved markets with ticks: {len(set(res.keys()) & set(ticks.keys()))}")

    # Load gamma_market_meta to get questions
    meta = json.load(open("bot-data/gamma_market_meta.json"))

    # Categorize resolved markets with our data
    by_cat = defaultdict(list)
    for mid in res:
        m = meta.get(mid)
        if not m:
            continue
        q = m.get("q", "")
        if not q:
            continue
        cat = categorize(q)
        by_cat[cat].append({
            "id": mid,
            "question": q,
            "yes_won": res[mid]["yes_won"],
        })

    print(f"\n━━━ Resolution YES rate by category ━━━")
    print(f"  {'category':<14} {'n':>4} {'YES':>5} {'NO':>5} {'YES rate':>9}")
    for cat in sorted(by_cat, key=lambda c: -len(by_cat[c])):
        lst = by_cat[cat]
        if len(lst) < 5:
            continue
        n = len(lst)
        yes = sum(1 for m in lst if m["yes_won"])
        rate = yes / n * 100
        print(f"  {cat:<14} {n:>4} {yes:>5} {n-yes:>5} {rate:>7.1f}%")

    # Now check: for top variants, do they perform differently per category?
    # Need to load mass_backtest results per market per variant — but we have only aggregated.
    # Use the variant's per-market split via re-running on filtered subsets.
    # For speed: just compute base rate and bias per category.

    # Identify "always-NO" easy categories (where bet NO on every market wins big)
    print(f"\n━━━ Always-NO bet ROI per category ━━━")
    print(f"  {'category':<14} {'n':>4} {'NO_wins':>8} {'avg_entry':>10} {'NO_ROI':>9}")
    for cat in sorted(by_cat, key=lambda c: -len(by_cat[c])):
        lst = by_cat[cat]
        if len(lst) < 5:
            continue
        n = len(lst)
        no_wins = sum(1 for m in lst if not m["yes_won"])
        # Approximate entry: use first tick mid
        avg_entry = []
        for m in lst:
            mid = m["id"]
            if mid in ticks:
                # First mid value, NO entry = 1 - bid ≈ 1 - mid_first
                first_mid = ticks[mid]["mid"][0] if len(ticks[mid]["mid"]) > 0 else None
                if first_mid is not None and 0 < first_mid < 1:
                    no_entry = 1 - first_mid
                    avg_entry.append(no_entry)
        if not avg_entry:
            continue
        avg_e = sum(avg_entry) / len(avg_entry)
        no_rate = no_wins / n
        # Approx ROI: pay avg_e, win pays 1 (NO share = 1 if NO wins)
        # ROI = no_rate / avg_e - 1
        roi = (no_rate / avg_e - 1) * 100 if avg_e > 0 else 0
        print(f"  {cat:<14} {n:>4} {no_wins:>8} {avg_e:>9.3f} {roi:>+8.1f}%")


if __name__ == "__main__":
    main()
