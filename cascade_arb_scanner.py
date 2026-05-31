#!/usr/bin/env python3
"""
Deadline cascade arbitrage scanner.

For markets like "Iran ceasefire by April 27" / "by April 30" / "by May 31":
  Logically: P(by April 27) ≤ P(by April 30) ≤ P(by May 31)
  If market violates this (later deadline cheaper than earlier), it's free money.

Strategy:
  1. Fetch all active markets
  2. Group by "base question" (strip dates)
  3. Within each group, sort by end_date
  4. Detect monotonicity violations on YES prices
  5. Compute risk-free profit per pair

Output: bot-data/cascade_arb_opportunities.json
"""
import asyncio, json, os, re
from collections import defaultdict
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
DATA = "bot-data"
RESULTS_FILE = os.path.join(DATA, "cascade_arb_opportunities.json")


# ─── Question normalization ─────────────────────────────────────────────────

DATE_PATTERNS = [
    r'\bby (january|february|march|april|may|june|july|august|september|october|november|december) \d{1,2}(?:,? \d{4})?\b',
    r'\bbefore (january|february|march|april|may|june|july|august|september|october|november|december) \d{1,2}(?:,? \d{4})?\b',
    r'\bby end of (january|february|march|april|may|june|july|august|september|october|november|december)\b',
    r'\bby end of \d{4}\b',
    r'\bby \d{4}\b',
    r'\bbefore \d{4}\b',
    r'\bin (january|february|march|april|may|june|july|august|september|october|november|december)\b',
    r'\bon \d{4}-\d{2}-\d{2}\b',
    r'\b\d{4}-\d{2}-\d{2}\b',
    r'\b(january|february|march|april|may|june|july|august|september|october|november|december) \d{1,2},? \d{4}\b',
    r'\b(january|february|march|april|may|june|july|august|september|october|november|december) \d{1,2}\b',
    r'\b(april|may|june) \d{1,2}-\d{1,2}\b',
]


def normalize_question(q):
    """Strip date references → 'base' question for grouping."""
    base = q.lower().strip()
    for pat in DATE_PATTERNS:
        base = re.sub(pat, '<DATE>', base)
    # Multiple spaces and date placeholders consolidated
    base = re.sub(r'\s+', ' ', base)
    base = re.sub(r'(<DATE>)+', '<DATE>', base)
    base = base.strip(' ?.')
    return base


def get_yes_price(mkt):
    op = mkt.get("outcomePrices", "")
    if isinstance(op, str):
        try: op = json.loads(op)
        except: return None
    if op and len(op) >= 1:
        try: return float(op[0])
        except: pass
    return None


def parse_end_date(end_str):
    if not end_str:
        return None
    try:
        return datetime.fromisoformat(end_str.replace("Z", "+00:00"))
    except:
        return None


# ─── Main scanner ───────────────────────────────────────────────────────────

async def fetch_active_markets(client, total=1000):
    markets = []
    for offset in range(0, total, 100):
        try:
            r = await client.get(f"{GAMMA}/markets", params={
                "active": "true", "closed": "false",
                "limit": 100, "offset": offset,
                "order": "volume24hr", "ascending": "false",
            })
            if r.status_code != 200:
                break
            batch = r.json()
            if not batch:
                break
            markets.extend(batch)
        except Exception as e:
            print(f"  fetch err: {e}")
            break
    return markets


def parse_question_deadline(q):
    """Extract the deadline date FROM the question text.
    Only matches strict 'by/before <date>' patterns — NOT 'on <date>' (point-in-time)
    or 'between X and Y' (window). Returns datetime or None.
    """
    ql = q.lower()
    # Reject point-in-time and window questions
    if re.search(r'\bon (january|february|march|april|may|june|july|august|september|october|november|december) \d', ql):
        return None
    if re.search(r'\bon \d{4}-\d{2}-\d{2}\b', ql):
        return None
    if re.search(r'\bbetween .+ and .+\b', ql):
        return None
    if re.search(r'\bfrom .+ to .+\b', ql):
        return None
    if re.search(r'\b\w+ \d{1,2}-\d{1,2}\b', ql):  # "April 6-12" range
        return None

    # Match by/before <month> <day>[, year]
    months = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
              "july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
    m = re.search(r'\b(?:by|before)\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})(?:,?\s*(\d{4}))?\b', ql)
    if m:
        month = months[m.group(1)]
        day = int(m.group(2))
        year = int(m.group(3)) if m.group(3) else 2026
        try:
            return datetime(year, month, day, tzinfo=timezone.utc)
        except ValueError:
            return None
    # "by end of <month>"
    m = re.search(r'\bby end of (january|february|march|april|may|june|july|august|september|october|november|december)(?:,?\s*(\d{4}))?\b', ql)
    if m:
        month = months[m.group(1)]
        year = int(m.group(2)) if m.group(2) else 2026
        # End of month — use 28th conservatively
        return datetime(year, month, 28, tzinfo=timezone.utc)
    # "by end of <year>"
    m = re.search(r'\bby end of (\d{4})\b', ql)
    if m:
        return datetime(int(m.group(1)), 12, 31, tzinfo=timezone.utc)
    # "by <year>"
    m = re.search(r'\bby (\d{4})\b', ql)
    if m:
        return datetime(int(m.group(1)), 12, 31, tzinfo=timezone.utc)
    return None


def find_cascades(markets):
    """Group markets by base question, find monotonicity violations.
    Only includes 'by/before <date>' questions (true cumulative events)."""
    by_base = defaultdict(list)
    for m in markets:
        q = m.get("question", "")
        base = normalize_question(q)
        if "<DATE>" not in base:
            continue
        # CRITICAL: parse deadline from QUESTION, not end_date
        question_deadline = parse_question_deadline(q)
        if not question_deadline:
            continue
        yp = get_yes_price(m)
        if yp is None or yp <= 0.02 or yp >= 0.98:
            continue
        cid = m.get("conditionId", "")
        by_base[base].append({
            "cid": cid, "q": q, "yes_p": yp,
            "question_deadline": question_deadline,
            "end": parse_end_date(m.get("endDate", "")),
            "fees_on": m.get("feesEnabled", False),
            "vol24": float(m.get("volume24hr", 0) or 0),
        })

    # Filter to groups with 2+ markets, all with valid deadlines
    cascades = {b: ms for b, ms in by_base.items() if len(ms) >= 2}
    return cascades


def detect_violations(cascade):
    """Within cascade sorted by QUESTION DEADLINE, find pairs where earlier
    has higher price than later (logically impossible for cumulative events)."""
    sorted_m = sorted(cascade, key=lambda m: m["question_deadline"])
    violations = []
    for i in range(len(sorted_m)):
        for j in range(i+1, len(sorted_m)):
            m_early = sorted_m[i]
            m_late = sorted_m[j]
            # P(early deadline) should be ≤ P(late deadline)
            if m_early["yes_p"] > m_late["yes_p"] + 0.01:  # need ≥1% gap
                gap = m_early["yes_p"] - m_late["yes_p"]
                # Risk-free arb: SELL YES on early ($yp_early), BUY YES on late ($yp_late)
                # Or equivalently: BUY NO on early at (1-yp_early), BUY YES on late at yp_late
                # If event happens by EARLY date: early=YES (lose NO bet), late=YES (win YES bet)
                # If event happens between early-late: early=NO (win NO bet), late=YES (win YES bet)
                # If event never: early=NO (win NO bet), late=NO (lose YES bet)
                # All three scenarios: at least one wins, sometimes both
                # Cost: (1-yp_early) + yp_late
                # Worst payout: $1 (one of them wins for sure if event ever happens within late window)
                # If event NEVER happens: NO_early wins ($1), YES_late loses → payout = $1, cost = (1-yp_early) + yp_late
                #
                # Better formulation (more general):
                # Buy NO_early (cost: 1-p_early) - pays $1 if event NOT by early date
                # Sell YES_late (or BUY NO_late, cost: 1-p_late) - we keep p_late, owe $1 if YES_late wins
                # Wait this is getting complex. Simple form:
                #
                # PAIR TRADE:
                #   Buy 1 share YES_late at p_late
                #   Buy 1 share NO_early at (1-p_early)
                #   Cost = p_late + (1-p_early)
                #
                # Outcomes:
                #   Event by early: YES_late wins ($1), NO_early loses ($0). Total: $1
                #   Event between early-late: YES_late wins ($1), NO_early wins ($1). Total: $2
                #   Event after late or never: YES_late loses ($0), NO_early wins ($1). Total: $1
                #
                # Min payout = $1, max = $2
                # Profit if cost < 1: at minimum break-even, often more
                # Cost = p_late + (1-p_early) = 1 - (p_early - p_late)
                # If p_early > p_late, cost < 1, guaranteed profit ≥ (p_early - p_late)
                cost = m_late["yes_p"] + (1 - m_early["yes_p"])
                min_payout = 1.0  # pessimistic case (event by early date)
                guaranteed_profit = min_payout - cost  # = p_early - p_late
                violations.append({
                    "type": "deadline_cascade_violation",
                    "early_q": m_early["q"],
                    "early_deadline": m_early["question_deadline"].isoformat(),
                    "early_p": m_early["yes_p"], "early_cid": m_early["cid"],
                    "early_fees": m_early["fees_on"],
                    "late_q": m_late["q"],
                    "late_deadline": m_late["question_deadline"].isoformat(),
                    "late_p": m_late["yes_p"], "late_cid": m_late["cid"],
                    "late_fees": m_late["fees_on"],
                    "gap_pct": round(gap * 100, 2),
                    "cost_per_pair": round(cost, 4),
                    "guaranteed_profit_per_pair": round(guaranteed_profit, 4),
                    "expected_min_roi": round(guaranteed_profit / cost * 100, 2),
                    "vol24_min": min(m_early["vol24"], m_late["vol24"]),
                })
    return violations


async def main():
    os.makedirs(DATA, exist_ok=True)
    print(f"[{datetime.now():%H:%M:%S}] Fetching active markets...")
    client = httpx.AsyncClient(timeout=30.0)
    markets = await fetch_active_markets(client, total=1000)
    print(f"  Fetched {len(markets)} markets")

    cascades = find_cascades(markets)
    print(f"  Found {len(cascades)} multi-deadline groups")

    all_violations = []
    cascade_summary = []
    for base, ms in cascades.items():
        viols = detect_violations(ms)
        if viols:
            all_violations.extend(viols)
            cascade_summary.append({
                "base": base,
                "n_markets": len(ms),
                "n_violations": len(viols),
            })

    print(f"\n  Cascades with violations: {len(cascade_summary)}")
    print(f"  Total arbitrage opportunities: {len(all_violations)}")

    # Sort by guaranteed profit
    all_violations.sort(key=lambda v: -v["guaranteed_profit_per_pair"])

    # Save
    output = {
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "total_markets_scanned": len(markets),
        "n_cascades_found": len(cascades),
        "n_cascades_with_violations": len(cascade_summary),
        "n_arb_opportunities": len(all_violations),
        "cascade_summary": cascade_summary,
        "violations": all_violations,
    }
    with open(RESULTS_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved to {RESULTS_FILE}")

    # Print top opportunities
    if all_violations:
        print(f"\n━━━ TOP 15 ARBITRAGE OPPORTUNITIES ━━━")
        print(f"{'Gap':>6} {'Profit':>8} {'ROI':>6} {'Vol24':>8}  Markets")
        for v in all_violations[:15]:
            print(f"  {v['gap_pct']:>4.1f}%  ${v['guaranteed_profit_per_pair']:>6.4f}  "
                  f"{v['expected_min_roi']:>4.1f}%  ${v['vol24_min']:>7,.0f}")
            print(f"    EARLY deadline {v['early_deadline'][:10]} (p={v['early_p']:.3f}, fees={v['early_fees']}): "
                  f"{v['early_q'][:65]}")
            print(f"    LATE  deadline {v['late_deadline'][:10]} (p={v['late_p']:.3f}, fees={v['late_fees']}): "
                  f"{v['late_q'][:65]}")
            print()
    else:
        print(f"\n  No violations found. All cascades are properly priced.")
        # Show a sample cascade for context
        large_cascades = sorted(cascades.items(), key=lambda x: -len(x[1]))[:5]
        print(f"\n  Sample cascades (largest groups):")
        for base, ms in large_cascades:
            print(f"\n  Base: {base[:80]}")
            ms_sorted = sorted(ms, key=lambda m: m["question_deadline"])
            for m in ms_sorted[:8]:
                print(f"    deadline {m['question_deadline'].strftime('%Y-%m-%d')}  "
                      f"p={m['yes_p']:.3f}  | {m['q'][:65]}")

    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
