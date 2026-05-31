#!/usr/bin/env python3
"""
Auto-tune analyzer for hybrid/forest strategies.

Reads arena_trades.jsonl and finds best parameter combinations per indicator family.
Writes recommendations to bot-data/autotune_report.json.

Runs retrospective analysis — doesn't change running strategies. Use output
to manually update multi_strategy.py generator for next deployment.
"""
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

TRADES = Path("bot-data/arena_trades.jsonl")
REPORT = Path("bot-data/autotune_report.json")


def load_trades():
    trades = []
    with open(TRADES) as f:
        for line in f:
            try:
                trades.append(json.loads(line))
            except Exception:
                continue
    return trades


def parse_strategy_name(name):
    """Parse 'S123|mean_re|p5|e0.01|sl-25%|tp10%|free|b' into dict."""
    parts = name.split("|")
    if len(parts) < 7:
        return None
    try:
        period = int(parts[2].lstrip("p")) if parts[2].startswith("p") else None
        entry = float(parts[3].lstrip("e"))
        sl_raw = parts[4].lstrip("sl")
        if "off" in sl_raw:
            sl = -99.0
        else:
            sl = -float(sl_raw.rstrip("%")) / 100
        tp = float(parts[5].lstrip("tp").rstrip("%")) / 100
        fees = parts[6]
        return {"period": period, "entry": entry, "sl": sl, "tp": tp, "fees": fees}
    except Exception:
        return None


def tune_by_params(trades, indicator_filter):
    """Group trades by (period, entry, sl, tp, fees) and rank."""
    groups = defaultdict(list)
    for t in trades:
        ind = t.get("indicator", "")
        if not ind.startswith(indicator_filter):
            continue
        p = parse_strategy_name(t.get("strategy_name", ""))
        if not p:
            continue
        key = (p["period"], p["entry"], p["sl"], p["tp"], p["fees"])
        groups[key].append(t)

    results = []
    for key, ts in groups.items():
        if len(ts) < 30:  # need minimum sample
            continue
        wins = sum(1 for t in ts if t["pnl"] > 0)
        losses = len(ts) - wins
        pnl_total = sum(t["pnl"] for t in ts)
        gross_win = sum(t["pnl"] for t in ts if t["pnl"] > 0)
        gross_loss = abs(sum(t["pnl"] for t in ts if t["pnl"] <= 0))
        pf = gross_win / max(gross_loss, 0.01)
        results.append({
            "period": key[0], "entry": key[1], "sl": key[2],
            "tp": key[3], "fees": key[4],
            "n": len(ts), "wins": wins, "losses": losses,
            "wr": round(wins / len(ts) * 100, 1),
            "total_pnl": round(pnl_total, 2),
            "avg": round(pnl_total / len(ts), 3),
            "pf": round(pf, 2),
        })
    return sorted(results, key=lambda x: -x["avg"])


def tune_hybrid(trades):
    """Find best hybrid (wavelet+BB+zscore) params across existing deployments.
    For now hybrids don't have log history — simulate from forest log as proxy."""
    # Placeholder: actual hybrid tuning will require hybrid trades to accumulate
    # For now, report top wavelet + BB + zscore combinations that appear together
    pass


def generate_recommendations():
    trades = load_trades()
    print(f"Loaded {len(trades):,} trades")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_trades_analyzed": len(trades),
    }

    for fam in ["mean_rev_ema", "wavelet", "bollinger", "zscore", "rsi",
                "forest", "ensemble", "hybrid"]:
        ranked = tune_by_params(trades, fam)
        if not ranked:
            continue
        report[fam] = {
            "top_10": ranked[:10],
            "bottom_5": ranked[-5:],
        }
        print(f"\n━━━ {fam.upper()} ({len(ranked)} param combos, ≥30 trades each) ━━━")
        print(f"  {'period':>6} {'entry':>7} {'sl':>7} {'tp':>6} {'fees':>4} | "
              f"{'n':>5} {'wr%':>5} {'avg$':>8} {'pf':>5}")
        for r in ranked[:5]:
            sl_s = f"{r['sl']:.2f}" if r["sl"] > -90 else "off"
            print(f"  {str(r['period']):>6} {r['entry']:>7.3f} {sl_s:>7} "
                  f"{r['tp']:>5.0%} {r['fees']:>4} | {r['n']:>5} "
                  f"{r['wr']:>5.1f} {r['avg']:>+7.3f} {r['pf']:>5.2f}")

        print(f"  Worst 3:")
        for r in ranked[-3:]:
            sl_s = f"{r['sl']:.2f}" if r["sl"] > -90 else "off"
            print(f"  {str(r['period']):>6} {r['entry']:>7.3f} {sl_s:>7} "
                  f"{r['tp']:>5.0%} {r['fees']:>4} | {r['n']:>5} "
                  f"{r['wr']:>5.1f} {r['avg']:>+7.3f} {r['pf']:>5.2f}")

    # Save report
    with open(REPORT, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n✓ Saved to {REPORT}")

    # Print suggested next-iteration grid
    print("\n" + "="*70)
    print("RECOMMENDED PARAM TUPLES FOR NEXT STRATEGY GENERATION")
    print("="*70)
    for fam in ["mean_rev_ema", "wavelet", "bollinger", "zscore"]:
        if fam not in report:
            continue
        tops = report[fam]["top_10"][:3]
        if not tops:
            continue
        print(f"\n  {fam}:")
        for r in tops:
            sl_s = f"{r['sl']:.2f}" if r["sl"] > -90 else "off"
            print(f"    period={r['period']}, entry={r['entry']}, sl={sl_s}, "
                  f"tp={r['tp']:.0%}, fees={r['fees']}  "
                  f"(n={r['n']}, avg ${r['avg']:+.3f})")


if __name__ == "__main__":
    generate_recommendations()
