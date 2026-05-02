#!/usr/bin/env python3
"""
Daily performance report — comprehensive snapshot of all bots' state.
Run as cron job or on demand. Output to bot-data/daily_report_YYYYMMDD.md

Includes:
- Live validator: top variants by equity, retired count, total close stats
- Arena: top strategies, profitable count, indicator family breakdown
- All sub-bots: position counts, realized PnL
- Phase 3 OBI: latest results summary
- News pipeline: signals captured, matched, accuracy if measurable
- System health: file ages, RAM proxy
"""
import json, os, time
from collections import Counter, defaultdict
from datetime import datetime, timezone


DATA = "data" if os.path.exists("data") else "bot-data"


def main():
    now_utc = datetime.now(timezone.utc)
    date_str = now_utc.strftime("%Y-%m-%d")
    fname = f"{DATA}/daily_report_{now_utc.strftime('%Y%m%d')}.md"

    md = [f"# Polymarket Arena Daily Report — {date_str}\n"]
    md.append(f"_Generated: {now_utc.strftime('%Y-%m-%d %H:%M UTC')}_\n")

    # === 1. Watchdog ===
    md.append("## 1. System health\n")
    if os.path.exists(f"{DATA}/watchdog.json"):
        wd = json.load(open(f"{DATA}/watchdog.json"))
        md.append(f"- **stalled**: {wd.get('stalled')}, alerts: {wd.get('alerts')}")
        md.append(f"- **phase3 status**: {wd.get('phase3', '?')}")
        md.append("\n**File ages:**")
        for fn, st in wd.get("files", {}).items():
            age = st.get("age_sec", "missing")
            sz = st.get("size_kb", "-")
            md.append(f"- {fn}: age={age}s, size={sz}KB")
    md.append("")

    # === 2. Live Validator ===
    md.append("## 2. Live Validator\n")
    if os.path.exists(f"{DATA}/live_validator.json"):
        lv = json.load(open(f"{DATA}/live_validator.json"))
        variants = lv["variants"]
        retired = sum(1 for v in variants if v.get("retired"))
        alive = [v for v in variants if not v.get("retired")]
        n_open = sum(len(v.get("open_cids", {})) for v in variants)
        n_closed = sum(v.get("wins_live", 0) + v.get("losses_live", 0) for v in variants)
        realized = sum(v.get("realized_pnl_live", 0) for v in variants)
        alive_eq = sum(v.get("equity", 1000) for v in alive)
        ex = {}
        for v in variants:
            for r, c in v.get("exit_reason_counts", {}).items():
                ex[r] = ex.get(r, 0) + c
        w3 = sum(v.get("win3_skips", 0) for v in variants)

        md.append(f"- **Variants:** {len(variants)} total, {len(alive)} alive, {retired} retired")
        md.append(f"- **Positions:** {n_open} open, {n_closed} closed")
        md.append(f"- **Realized PnL:** ${realized:+.4f} (actual $0.01 bets)")
        md.append(f"- **Aggregate equity:** ${alive_eq:.0f} / ${len(alive)*1000} starting "
                  f"= **{(alive_eq/(len(alive)*1000)-1)*100:+.2f}%**")
        md.append(f"- **Exit reasons:** {ex}")
        md.append(f"- **Win3 skips:** {w3}")

        # Family breakdown
        fams = Counter()
        for v in alive:
            fams[v["variant"].split("_")[0]] += 1
        md.append(f"- **Alive families:** {dict(fams)}")

        # Top 10
        eligible = [v for v in alive
                    if (v.get("wins_live", 0) + v.get("losses_live", 0)) >= 10]
        eligible.sort(key=lambda v: -v.get("equity", 1000))
        md.append(f"\n**Top 10 alive variants (n>=10 closes):**\n")
        md.append("| Variant | n | WR | LIVE ROI | BT | Equity |")
        md.append("|---------|----|------|---------|-----|--------|")
        for v in eligible[:10]:
            n = v["wins_live"] + v["losses_live"]
            wr = v["wins_live"] / n * 100
            roi = v["realized_pnl_live"] / (n * 0.01) * 100
            md.append(f"| `{v['variant'][:60]}` | {n} | {wr:.0f}% | "
                     f"{roi:+.1f}% | {v.get('backtest_roi',0):+.1f}% | "
                     f"${v.get('equity', 1000):.0f} |")

    md.append("")

    # === 3. Arena ===
    md.append("## 3. Arena (multi_strategy)\n")
    if os.path.exists(f"{DATA}/arena_results.json"):
        ar = json.load(open(f"{DATA}/arena_results.json"))
        results = ar["results"]
        prof = sum(1 for r in results if r.get("equity", 0) > 1000)
        active = ar.get("active", 0)
        # Top 10
        results.sort(key=lambda r: -r.get("equity", 0))
        md.append(f"- **Total:** {len(results)} strategies, {active} active, {prof} profitable")
        # Family breakdown
        fams = Counter()
        for r in results:
            nm = r["name"].split("|")
            if len(nm) > 1:
                fams[nm[1]] += 1
        md.append(f"- **Family distribution:** {dict(fams)}")
        # Top by equity
        md.append(f"\n**Top 10 by equity:**\n")
        md.append("| Strategy | Equity | W/L | WR |")
        md.append("|----------|--------|------|----|")
        for r in results[:10]:
            n = r.get("wins", 0) + r.get("losses", 0)
            wr = r.get("wins", 0) / max(n, 1) * 100
            md.append(f"| `{r['name'][:55]}` | ${r.get('equity', 0):.0f} | "
                     f"{r.get('wins', 0)}/{r.get('losses', 0)} | {wr:.0f}% |")
    md.append("")

    # === 4. Sub-bots ===
    md.append("## 4. Sub-bots\n")
    for fn, label in [
        ("political_skeptic.json", "Political Skeptic (Strategy A+B)"),
        ("news_trader.json", "News Trader (Strategy C)"),
        ("theta_decay.json", "Theta Decay"),
    ]:
        p = f"{DATA}/{fn}"
        if not os.path.exists(p):
            continue
        d = json.load(open(p))
        wins = d.get("wins", 0)
        losses = d.get("losses", 0)
        n = wins + losses
        wr = wins / max(n, 1) * 100 if n else 0
        eq = d.get("equity_arena_scale", 1000)
        n_open = len(d.get("open_positions", {}))
        pnl = d.get("realized_pnl", 0)
        md.append(f"- **{label}:** open={n_open}, W/L={wins}/{losses} (WR {wr:.0f}%), "
                  f"realized=${pnl:+.4f}, equity=${eq:.0f}")
    md.append("")

    # Always-NO bots
    md.append("**Always-NO variants:**\n")
    for v in ["v1_fee_free", "v2_with_fees", "v3_aggressive", "v4_conservative", "v5_all_cats"]:
        p = f"{DATA}/always_no_{v}.json"
        if os.path.exists(p):
            d = json.load(open(p))
            wins = d.get("wins", 0); losses = d.get("losses", 0)
            n = wins + losses
            wr = wins / max(n, 1) * 100 if n else 0
            md.append(f"- {v}: W/L={wins}/{losses} (WR {wr:.0f}%), pnl=${d.get('realized_pnl', 0):+.4f}")
    md.append("")

    # === 5. Phase 3 ===
    md.append("## 5. Phase 3 OBI/microprice\n")
    p3 = f"{DATA}/phase3_obi_results.json"
    if os.path.exists(p3):
        d = json.load(open(p3))
        age_h = (time.time() - os.path.getmtime(p3)) / 3600
        md.append(f"- **Last run:** {d.get('ran_at', '?')} ({age_h:.1f}h ago)")
        md.append(f"- **Pairs:** {d.get('n_pairs', 0):,} from {d.get('n_markets', 0)} markets")
        md.append("- **Correlations:**")
        for k, v in sorted(d.get("correlations", {}).items()):
            md.append(f"  - {k}: {v:+.4f}")
    md.append("")

    # === 6. News pipeline ===
    md.append("## 6. News pipeline\n")
    if os.path.exists(f"{DATA}/news_signals.jsonl"):
        n_sig = sum(1 for _ in open(f"{DATA}/news_signals.jsonl"))
        md.append(f"- **News signals captured:** {n_sig}")
    md.append("")

    # === 7. Whale fade ===
    if os.path.exists(f"{DATA}/whale_fade_grid.json"):
        wf = json.load(open(f"{DATA}/whale_fade_grid.json"))
        vars_data = wf.get("variants", wf.get("results", []))
        if isinstance(vars_data, dict):
            vars_data = list(vars_data.values())
        n_open = sum(len(v.get("open_cids", [])) for v in vars_data)
        n_total_closed = sum(v.get("wins", 0) + v.get("losses", 0) for v in vars_data)
        n_pnl = sum(v.get("realized_pnl", 0) for v in vars_data)
        md.append("## 7. Whale Fade Grid\n")
        md.append(f"- **Variants:** {len(vars_data)}")
        md.append(f"- **Open:** {n_open}, Closed: {n_total_closed}, Realized: ${n_pnl:+.4f}")
    md.append("")

    # Save
    with open(fname, "w") as f:
        f.write("\n".join(md))
    print(f"Saved to {fname}")
    print(f"Lines: {len(md)}")


if __name__ == "__main__":
    main()
