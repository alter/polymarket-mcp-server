#!/usr/bin/env python3
"""
Equity-curve snapshotter — the missing time-series that unblocks max-DD / Calmar /
Sharpe reporting (mandatory per CLAUDE.md reporting rules).

WHY THIS EXISTS: every bot state file holds only a CURRENT snapshot (+ peak_equity
running max for arena). With no equity TIME-SERIES, true path max-DD and Calmar are
uncomputable — we could only ever report current-DD-from-peak. This process reads
the state files the bots already write and appends (ts, equity) rows so a real
equity curve accumulates. After ~30d of honest-fill data the report mode below
produces genuine max-DD / Calmar / Sharpe instead of "insufficient history".

It does NOT touch any bot — pure reader. Decoupled on purpose.

Usage:
    python equity_snapshotter.py            # daemon: append a snapshot every INTERVAL
    python equity_snapshotter.py --once     # single snapshot, then exit
    python equity_snapshotter.py --report           # metrics for all entities
    python equity_snapshotter.py --report S306 BO_p20_fade   # specific ids

State written: bot-data/equity_curve.jsonl  (one row per entity per cycle)
Row schema: {"ts","iso","src","id","equity","realized","wins","losses","open"}
"""
import glob
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

DATA = os.environ.get("EQUITY_DATA_DIR", "data")  # container uses /app/data; local tests pass bot-data
CURVE = os.path.join(DATA, "equity_curve.jsonl")

INTERVAL = int(os.environ.get("EQUITY_SNAPSHOT_INTERVAL", "3600"))  # 1h default
ARENA_TOP_N = 50          # snapshot active arena strategies + this many top-by-equity
ALWAYS_KEEP = {"S306"}    # always snapshot the deployed strategy even if not in top-N

# Individual paper bots: file -> id. Their native metric is realized_pnl (BET_USD is
# tiny, no fixed bankroll), so equity is recorded as realized_pnl and DD/Calmar run
# on the cumulative-P&L curve.
BOT_FILES = {
    "always_no_v5_all_cats.json": "always_no_v5",
    "always_no_v2_with_fees.json": "always_no_v2_fees",
    "council.json": "council",
    "whale_follower.json": "whale_follower",
    "whale_fade_grid.json": "whale_fade",
    "theta_decay.json": "theta_decay",
    "news_trader.json": "news_trader",
    "political_skeptic.json": "political_skeptic",
    "subpenny.json": "subpenny",
    "tail_drift.json": "tail_drift",
    "regime_router.json": "regime_router",
    "oil_iran_portfolio.json": "oil_iran",
}


def _load(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _open_count(d):
    for k in ("open_positions", "positions"):
        v = d.get(k)
        if isinstance(v, (dict, list)):
            return len(v)
    return 0


def collect_rows(now, iso):
    """Read every state file and return a list of snapshot rows for this cycle."""
    rows = []

    # --- arena (multi_strategy) ------------------------------------------------
    arena = _load(os.path.join(DATA, "arena_results.json"))
    if arena and isinstance(arena.get("results"), list):
        results = arena["results"]
        active = [r for r in results if not r.get("retired")]
        top = sorted(results, key=lambda r: -(r.get("equity") or r.get("balance") or 0))[:ARENA_TOP_N]
        seen = set()
        chosen = list(active) + list(top)
        for r in chosen:
            sid = (r.get("name") or "").split("|")[0] or str(r.get("id"))
            if sid in seen:
                continue
            seen.add(sid)
            rows.append({
                "ts": now, "iso": iso, "src": "arena", "id": sid,
                "equity": round(float(r.get("equity") or r.get("balance") or 0), 4),
                "realized": round(float(r.get("realized") or 0), 4),
                "wins": r.get("wins", 0), "losses": r.get("losses", 0),
                "open": _open_count(r),
            })
        # always include ALWAYS_KEEP ids even if retired and out of top-N
        for r in results:
            sid = (r.get("name") or "").split("|")[0] or str(r.get("id"))
            if sid in ALWAYS_KEEP and sid not in seen:
                seen.add(sid)
                rows.append({
                    "ts": now, "iso": iso, "src": "arena", "id": sid,
                    "equity": round(float(r.get("equity") or r.get("balance") or 0), 4),
                    "realized": round(float(r.get("realized") or 0), 4),
                    "wins": r.get("wins", 0), "losses": r.get("losses", 0),
                    "open": _open_count(r),
                })

    # --- live_validator --------------------------------------------------------
    lv = _load(os.path.join(DATA, "live_validator.json"))
    if lv and isinstance(lv.get("variants"), list):
        for v in lv["variants"]:
            rows.append({
                "ts": now, "iso": iso, "src": "lv", "id": v.get("variant", "?"),
                "equity": round(float(v.get("equity", 1000.0)), 4),
                "realized": round(float(v.get("realized_pnl_live") or 0), 4),
                "wins": v.get("wins_live", 0), "losses": v.get("losses_live", 0),
                "open": len(v.get("open_cids") or {}),
            })

    # --- individual paper bots -------------------------------------------------
    for fname, bid in BOT_FILES.items():
        d = _load(os.path.join(DATA, fname))
        if not isinstance(d, dict):
            continue
        rp = d.get("realized_pnl")
        if rp is None:
            continue
        rows.append({
            "ts": now, "iso": iso, "src": "bot", "id": bid,
            "equity": round(float(rp), 4),  # cumulative P&L is the curve for these
            "realized": round(float(rp), 4),
            "wins": d.get("wins", 0), "losses": d.get("losses", 0),
            "open": _open_count(d),
        })

    return rows


def snapshot_once():
    os.makedirs(DATA, exist_ok=True)
    now = int(time.time())
    iso = datetime.now(timezone.utc).isoformat()
    rows = collect_rows(now, iso)
    with open(CURVE, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    by_src = defaultdict(int)
    for r in rows:
        by_src[r["src"]] += 1
    print(f"[equity] {iso} wrote {len(rows)} rows "
          f"(arena={by_src['arena']} lv={by_src['lv']} bot={by_src['bot']})")
    return len(rows)


# ---------------------------------------------------------------------------
# Report mode: compute max-DD / Calmar / Sharpe from the accumulated curve.
# ---------------------------------------------------------------------------
def _max_drawdown(series):
    """series: list of equity values in time order. Returns (max_dd_pct, cur_dd_pct).
    Drawdown is measured against a base = first value if all positive, else shifted
    so the curve is positive (handles cumulative-P&L curves that cross zero)."""
    if len(series) < 2:
        return 0.0, 0.0
    base = 0.0
    mn = min(series)
    if mn <= 0:
        base = -mn + 1.0  # shift so every point is >= 1.0; DD% then well-defined
    vals = [v + base for v in series]
    peak = vals[0]
    max_dd = 0.0
    for v in vals:
        if v > peak:
            peak = v
        dd = (peak - v) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    cur_peak = max(vals)
    cur_dd = (cur_peak - vals[-1]) / cur_peak if cur_peak > 0 else 0.0
    return max_dd * 100, cur_dd * 100


def _metrics(rows):
    rows = sorted(rows, key=lambda r: r["ts"])
    eq = [r["equity"] for r in rows]
    ts = [r["ts"] for r in rows]
    span_days = (ts[-1] - ts[0]) / 86400 if len(ts) > 1 else 0.0
    max_dd, cur_dd = _max_drawdown(eq)

    # per-cycle simple returns for Sharpe (needs a positive base for ratio returns)
    base = 0.0
    mn = min(eq)
    if mn <= 0:
        base = -mn + 1.0
    vals = [v + base for v in eq]
    rets = [(vals[i] - vals[i - 1]) / vals[i - 1] for i in range(1, len(vals)) if vals[i - 1] > 0]

    sharpe = None
    if len(rets) >= 30:
        mean = sum(rets) / len(rets)
        var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
        sd = math.sqrt(var)
        if sd > 0 and span_days > 0:
            cycles_per_year = len(rets) / (span_days / 365.0)
            sharpe = mean / sd * math.sqrt(cycles_per_year)

    calmar = None
    raw_return_pct = None
    if vals[0] > 0:
        raw_return_pct = (vals[-1] - vals[0]) / vals[0] * 100
    if span_days >= 30 and max_dd > 0 and vals[0] > 0:
        total_ret = (vals[-1] - vals[0]) / vals[0]
        ann_ret = (1 + total_ret) ** (365.0 / span_days) - 1
        calmar = ann_ret * 100 / max_dd

    return {
        "n_points": len(eq), "span_days": round(span_days, 2),
        "equity_first": eq[0], "equity_last": eq[-1],
        "raw_return_pct": None if raw_return_pct is None else round(raw_return_pct, 2),
        "max_dd_pct": round(max_dd, 2), "cur_dd_pct": round(cur_dd, 2),
        "calmar": None if calmar is None else round(calmar, 2),
        "sharpe": None if sharpe is None else round(sharpe, 2),
    }


def report(ids=None):
    if not os.path.exists(CURVE):
        print(f"No curve yet at {CURVE}. Run the snapshotter first "
              f"(it appends every {INTERVAL}s).")
        return
    by_id = defaultdict(list)
    with open(CURVE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            by_id[(r["src"], r["id"])].append(r)

    keep = set(ids) if ids else None
    out = []
    for (src, eid), rows in by_id.items():
        if keep and eid not in keep:
            continue
        if len(rows) < 2:
            continue
        m = _metrics(rows)
        out.append((src, eid, m))
    out.sort(key=lambda x: -(x[2]["equity_last"]))

    if not out:
        print("Not enough points yet (need >=2 snapshots per entity). "
              "Let the daemon run a few cycles.")
        return

    span = max(m["span_days"] for _, _, m in out)
    print(f"=== equity-curve metrics ({len(out)} entities, max span {span:.2f}d) ===")
    if span < 30:
        print(f"NOTE: span < 30d → Calmar = insufficient history (shown as n/a). "
              f"Raw-period return shown instead.\n")
    hdr = f'{"src":<6}{"id":<34}{"eq_last":>10}{"raw%":>8}{"maxDD%":>8}{"curDD%":>8}{"Calmar":>8}{"Sharpe":>8}{"pts":>5}'
    print(hdr)
    print("-" * len(hdr))
    for src, eid, m in out:
        cal = "n/a" if m["calmar"] is None else f'{m["calmar"]:.2f}'
        shp = "n/a" if m["sharpe"] is None else f'{m["sharpe"]:.2f}'
        raw = "n/a" if m["raw_return_pct"] is None else f'{m["raw_return_pct"]:.1f}'
        print(f'{src:<6}{eid[:33]:<34}{m["equity_last"]:>10.2f}{raw:>8}'
              f'{m["max_dd_pct"]:>8.2f}{m["cur_dd_pct"]:>8.2f}{cal:>8}{shp:>8}{m["n_points"]:>5}')


def main():
    args = sys.argv[1:]
    if args and args[0] == "--report":
        report(args[1:] or None)
        return
    if args and args[0] == "--once":
        snapshot_once()
        return
    print(f"[equity] snapshotter starting, interval={INTERVAL}s, curve={CURVE}")
    snapshot_once()  # immediate first row
    while True:
        try:
            time.sleep(INTERVAL)
            snapshot_once()
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[equity] loop err: {e}")


if __name__ == "__main__":
    main()
