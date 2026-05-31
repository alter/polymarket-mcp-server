#!/usr/bin/env python3
"""
Watchdog — runs inside docker container, monitors all bot processes by file
mtime. Writes status to bot-data/watchdog.json every 5 minutes.

Also auto-triggers Phase 3 OBI backtest once 24h+ of orderbook snapshots
accumulated and result file doesn't exist or is stale.
"""
import gc
import json
import os
import subprocess
import time
from datetime import datetime, timezone

DATA = "data"
WATCHDOG_FILE = os.path.join(DATA, "watchdog.json")
LOG_FILES = [
    "live_validator.log",
    "orderbook_collector.log",
    "oil_iran.log",
    "always_no.log",
    "whale_fade.log",
]
# DATA-COLLECTION-ONLY mode (2026-05-31): all paper-trading bots are paused, so their
# state files (live_validator/political_skeptic/council/theta_decay/whale_follower/
# whale_fade_grid/arena_results/maker) no longer update — monitoring them would trigger
# a stall→container-restart loop. We watch ONLY the two live data-collection feeds.
# To resume trading: restore the full list below.
DATA_FILES = [
    "arena_ticks.jsonl",
    "orderbook_snapshots.jsonl",
]

THRESHOLD_SEC = 600         # log stale if mtime older than 10 min
DATA_THRESHOLD_SEC = 900    # most data files should grow within 15 min
CHECK_INTERVAL = 300        # 5 min
# Per-file overrides — tighter for fast-writers, looser for slow scanners
PER_FILE_THRESHOLDS = {
    "orderbook_snapshots.jsonl": 300,   # writes every ~5s; 5min hang = dead
    "arena_ticks.jsonl": 1800,          # heal at 30min; container-kill fallback at 60min
    "arena_results.json": 1500,         # multi_strategy saves slow, every ~10-15min
    "live_validator.json": 1200,        # saves every 5min but can lag
    "political_skeptic.json": 1500,     # scans every 10min
    "council.json": 1500,               # scans every 10min
    "theta_decay.json": 1500,           # scans every 10min
    "whale_follower.json": 2400,        # scans every 30min
    "whale_fade_grid.json": 1500,       # scans every 10min
}

# Selective arena heal: restart only multi_strategy.py instead of the whole container.
# Only fires when arena_ticks is stuck but orderbook is still fresh (network OK).
ARENA_HEAL_AFTER_SEC = 1800   # 30 min of arena_ticks silence → heal
ARENA_HEAL_COOLDOWN  = 3600   # don't heal more than once per hour

# Self-heal: if stalled for N consecutive checks, exit non-zero so
# run_all_bots.sh detects child death and tears down container,
# letting docker `restart: unless-stopped` recreate everything.
STALL_TOLERANCE_CHECKS = 2   # 2 × 5min = 10min before we surrender

PHASE3_MIN_DURATION_SEC = 24 * 3600
PHASE3_BACKTEST = "phase3_obi_backtest.py"
PHASE3_RESULT = "phase3_obi_results.json"


def file_status(path):
    if not os.path.exists(path):
        return {"missing": True}
    return {
        "age_sec": int(time.time() - os.path.getmtime(path)),
        "size_kb": int(os.path.getsize(path) / 1024),
    }


def maybe_heal_arena(report, now, last_heal_time):
    """Kill multi_strategy.py so the restart loop in run_all_bots.sh revives it.

    Conditions:
    - arena_ticks.jsonl stale >= ARENA_HEAL_AFTER_SEC
    - orderbook_snapshots.jsonl fresh (< 300s) — confirms network/CLOB is alive
    - cooldown since last heal
    """
    if now - last_heal_time < ARENA_HEAL_COOLDOWN:
        return None, last_heal_time

    ticks_st  = report["files"].get("arena_ticks.jsonl", {})
    ob_st     = report["files"].get("orderbook_snapshots.jsonl", {})

    ticks_age = ticks_st.get("age_sec", 0)
    ob_age    = ob_st.get("age_sec", 9999)

    if ticks_age < ARENA_HEAL_AFTER_SEC:
        return None, last_heal_time
    if ob_age > 300:
        return None, last_heal_time  # network issue, pkill won't help

    try:
        result = subprocess.run(
            ["pkill", "-TERM", "-f", "/app/multi_strategy.py"],
            capture_output=True,
        )
        msg = (f"HEAL: killed multi_strategy.py "
               f"(arena_ticks stale {ticks_age}s, ob fresh {ob_age}s) "
               f"— restart loop will revive in ~10s. pkill rc={result.returncode}")
        print(msg)
        return msg, now
    except Exception as e:
        msg = f"HEAL attempt failed: {e}"
        print(msg)
        return msg, last_heal_time


def maybe_run_phase3():
    """Auto-run OBI backtest if snapshot file >24h old and result missing/stale."""
    snap = os.path.join(DATA, "orderbook_snapshots.jsonl")
    if not os.path.exists(snap):
        return None
    # Read first line ts
    try:
        with open(snap) as f:
            first = f.readline()
        first_ts = datetime.fromisoformat(
            json.loads(first)["ts"].replace("Z", "+00:00")
        ).timestamp()
    except Exception as e:
        return f"first_ts read err: {e}"

    duration = time.time() - first_ts
    if duration < PHASE3_MIN_DURATION_SEC:
        hours = duration / 3600
        return f"awaiting data: {hours:.1f}h / 24h"

    backtest_path = os.path.join("/app", PHASE3_BACKTEST)
    if not os.path.exists(backtest_path):
        return "phase3 backtest script not found"

    result_path = os.path.join(DATA, PHASE3_RESULT)
    # Re-run if result older than snapshot file mtime
    if os.path.exists(result_path):
        if os.path.getmtime(result_path) > os.path.getmtime(snap) - 3600:
            return "phase3 result fresh"

    # Fire backtest in background
    log_path = os.path.join(DATA, "phase3_backtest.log")
    try:
        subprocess.Popen(
            ["python3", "-u", backtest_path],
            stdout=open(log_path, "w"),
            stderr=subprocess.STDOUT,
        )
        return "phase3 backtest LAUNCHED"
    except Exception as e:
        return f"phase3 launch err: {e}"


def rotate_jsonl(filename, keep_days):
    """Trim a JSONL file to keep only lines with ts >= now - keep_days.

    Reads the file, rewrites in-place keeping only recent lines.
    Skips if file is small (<10MB) or was recently rotated (<6h).
    Uses a temp file + atomic rename to avoid data loss.
    """
    path = os.path.join(DATA, filename)
    if not os.path.exists(path):
        return
    size_mb = os.path.getsize(path) / 1024**2
    if size_mb < 10:
        return
    # Check rotation marker
    marker = path + ".rotated_at"
    if os.path.exists(marker):
        if time.time() - os.path.getmtime(marker) < 6 * 3600:
            return
    cutoff = time.time() - keep_days * 86400
    tmp = path + ".tmp"
    kept = dropped = 0
    try:
        with open(path, "rb") as fin, open(tmp, "wb") as fout:
            for line in fin:
                try:
                    ts_str = line[6:32].decode("ascii", errors="ignore")  # fast: slice "ts" field
                    # Parse only if it looks like an ISO timestamp
                    from datetime import datetime, timezone
                    ts = datetime.fromisoformat(
                        line[line.index(b'"ts":"') + 6: line.index(b'"ts":"') + 32]
                        .decode().split('"')[0].replace("Z", "+00:00")
                    ).timestamp()
                    if ts >= cutoff:
                        fout.write(line)
                        kept += 1
                    else:
                        dropped += 1
                except Exception:
                    fout.write(line)  # keep unparseable lines
                    kept += 1
        os.replace(tmp, path)
        open(marker, "w").close()
        print(f"[rotate] {filename}: dropped {dropped} lines ({size_mb:.0f}MB→), kept {kept}")
    except Exception as e:
        print(f"[rotate] {filename} err: {e}")
        if os.path.exists(tmp):
            os.remove(tmp)


def rotate_backups():
    """Backup critical state files with TWO retention policies:
    1. Per-30-min rotating (last 10 versions) — fast recovery
    2. Per-day snapshot (one per UTC day, never rotated) — historical

    Only backups NON-EMPTY state (closed bets > 0 or equity != STARTING_BALANCE)
    to avoid overwriting good backups with broken fresh state.
    """
    backup_dir = os.path.join(DATA, "backups")
    os.makedirs(backup_dir, exist_ok=True)
    targets = ["arena_results.json", "live_validator.json",
               "political_skeptic.json", "theta_decay.json"]
    now_dt = datetime.now(timezone.utc)
    ts_label = now_dt.strftime("%Y%m%d_%H%M")
    day_label = now_dt.strftime("%Y%m%d")
    for fn in targets:
        src = os.path.join(DATA, fn)
        if not os.path.exists(src):
            continue
        size = os.path.getsize(src)
        if size < 1000:
            continue
        try:
            with open(src) as f:
                content = json.load(f)
        except Exception:
            print(f"[backup] {fn} unparsable, skipping")
            continue
        # Skip if state looks fresh (live_validator and political_skeptic check)
        # Avoid rotating GOOD older backups out for FRESH (no-progress) states.
        if fn == "live_validator.json":
            variants = content.get("variants", [])
            n_closed = sum(v.get("wins_live", 0) + v.get("losses_live", 0) for v in variants)
            if n_closed == 0 and len(variants) > 0:
                print(f"[backup] {fn} state appears fresh (0 closed), skipping rotation")
                continue
        elif fn == "political_skeptic.json":
            if (content.get("wins", 0) + content.get("losses", 0) == 0
                    and len(content.get("open_positions", {})) == 0):
                continue
        # Daily snapshot: one per UTC day, never overwrite
        daily_dst = os.path.join(backup_dir, f"{fn}.daily.{day_label}")
        if not os.path.exists(daily_dst):
            try:
                with open(src, "rb") as fr, open(daily_dst, "wb") as fw:
                    fw.write(fr.read())
            except Exception as e:
                print(f"[backup] {fn} daily err: {e}")
        # Rotating snapshot
        rotating_dst = os.path.join(backup_dir, f"{fn}.{ts_label}")
        try:
            with open(src, "rb") as fr, open(rotating_dst, "wb") as fw:
                fw.write(fr.read())
        except Exception as e:
            print(f"[backup] {fn} copy err: {e}")
            continue
        # Keep only last 10 rotating (excluding daily snapshots)
        existing = sorted([f for f in os.listdir(backup_dir)
                           if f.startswith(fn + ".") and ".daily." not in f])
        if len(existing) > 10:
            for old in existing[:-10]:
                try:
                    os.remove(os.path.join(backup_dir, old))
                except Exception:
                    pass


def collect_live_metrics():
    """Brief snapshot of live validator + arena state."""
    out = {}
    lv_path = os.path.join(DATA, "live_validator.json")
    if os.path.exists(lv_path):
        try:
            d = json.load(open(lv_path))
            variants = d.get("variants", [])
            out["live_validator"] = {
                "n_variants": len(variants),
                "n_total_bets": sum(v.get("total_bets_live", 0) for v in variants),
                "n_open": sum(len(v.get("open_cids", {})) for v in variants),
                "n_closed": sum(
                    v.get("wins_live", 0) + v.get("losses_live", 0) for v in variants
                ),
                "realized_pnl": round(
                    sum(v.get("realized_pnl_live", 0) for v in variants), 4
                ),
            }
        except Exception as e:
            out["live_validator"] = {"err": str(e)}
    ar_path = os.path.join(DATA, "arena_results.json")
    if os.path.exists(ar_path):
        try:
            d = json.load(open(ar_path))
            results = d.get("results", [])
            profitable = sum(1 for r in results if r.get("equity", 0) > 1000)
            top1 = max(results, key=lambda r: r.get("equity", 0)) if results else {}
            out["arena"] = {
                "n_strategies": len(results),
                "n_profitable": profitable,
                "n_active": d.get("active", 0),
                "top_equity": round(top1.get("equity", 0), 0),
                "top_name": top1.get("name", "")[:60],
            }
        except Exception as e:
            out["arena"] = {"err": str(e)}
    return out


def maybe_run_daily_report():
    """Auto-generate daily report once per day at ~00:30 UTC."""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    report_path = os.path.join(DATA, f"daily_report_{today}.md")
    if os.path.exists(report_path):
        return None  # already generated today
    script = "/app/daily_report.py"
    if not os.path.exists(script):
        return None
    try:
        log_path = os.path.join(DATA, "daily_report.log")
        subprocess.Popen(
            ["python3", "-u", script],
            stdout=open(log_path, "a"),
            stderr=subprocess.STDOUT,
            cwd="/app",
        )
        return f"daily report LAUNCHED for {today}"
    except Exception as e:
        return f"daily report err: {e}"


def main():
    print("Watchdog started")
    last_backup = 0
    last_daily_report = 0
    last_arena_heal = 0
    last_rotate = 0
    consecutive_stalls = 0
    while True:
        now = time.time()
        # Backup every 30 min
        if now - last_backup > 1800:
            rotate_backups()
            last_backup = now
        # Rotate large JSONL files every 6h to cap disk usage
        if now - last_rotate > 6 * 3600:
            rotate_jsonl("orderbook_snapshots.jsonl", keep_days=7)
            rotate_jsonl("arena_ticks.jsonl", keep_days=30)
            last_rotate = now
        # Try generate daily report once per day (at first cycle after midnight UTC)
        if now - last_daily_report > 3600:  # check hourly
            r = maybe_run_daily_report()
            if r:
                print(f"[daily] {r}")
            last_daily_report = now
        report = {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "stalled": False,
            "alerts": [],
            "files": {},
        }
        for fn in LOG_FILES + DATA_FILES:
            path = os.path.join(DATA, fn)
            st = file_status(path)
            report["files"][fn] = st
            # Critical files MUST be growing
            if fn in DATA_FILES and not st.get("missing"):
                threshold = PER_FILE_THRESHOLDS.get(fn, DATA_THRESHOLD_SEC)
                if st["age_sec"] > threshold:
                    report["stalled"] = True
                    report["alerts"].append(
                        f"{fn} not updated in {st['age_sec']}s")

        report["live"] = collect_live_metrics()
        report["phase3"] = maybe_run_phase3()

        heal_msg, last_arena_heal = maybe_heal_arena(report, now, last_arena_heal)
        if heal_msg:
            report["arena_heal"] = heal_msg

        try:
            with open(WATCHDOG_FILE, "w") as f:
                json.dump(report, f, indent=1)
        except Exception as e:
            print(f"watchdog write err: {e}")

        if report["stalled"]:
            consecutive_stalls += 1
            print(f"STALLED [{consecutive_stalls}/{STALL_TOLERANCE_CHECKS}]: {report['alerts']}")
            if consecutive_stalls >= STALL_TOLERANCE_CHECKS:
                print(f"FATAL: stalled {consecutive_stalls} checks in a row, "
                      f"exiting to trigger container restart via docker policy")
                # Try to flush state one more time
                try:
                    with open(WATCHDOG_FILE, "w") as f:
                        report["surrender"] = True
                        json.dump(report, f, indent=1)
                except Exception:
                    pass
                # Exit non-zero — run_all_bots.sh `wait -n` will detect, kill rest,
                # docker `restart: unless-stopped` brings everything back fresh
                os._exit(2)
        else:
            consecutive_stalls = 0
            lv = report["live"].get("live_validator", {})
            print(
                f"[{datetime.now():%H:%M}] OK | "
                f"validator: {lv.get('n_total_bets', 0)} bets, "
                f"{lv.get('n_closed', 0)} closed | "
                f"phase3: {report['phase3']}"
            )

        gc.collect()
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
