#!/bin/bash
# Entry point — DATA-COLLECTION-ONLY mode (trading paused 2026-05-31 per user request).
#
# Running:
#   - orderbook_collector  → orderbook_snapshots.jsonl
#   - news_monitor         → news feed
#   - multi_strategy       → arena_ticks.jsonl ONLY (ARENA_COLLECT_ONLY=true: no paper trading)
#   - watchdog             → liveness (monitors ONLY the two data files, see watchdog.py)
#
# PAUSED (paper trading — re-enable by uncommenting their blocks AND restoring the full
# watchdog DATA_FILES list + dropping ARENA_COLLECT_ONLY):
#   oil_iran_trader, always_no_bot, whale_fade_bot, live_validator, theta_decay_bot,
#   political_skeptic_bot, news_trader_bot, whale_follower_bot, subpenny_lottery_bot,
#   tail_drift_bot, council_bot, regime_router, equity_snapshotter, maker_bot(retired).
#
# If any running process dies, container dies and docker `restart: unless-stopped`
# recreates everything.

set -e

echo "$(date) [entrypoint] Starting orderbook_collector..."
python -u /app/orderbook_collector.py > /app/data/orderbook_collector.log 2>&1 &
OB_PID=$!

echo "$(date) [entrypoint] Starting news_monitor..."
python -u /app/news_monitor.py > /app/data/news_monitor.log 2>&1 &
NM_PID=$!

echo "$(date) [entrypoint] Starting watchdog..."
python -u /app/watchdog.py > /app/data/watchdog.log 2>&1 &
WD_PID=$!

echo "$(date) [entrypoint] Starting multi_strategy (arena) in COLLECT_ONLY mode — ticks only, NO trading..."
(
  while true; do
    ARENA_COLLECT_ONLY=true python -u /app/multi_strategy.py
    echo "$(date) [arena-loop] multi_strategy exited (code=$?), restarting in 10s..."
    sleep 10
  done
) &
ARENA_PID=$!

# Wait for any to exit, then kill all
wait -n $OB_PID $NM_PID $WD_PID $ARENA_PID
EXIT_CODE=$?
echo "$(date) [entrypoint] One process exited with $EXIT_CODE. Killing rest."
kill -TERM $OB_PID $NM_PID $WD_PID $ARENA_PID 2>/dev/null || true
exit $EXIT_CODE
