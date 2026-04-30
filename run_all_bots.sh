#!/bin/bash
# Entry point launching arena, oil-iran trader, and always-no bot.
# If any dies, container dies and docker restarts everything.

set -e
echo "$(date) [entrypoint] Starting oil_iran_trader..."
python -u /app/oil_iran_trader.py > /app/data/oil_iran.log 2>&1 &
OIL_PID=$!

echo "$(date) [entrypoint] Starting always_no_bot..."
python -u /app/always_no_bot.py > /app/data/always_no.log 2>&1 &
NO_PID=$!

echo "$(date) [entrypoint] Starting whale_fade_bot..."
python -u /app/whale_fade_bot.py > /app/data/whale_fade.log 2>&1 &
WHALE_PID=$!

echo "$(date) [entrypoint] Starting live_validator..."
python -u /app/live_validator.py > /app/data/live_validator.log 2>&1 &
LV_PID=$!

echo "$(date) [entrypoint] Starting orderbook_collector..."
python -u /app/orderbook_collector.py > /app/data/orderbook_collector.log 2>&1 &
OB_PID=$!

echo "$(date) [entrypoint] Starting watchdog..."
python -u /app/watchdog.py > /app/data/watchdog.log 2>&1 &
WD_PID=$!

echo "$(date) [entrypoint] Starting theta_decay_bot..."
python -u /app/theta_decay_bot.py > /app/data/theta_decay.log 2>&1 &
TD_PID=$!

echo "$(date) [entrypoint] Starting political_skeptic_bot..."
python -u /app/political_skeptic_bot.py > /app/data/political_skeptic.log 2>&1 &
SKP_PID=$!

echo "$(date) [entrypoint] Starting news_monitor..."
python -u /app/news_monitor.py > /app/data/news_monitor.log 2>&1 &
NM_PID=$!

echo "$(date) [entrypoint] Starting news_trader_bot..."
python -u /app/news_trader_bot.py > /app/data/news_trader.log 2>&1 &
NT_PID=$!

echo "$(date) [entrypoint] Starting whale_follower_bot..."
python -u /app/whale_follower_bot.py > /app/data/whale_follower.log 2>&1 &
WF_PID=$!

echo "$(date) [entrypoint] Starting subpenny_lottery_bot..."
python -u /app/subpenny_lottery_bot.py > /app/data/subpenny.log 2>&1 &
SP_PID=$!

echo "$(date) [entrypoint] Starting tail_drift_bot..."
python -u /app/tail_drift_bot.py > /app/data/tail_drift.log 2>&1 &
TD2_PID=$!

echo "$(date) [entrypoint] Starting council_bot..."
python -u /app/council_bot.py > /app/data/council.log 2>&1 &
CB_PID=$!

echo "$(date) [entrypoint] Starting multi_strategy (arena)..."
python -u /app/multi_strategy.py &
ARENA_PID=$!

# Wait for any to exit, then kill all
wait -n $OIL_PID $NO_PID $WHALE_PID $LV_PID $OB_PID $WD_PID $TD_PID $SKP_PID $NM_PID $NT_PID $WF_PID $SP_PID $TD2_PID $CB_PID $ARENA_PID
EXIT_CODE=$?
echo "$(date) [entrypoint] One process exited with $EXIT_CODE. Killing rest."
kill -TERM $OIL_PID $NO_PID $WHALE_PID $LV_PID $OB_PID $WD_PID $TD_PID $SKP_PID $NM_PID $NT_PID $WF_PID $SP_PID $TD2_PID $CB_PID $ARENA_PID 2>/dev/null || true
exit $EXIT_CODE
