#!/bin/bash
# Entry point that launches both arena AND oil-iran trader.
# If either dies, the container dies and docker restarts the whole thing.

set -e
echo "$(date) [entrypoint] Starting oil_iran_trader in background..."
python /app/oil_iran_trader.py > /app/data/oil_iran.log 2>&1 &
OIL_PID=$!

echo "$(date) [entrypoint] Starting multi_strategy (arena) in foreground..."
python /app/multi_strategy.py &
ARENA_PID=$!

# Wait for either process to exit. If one dies, kill the other and exit.
wait -n $OIL_PID $ARENA_PID
EXIT_CODE=$?
echo "$(date) [entrypoint] One process exited with code $EXIT_CODE. Killing the other and exiting."
kill -TERM $OIL_PID $ARENA_PID 2>/dev/null || true
exit $EXIT_CODE
