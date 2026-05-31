#!/bin/bash
# Quick arena status — single-glance dashboard.
cd "$(dirname "$0")"

# Use gtimeout (macOS coreutils) if available, else fall back to timeout (Linux/none)
if command -v gtimeout &>/dev/null; then
    TIMEOUT="gtimeout 5"
elif command -v timeout &>/dev/null; then
    TIMEOUT="timeout 5"
else
    TIMEOUT=""
fi

echo "═══ Polymarket Arena Status ═══"
echo ""

echo "── Watchdog ──"
$TIMEOUT docker exec polymarket-ws-bot cat /app/data/watchdog.json 2>/dev/null | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except:
    print('  watchdog.json not readable')
    exit()
print(f\"  stalled={d['stalled']}, alerts={len(d.get('alerts',[]))}\")
print(f\"  phase3: {d.get('phase3','?')}\")
"
echo ""

echo "── Live Validator (forward valid of mass_backtest top) ──"
python3 -c "
import json
d = json.load(open('bot-data/live_validator.json'))
n_open = sum(len(v.get('open_cids',{})) for v in d['variants'])
n_closed = sum(v.get('wins_live',0)+v.get('losses_live',0) for v in d['variants'])
pnl = sum(v.get('realized_pnl_live',0) for v in d['variants'])
exits = {}
for v in d['variants']:
    for r, c in v.get('exit_reason_counts', {}).items():
        exits[r] = exits.get(r,0) + c
print(f'  variants: {len(d[\"variants\"])}, open: {n_open}, closed: {n_closed}')
print(f'  realized: \${pnl:+.4f}, exits: {exits}')
profitable = sorted(
    [v for v in d['variants'] if v.get('wins_live',0)+v.get('losses_live',0) >= 5],
    key=lambda v: -v.get('realized_pnl_live', 0))
if profitable:
    print(f'  TOP 5 with >=5 closed bets:')
    for v in profitable[:5]:
        n = v['wins_live'] + v['losses_live']
        wr = v['wins_live']/n*100
        roi = v['realized_pnl_live']/(n*0.01)*100
        print(f'    {v[\"variant\"][:55]:<55} BT={v[\"backtest_roi\"]:+5.1f}% LIVE={roi:+5.1f}% n={n} WR={wr:.0f}%')
"
echo ""

echo "── Arena (multi_strategy) ──"
python3 -c "
import json
d = json.load(open('bot-data/arena_results.json'))
results = d['results']
profitable = sum(1 for r in results if r.get('equity', 0) > 1000)
top5 = sorted(results, key=lambda r: -r.get('equity', 0))[:5]
print(f'  strategies: {len(results)}, active: {d.get(\"active\", 0)}, profitable: {profitable}')
print(f'  TOP 5:')
for r in top5:
    eq = r.get('equity', 0)
    w, l = r.get('wins', 0), r.get('losses', 0)
    wr = w/(w+l)*100 if (w+l) else 0
    print(f'    {r[\"name\"][:55]:<55} eq=\${eq:>6.0f} W/L={w}/{l} WR={wr:.0f}%')
"
echo ""

echo "── Orderbook depth collector ──"
LINES=$($TIMEOUT docker exec polymarket-ws-bot wc -l /app/data/orderbook_snapshots.jsonl 2>/dev/null | awk '{print $1}')
echo "  snapshots: $LINES"
python3 -c "
import os, json
from datetime import datetime, timezone
path = 'bot-data/orderbook_snapshots.jsonl'
if os.path.exists(path):
    with open(path) as f:
        first = f.readline()
    try:
        ts = datetime.fromisoformat(json.loads(first)['ts'].replace('Z','+00:00')).timestamp()
        import time
        age_h = (time.time() - ts) / 3600
        print(f'  span: {age_h:.1f}h (Phase 3 auto-runs at 24h)')
    except: pass
"
echo ""

echo "── Phase 3 OBI results (if exists) ──"
if [ -f bot-data/phase3_obi_results.json ]; then
  python3 -c "
import json
d = json.load(open('bot-data/phase3_obi_results.json'))
print(f'  ran_at: {d[\"ran_at\"]}')
print(f'  pairs: {d[\"n_pairs\"]:,}, markets: {d[\"n_markets\"]}')
print(f'  correlations:')
for k, v in sorted(d['correlations'].items()):
    print(f'    {k}: {v:+.4f}')
"
else
  echo "  not yet generated"
fi
