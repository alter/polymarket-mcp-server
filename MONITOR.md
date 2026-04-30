# MONITOR.md — запуск системы и проверка результатов

Paper-trading контейнер на Polymarket: 15 параллельных Python-процессов в одном docker-контейнере, общий лимит 1GB RAM / 2 CPU. Стартовый виртуальный капитал — $1000 на стратегию.

## 1. Запуск

### Первичный запуск (build + up)

```bash
cd /Users/rdolgov/workflow/git/polymarket-mcp-server
docker compose -f docker-compose.bot.yml build
docker compose -f docker-compose.bot.yml up -d
```

Контейнер: `polymarket-ws-bot`. Auto-restart политика `unless-stopped` — крашнется → docker поднимет.

### Что внутри (`run_all_bots.sh`)

15 процессов, состояние пишется в `bot-data/<bot>.json`, логи в `bot-data/<bot>.log`:

| Процесс | Что делает | State-файл |
|---|---|---|
| `multi_strategy.py` | арена 1023 стратегий (wavelet/RSI/BB/BO/ME/ZS × фильтры × exit policy) | `arena_results.json` |
| `live_validator.py` | 200 forward-validation вариантов из mass_backtest top | `live_validator.json` |
| `oil_iran_trader.py` | causal trader (нефть ↔ Иран события) | — |
| `always_no_bot.py` | 5 v-вариантов "всегда NO" базовой ставки | `always_no_v{1..5}_*.json` |
| `whale_fade_bot.py` | fade крупных тейкеров | `whale_fade.json` |
| `whale_follower_bot.py` | копирует топ-20 кошельков лидерборда | `whale_follower.json` |
| `political_skeptic_bot.py` | 23 паттерна (base_rate, impossible, stat_pattern) | `political_skeptic.json` |
| `theta_decay_bot.py` | покупка YES @ 0.92 на market mid >= 0.80 | `theta_decay.json` |
| `news_monitor.py` + `news_trader_bot.py` | RSS → keyword direction inference | `news_trader.json` |
| `subpenny_lottery_bot.py` | mid ∈ [0.01, 0.05], 1-36ч до закрытия | `subpenny.json` |
| `tail_drift_bot.py` | mid в крайностях, 0.5-6ч до закрытия | `tail_drift.json` |
| `orderbook_collector.py` | снапшоты bid/ask depth | `orderbook_snapshots.jsonl` |
| `council_bot.py` | мета-агрегатор: consensus K+ семей по сторонам | `council.json` |
| `watchdog.py` | контроль freshness всех state-файлов, авто-триггер OBI/daily report | `watchdog.json` |

### Деплой изменений в код (без rebuild)

Volumes из `docker-compose.bot.yml` маунтят `.py`-файлы внутрь контейнера. Поправил локально → restart процесса:

```bash
# поправил council_bot.py — рестарт всего контейнера (быстрее)
docker restart polymarket-ws-bot

# или только одного процесса (если нужно сохранить state остальных)
docker exec polymarket-ws-bot pkill -f council_bot.py
# run_all_bots.sh ловит exit → kill всех → restart container
```

После добавления **новых** Python-зависимостей — нужен rebuild:
```bash
docker compose -f docker-compose.bot.yml build && docker compose -f docker-compose.bot.yml up -d
```

### Стоп

```bash
docker compose -f docker-compose.bot.yml down
```

## 2. Проверка результатов

### Быстрый dashboard (одной командой)

```bash
./status.sh
```

Выведет: watchdog state, LV top-5 по live ROI, arena top-5 по equity, OBI корреляции, age orderbook snapshots.

### Watchdog — жив ли контейнер

```bash
cat bot-data/watchdog.json | python3 -c "import json,sys; d=json.load(sys.stdin); print('stalled:', d['stalled']); [print(f'  {k}={v[\"age_sec\"]}s') for k,v in d['files'].items()]"
```

Что смотреть:
- `stalled: false` — всё ок
- любой `age_sec > 1200` — процесс мёртв; делать `docker restart polymarket-ws-bot`

### Live Validator (200 forward-validation вариантов)

```bash
python3 -c "
import json
d = json.load(open('bot-data/live_validator.json'))
live = [v for v in d['variants'] if not v.get('retired')]
n_open = sum(len(v.get('open_cids',{})) for v in live)
n_closed = sum(v.get('wins_live',0)+v.get('losses_live',0) for v in live)
pnl = sum(v.get('realized_pnl_live',0) for v in live)
print(f'live={len(live)} open={n_open} closed={n_closed} pnl=\${pnl:+.4f}')
top = sorted([v for v in live if v.get('wins_live',0)+v.get('losses_live',0)>=5],
             key=lambda v: -v.get('realized_pnl_live', 0))[:5]
for v in top:
    n = v['wins_live']+v['losses_live']
    print(f'  {v[\"variant\"][:50]:<50} BT={v[\"backtest_roi\"]:+5.1f}% LIVE={v[\"realized_pnl_live\"]/(n*0.01)*100:+5.1f}% n={n}')
"
```

Поля:
- `backtest_roi` — ROI на исторических ticks (in-sample)
- `realized_pnl_live` — кумулятивный PnL после деплоя ($-scale: $50/trade арена-мерка)
- `wins_live` / `losses_live` / `n_total_bets` — счётчики
- `equity_arena_scale` — $1000 + realized × 5000 (arena-уровень)
- `retired: true` → equity упал ниже $800
- `promoted: true` → equity > $1100, бет $0.05 вместо $0.02

### Arena (multi_strategy)

```bash
python3 -c "
import json
d = json.load(open('bot-data/arena_results.json'))
res = d['results']
res.sort(key=lambda r:-r.get('equity',0))
prof = sum(1 for r in res if r['equity']>1000)
print(f'total={len(res)} profitable={prof}')
for r in res[:5]:
    wr = r['wins']/(r['wins']+r['losses'])*100 if r['wins']+r['losses'] else 0
    print(f'  {r[\"name\"][:55]:<55} eq=\${r[\"equity\"]:.0f} trades={r[\"trades\"]} WR={wr:.0f}%')
"
```

### OBI Phase 3 (orderbook imbalance signal)

Авто-генерируется watchdog'ом каждые 24h из `orderbook_snapshots.jsonl`:

```bash
python3 -c "
import json, os, time
d = json.load(open('bot-data/phase3_obi_results.json'))
print(f'age={(time.time()-os.path.getmtime(\"bot-data/phase3_obi_results.json\"))/3600:.1f}h pairs={d[\"n_pairs\"]:,}')
elig = sorted([c for c in d['candidates'] if c['n']>=50], key=lambda c:-abs(c['mean_fwd_pct']))[:5]
for c in elig:
    print(f'  mean_fwd={c[\"mean_fwd_pct\"]:+.3f}% n={c[\"n\"]}')
"
```

### Council bot (мета-агрегатор)

```bash
cat bot-data/council.json | python3 -c "
import json,sys
d = json.load(sys.stdin)
print(f'open={len(d[\"open_positions\"])} bets={d[\"total_bets\"]} W/L={d[\"wins\"]}/{d[\"losses\"]} pnl=\${d[\"realized_pnl\"]:+.4f}')
print(f'conflicts_observed={d[\"conflicts_observed\"]}')
"
```

### Логи отдельного бота

```bash
tail -f bot-data/live_validator.log
tail -f bot-data/council.log
tail -f bot-data/watchdog.log
docker logs -f polymarket-ws-bot          # консолидированный stdout (multi_strategy)
docker logs --tail=100 polymarket-ws-bot
```

## 3. Diagnostic чеклист (когда что-то сломалось)

1. **Контейнер жив?** `docker ps | grep polymarket-ws-bot` — должен быть `Up`
2. **OOM kill?** `docker logs polymarket-ws-bot 2>&1 | tail -20` ищем "Killed" / exit 137
3. **Watchdog stalled?** `cat bot-data/watchdog.json | jq .stalled`
4. **State файлы устарели?** Любой `age_sec > 1200` в watchdog → процесс упал
5. **Resource usage?** `docker stats polymarket-ws-bot --no-stream`

Стандартный фикс: `docker restart polymarket-ws-bot`. State сохраняется в `bot-data/` (volume mount), 50 ротирующихся бэкапов в `bot-data/backups/`.

## 4. Backtest / offline analysis (вне контейнера)

Запускаются на хосте, читают `bot-data/arena_ticks.jsonl` и `arena_results.json`:

```bash
# venv первый раз
python3 -m venv venv && source venv/bin/activate && pip install -e ".[dev]"

# mass backtest всех вариантов (9072 конфига × 273 рынка, ~5 мин на 4 ядра)
python3 mass_backtest.py

# walk-forward 70/30 + bootstrap p-value
python3 walkforward_validation.py

# K-of-N ensemble (MetaVote)
python3 mass_backtest_metavote.py

# дневной отчёт (генерится auto через watchdog раз в сутки)
python3 daily_report.py
```

Отчёт: `bot-data/walkforward_results.json`, `bot-data/mass_backtest_metavote.json`, `bot-data/daily_report_YYYYMMDD.md`.

## 5. Reporting rules

При показе любого PnL/ROI указывать (см. CLAUDE.md):
- Time window (start → now, дни/часы)
- Starting capital ($1000/strategy)
- Position size ($0.01-0.05 actual = $50-250 arena-scale)
- n_total_bets (не только W/L)
- Max drawdown (peak-to-trough)
- Calmar ratio только при duration ≥ 30 дней

`+30%` без контекста — бесполезно. `+30% за 5 дней, n=500, max DD −8%` — полезно.
