# Polymarket Arena Daily Report — 2026-05-22

_Generated: 2026-05-22 00:59 UTC_

## 1. System health

- **stalled**: True, alerts: ['arena_ticks.jsonl not updated in 3655s', 'arena_results.json not updated in 3654s', 'theta_decay.json not updated in 3644s', 'maker.json not updated in 3655s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=14s, size=2KB
- orderbook_collector.log: age=11s, size=1KB
- oil_iran.log: age=26s, size=4KB
- always_no.log: age=0s, size=10KB
- whale_fade.log: age=9s, size=2KB
- arena_ticks.jsonl: age=3655s, size=111154KB
- orderbook_snapshots.jsonl: age=11s, size=384494KB
- live_validator.json: age=14s, size=137KB
- arena_results.json: age=3654s, size=885KB
- political_skeptic.json: age=8s, size=5KB
- council.json: age=2s, size=36KB
- theta_decay.json: age=3644s, size=3KB
- whale_follower.json: age=8s, size=20KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=3655s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 206 alive, 0 retired
- **Positions:** 57 open, 78 closed
- **Realized PnL:** $-0.2220 (actual $0.01 bets)
- **Aggregate equity:** $204890 / $206000 starting = **-0.54%**
- **Exit reasons:** {'htr': 24, 'sl': 12, 'tp': 42}
- **Win3 skips:** 0
- **Alive families:** {'BO': 37, 'RS': 81, 'BB': 32, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 4, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 400 active, 723 profitable
- **Family distribution:** {'rsi': 39, 'forest_': 960, 'mean_re': 174, 'bolling': 37, 'zscore': 34, 'ensembl': 70, 'breakou': 19, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $1229 | 482/121 | 80% |
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1229 | 641/254 | 72% |
| `S199|rsi|p7|e30.00|sloff|tp10%|free|b` | $1228 | 655/260 | 72% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1227 | 660/263 | 72% |
| `S202|rsi|p7|e25.00|sloff|tp10%|free|b` | $1225 | 633/252 | 72% |
| `S207|rsi|p7|e20.00|sl-25%|tp10%|free|b` | $1207 | 618/248 | 71% |
| `S203|rsi|p7|e35.00|sl-10%|tp5%|free|b` | $1202 | 730/284 | 72% |
| `S200|rsi|p7|e25.00|sl-10%|tp5%|free|b` | $1193 | 654/264 | 71% |
| `S197|rsi|p7|e30.00|sl-10%|tp5%|free|b` | $1189 | 674/273 | 71% |
| `S1236|forest_|p15|e0.59|sloff|tp10%|free|b` | $1169 | 24/0 | 100% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=8, W/L=5/0 (WR 100%), realized=$+0.0072, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/5 (WR 82%), pnl=$+0.2361
- v2_with_fees: W/L=577/867 (WR 40%), pnl=$-1.2424
- v3_aggressive: W/L=24/7 (WR 77%), pnl=$+0.2205
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/14 (WR 75%), pnl=$+0.3419

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-21T20:14:49.432709+00:00 (3.7h ago)
- **Pairs:** 980,000 from 1220 markets
- **Correlations:**
  - mu_dev_k1: +0.0518
  - mu_dev_k10: +0.0589
  - mu_dev_k2: +0.0586
  - mu_dev_k5: +0.0613
  - obi_l1_k1: +0.0781
  - obi_l1_k10: +0.0944
  - obi_l1_k2: +0.0839
  - obi_l1_k5: +0.0905
  - obi_l3_k1: +0.0518
  - obi_l3_k10: +0.0601
  - obi_l3_k2: +0.0539
  - obi_l3_k5: +0.0574

## 6. News pipeline

- **News signals captured:** 6210

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 35280, Closed: 4160, Realized: $-12.5240
