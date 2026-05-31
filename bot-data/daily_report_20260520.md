# Polymarket Arena Daily Report — 2026-05-20

_Generated: 2026-05-20 02:55 UTC_

## 1. System health

- **stalled**: True, alerts: ['theta_decay.json not updated in 3648s', 'maker.json not updated in 3656s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=22s, size=1KB
- orderbook_collector.log: age=18s, size=0KB
- oil_iran.log: age=13s, size=2KB
- always_no.log: age=13s, size=5KB
- whale_fade.log: age=18s, size=1KB
- arena_ticks.jsonl: age=20s, size=110980KB
- orderbook_snapshots.jsonl: age=18s, size=383789KB
- live_validator.json: age=22s, size=129KB
- arena_results.json: age=20s, size=885KB
- political_skeptic.json: age=28s, size=5KB
- council.json: age=9s, size=35KB
- theta_decay.json: age=3648s, size=3KB
- whale_follower.json: age=28s, size=20KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=3656s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 206 alive, 0 retired
- **Positions:** 42 open, 36 closed
- **Realized PnL:** $-0.2640 (actual $0.01 bets)
- **Aggregate equity:** $204680 / $206000 starting = **-0.64%**
- **Exit reasons:** {'htr': 24, 'sl': 12}
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
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1228 | 640/254 | 72% |
| `S199|rsi|p7|e30.00|sloff|tp10%|free|b` | $1228 | 654/260 | 72% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1227 | 659/263 | 71% |
| `S202|rsi|p7|e25.00|sloff|tp10%|free|b` | $1225 | 632/252 | 71% |
| `S207|rsi|p7|e20.00|sl-25%|tp10%|free|b` | $1207 | 617/248 | 71% |
| `S203|rsi|p7|e35.00|sl-10%|tp5%|free|b` | $1201 | 729/284 | 72% |
| `S200|rsi|p7|e25.00|sl-10%|tp5%|free|b` | $1193 | 653/264 | 71% |
| `S197|rsi|p7|e30.00|sl-10%|tp5%|free|b` | $1188 | 673/273 | 71% |
| `S1236|forest_|p15|e0.59|sloff|tp10%|free|b` | $1169 | 24/0 | 100% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=7, W/L=5/0 (WR 100%), realized=$+0.0072, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/5 (WR 82%), pnl=$+0.2361
- v2_with_fees: W/L=501/745 (WR 40%), pnl=$-1.1138
- v3_aggressive: W/L=24/7 (WR 77%), pnl=$+0.2205
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/14 (WR 75%), pnl=$+0.3419

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-19T22:00:11.342470+00:00 (4.9h ago)
- **Pairs:** 978,589 from 1147 markets
- **Correlations:**
  - mu_dev_k1: +0.0518
  - mu_dev_k10: +0.0589
  - mu_dev_k2: +0.0586
  - mu_dev_k5: +0.0613
  - obi_l1_k1: +0.0780
  - obi_l1_k10: +0.0945
  - obi_l1_k2: +0.0838
  - obi_l1_k5: +0.0905
  - obi_l3_k1: +0.0518
  - obi_l3_k10: +0.0601
  - obi_l3_k2: +0.0539
  - obi_l3_k5: +0.0575

## 6. News pipeline

- **News signals captured:** 6011

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 9400, Closed: 2200, Realized: $-3.9000
