# Polymarket Arena Daily Report — 2026-05-16

_Generated: 2026-05-16 03:08 UTC_

## 1. System health

- **stalled**: True, alerts: ['arena_ticks.jsonl not updated in 1871s', 'orderbook_snapshots.jsonl not updated in 1876s', 'political_skeptic.json not updated in 2368s', 'theta_decay.json not updated in 1869s', 'maker.json not updated in 1882s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=13s, size=6KB
- orderbook_collector.log: age=23s, size=0KB
- oil_iran.log: age=19s, size=2KB
- always_no.log: age=1882s, size=3KB
- whale_fade.log: age=14s, size=10KB
- arena_ticks.jsonl: age=1871s, size=110726KB
- orderbook_snapshots.jsonl: age=1876s, size=382874KB
- live_validator.json: age=13s, size=947KB
- arena_results.json: age=17s, size=886KB
- political_skeptic.json: age=2368s, size=5KB
- council.json: age=21s, size=49KB
- theta_decay.json: age=1869s, size=5KB
- whale_follower.json: age=2370s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=1882s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 82 alive, 124 retired
- **Positions:** 1768 open, 17258 closed
- **Realized PnL:** $-6.7871 (actual $0.01 bets)
- **Aggregate equity:** $86926 / $82000 starting = **+6.01%**
- **Exit reasons:** {'tp': 8887, 'sl': 5255, 'htr': 3116}
- **Win3 skips:** 4977
- **Alive families:** {'BO': 11, 'BB': 16, 'RS': 5, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BO_p50_follow|plt30|sany|fany` | 91 | 77% | +35.9% | +19.5% | $2634 |
| `BO_p100_follow|plt30|sany|fany` | 60 | 78% | +31.1% | +28.1% | $1932 |
| `BO_p100_follow|plt30|swide|fany` | 47 | 79% | +27.1% | +33.6% | $1638 |
| `BB_p50_sd25_fade|plt70|swide|fany` | 64 | 73% | +18.0% | +63.6% | $1578 |
| `BB_p50_sd25_fade|plt30|sany|fany` | 68 | 71% | +15.7% | +45.4% | $1534 |
| `BO_p100_follow|plt50|swide|fany` | 59 | 78% | +18.0% | +33.3% | $1530 |
| `BB_p50_sd25_fade|plt50|swide|fany` | 60 | 73% | +15.1% | +64.0% | $1452 |
| `BO_p100_follow|pany|swide|fany` | 63 | 75% | +8.4% | +33.1% | $1266 |
| `BO_p100_follow|plt70|swide|fany` | 63 | 75% | +8.4% | +33.2% | $1266 |
| `BB_p20_sd25_fade|plt30|sany|fany` | 34 | 85% | +10.4% | +35.7% | $1176 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 400 active, 723 profitable
- **Family distribution:** {'rsi': 39, 'forest_': 960, 'mean_re': 174, 'bolling': 37, 'zscore': 34, 'ensembl': 70, 'breakou': 19, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $1229 | 482/121 | 80% |
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1226 | 638/254 | 72% |
| `S199|rsi|p7|e30.00|sloff|tp10%|free|b` | $1226 | 652/260 | 71% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1225 | 657/263 | 71% |
| `S202|rsi|p7|e25.00|sloff|tp10%|free|b` | $1223 | 630/252 | 71% |
| `S207|rsi|p7|e20.00|sl-25%|tp10%|free|b` | $1205 | 615/248 | 71% |
| `S203|rsi|p7|e35.00|sl-10%|tp5%|free|b` | $1199 | 727/284 | 72% |
| `S200|rsi|p7|e25.00|sl-10%|tp5%|free|b` | $1188 | 650/264 | 71% |
| `S197|rsi|p7|e30.00|sl-10%|tp5%|free|b` | $1184 | 670/273 | 71% |
| `S1236|forest_|p15|e0.59|sloff|tp10%|free|b` | $1169 | 24/0 | 100% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/5 (WR 82%), pnl=$+0.2361
- v2_with_fees: W/L=352/506 (WR 41%), pnl=$-0.6487
- v3_aggressive: W/L=23/7 (WR 77%), pnl=$+0.2152
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/14 (WR 75%), pnl=$+0.3419

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-15T23:58:30.047789+00:00 (3.2h ago)
- **Pairs:** 976,942 from 1021 markets
- **Correlations:**
  - mu_dev_k1: +0.0518
  - mu_dev_k10: +0.0590
  - mu_dev_k2: +0.0586
  - mu_dev_k5: +0.0613
  - obi_l1_k1: +0.0779
  - obi_l1_k10: +0.0947
  - obi_l1_k2: +0.0837
  - obi_l1_k5: +0.0905
  - obi_l3_k1: +0.0517
  - obi_l3_k10: +0.0602
  - obi_l3_k2: +0.0539
  - obi_l3_k5: +0.0575

## 6. News pipeline

- **News signals captured:** 5744

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 87650, Closed: 507090, Realized: $+179.2115
