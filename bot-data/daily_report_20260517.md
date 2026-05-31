# Polymarket Arena Daily Report — 2026-05-17

_Generated: 2026-05-17 00:46 UTC_

## 1. System health

- **stalled**: True, alerts: ['orderbook_snapshots.jsonl not updated in 3648s', 'political_skeptic.json not updated in 3634s', 'theta_decay.json not updated in 3655s', 'whale_follower.json not updated in 3635s', 'maker.json not updated in 3656s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=26s, size=7KB
- orderbook_collector.log: age=21s, size=0KB
- oil_iran.log: age=6s, size=3KB
- always_no.log: age=1s, size=8KB
- whale_fade.log: age=20s, size=10KB
- arena_ticks.jsonl: age=15s, size=110772KB
- orderbook_snapshots.jsonl: age=3648s, size=383028KB
- live_validator.json: age=26s, size=848KB
- arena_results.json: age=15s, size=885KB
- political_skeptic.json: age=3634s, size=5KB
- council.json: age=5s, size=44KB
- theta_decay.json: age=3655s, size=4KB
- whale_follower.json: age=3635s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=3656s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 81 alive, 125 retired
- **Positions:** 1507 open, 17519 closed
- **Realized PnL:** $-6.8187 (actual $0.01 bets)
- **Aggregate equity:** $86606 / $81000 starting = **+6.92%**
- **Exit reasons:** {'tp': 8909, 'sl': 5334, 'htr': 3276}
- **Win3 skips:** 4977
- **Alive families:** {'BO': 11, 'BB': 16, 'RS': 4, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BO_p50_follow|plt30|sany|fany` | 96 | 78% | +35.8% | +19.5% | $2718 |
| `BO_p100_follow|plt30|sany|fany` | 65 | 78% | +28.8% | +28.1% | $1937 |
| `BB_p50_sd25_fade|plt70|swide|fany` | 68 | 75% | +20.6% | +63.6% | $1700 |
| `BO_p100_follow|plt30|swide|fany` | 51 | 80% | +26.2% | +33.6% | $1668 |
| `BO_p100_follow|plt50|swide|fany` | 63 | 79% | +19.1% | +33.3% | $1601 |
| `BB_p50_sd25_fade|plt50|swide|fany` | 64 | 75% | +18.0% | +64.0% | $1574 |
| `BB_p50_sd25_fade|plt30|sany|fany` | 73 | 71% | +11.4% | +45.4% | $1417 |
| `BO_p100_follow|pany|swide|fany` | 67 | 76% | +10.0% | +33.1% | $1336 |
| `BO_p100_follow|plt70|swide|fany` | 67 | 76% | +10.0% | +33.2% | $1336 |
| `BB_p20_sd25_fade|plt30|sany|fany` | 39 | 85% | +7.9% | +35.7% | $1154 |

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
- **Theta Decay:** open=9, W/L=2/0 (WR 100%), realized=$+0.0019, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/5 (WR 82%), pnl=$+0.2361
- v2_with_fees: W/L=387/568 (WR 41%), pnl=$-0.8613
- v3_aggressive: W/L=24/7 (WR 77%), pnl=$+0.2205
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/14 (WR 75%), pnl=$+0.3419

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-16T19:42:04.023585+00:00 (5.1h ago)
- **Pairs:** 977,239 from 1047 markets
- **Correlations:**
  - mu_dev_k1: +0.0518
  - mu_dev_k10: +0.0591
  - mu_dev_k2: +0.0586
  - mu_dev_k5: +0.0613
  - obi_l1_k1: +0.0779
  - obi_l1_k10: +0.0947
  - obi_l1_k2: +0.0837
  - obi_l1_k5: +0.0905
  - obi_l3_k1: +0.0517
  - obi_l3_k10: +0.0601
  - obi_l3_k2: +0.0539
  - obi_l3_k5: +0.0575

## 6. News pipeline

- **News signals captured:** 5793

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 70070, Closed: 524670, Realized: $+129.7965
