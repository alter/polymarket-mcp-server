# Polymarket Arena Daily Report — 2026-05-03

_Generated: 2026-05-03 00:56 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=53s, size=101KB
- orderbook_collector.log: age=27s, size=97KB
- oil_iran.log: age=26s, size=269KB
- always_no.log: age=454s, size=64KB
- whale_fade.log: age=2s, size=205KB
- arena_ticks.jsonl: age=7s, size=68868KB
- orderbook_snapshots.jsonl: age=27s, size=128977KB
- live_validator.json: age=23s, size=1136KB
- arena_results.json: age=538s, size=576KB
- political_skeptic.json: age=118s, size=5KB
- council.json: age=150s, size=38KB
- theta_decay.json: age=127s, size=1KB
- whale_follower.json: age=1569s, size=18KB
- whale_fade.json: age=missings, size=-KB

## 2. Live Validator

- **Variants:** 206 total, 169 alive, 37 retired
- **Positions:** 2278 open, 14367 closed
- **Realized PnL:** $-0.5950 (actual $0.01 bets)
- **Aggregate equity:** $173232 / $169000 starting = **+2.50%**
- **Exit reasons:** {'sl': 4827, 'tp': 7841, 'htr': 1699}
- **Win3 skips:** 5138
- **Alive families:** {'RS': 68, 'BO': 18, 'BB': 27, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 4, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BB_p10_sd15_follow|pgt30|sany|fany` | 159 | 70% | +12.8% | +13.3% | $2019 |
| `MV4_follow|pgt30|stight|fany` | 47 | 79% | +17.0% | +0.0% | $1400 |
| `BB_p30_sd25_fade|plt30|swide|fany` | 26 | 69% | +25.2% | +56.1% | $1327 |
| `BB_p30_sd25_fade|plt50|swide|fany` | 28 | 68% | +23.0% | +53.5% | $1322 |
| `BB_p30_sd25_fade|pany|swide|fany` | 34 | 62% | +18.4% | +52.6% | $1312 |
| `BB_p30_sd25_fade|plt30|sany|fany` | 28 | 68% | +18.8% | +39.9% | $1264 |
| `BB_p30_sd25_fade|plt70|swide|fany` | 32 | 59% | +16.4% | +53.1% | $1262 |
| `RS_p21_t80_fade|pany|swide|fany` | 29 | 62% | +17.9% | +28.3% | $1259 |
| `RS_p21_t80_fade|plt70|swide|fany` | 28 | 61% | +18.1% | +28.4% | $1254 |
| `RS_p21_t80_fade|plt50|swide|fany` | 28 | 61% | +18.1% | +28.4% | $1254 |

## 3. Arena (multi_strategy)

- **Total:** 1023 strategies, 848 active, 369 profitable
- **Family distribution:** {'mean_re': 170, 'wavelet': 64, 'ensembl': 70, 'bolling': 36, 'zscore': 34, 'forest_': 460, 'hybrid_': 92, 'breakou': 18, 'rsi': 37, 'macd': 18, 'momentu': 24}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S311|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $1649 | 236/56 | 81% |
| `S332|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1413 | 372/70 | 84% |
| `S335|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1402 | 326/59 | 85% |
| `S120|mean_re|p5|e0.01|sloff|tp10%|free|b` | $1389 | 371/63 | 85% |
| `S123|mean_re|p5|e0.01|sloff|tp5%|free|b` | $1385 | 375/63 | 86% |
| `S298|mean_re|p10|e0.01|sloff|tp10%|free|b` | $1375 | 277/50 | 85% |
| `S334|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1368 | 330/66 | 83% |
| `S1003|wavelet|p3|e0.01|sl-20%|tp10%|free|b` | $1366 | 335/70 | 83% |
| `S331|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1362 | 375/78 | 83% |
| `S310|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $1355 | 251/58 | 81% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=1, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=4, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=21/1 (WR 95%), pnl=$+0.2573
- v2_with_fees: W/L=149/159 (WR 48%), pnl=$+0.4482
- v3_aggressive: W/L=21/3 (WR 88%), pnl=$+0.2364
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=39/7 (WR 85%), pnl=$+0.3758

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-02T23:56:30.613791+00:00 (1.0h ago)
- **Pairs:** 324,115 from 321 markets
- **Correlations:**
  - mu_dev_k1: +0.0409
  - mu_dev_k10: +0.0576
  - mu_dev_k2: +0.0496
  - mu_dev_k5: +0.0547
  - obi_l1_k1: +0.0757
  - obi_l1_k10: +0.0926
  - obi_l1_k2: +0.0811
  - obi_l1_k5: +0.0874
  - obi_l3_k1: +0.0427
  - obi_l3_k10: +0.0534
  - obi_l3_k2: +0.0456
  - obi_l3_k5: +0.0506

## 6. News pipeline

- **News signals captured:** 98

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 107130, Closed: 187380, Realized: $-29.4380
