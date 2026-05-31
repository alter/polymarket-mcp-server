# Polymarket Arena Daily Report — 2026-05-29

_Generated: 2026-05-29 00:44 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=98s, size=15KB
- orderbook_collector.log: age=8s, size=9KB
- oil_iran.log: age=20s, size=26KB
- always_no.log: age=97s, size=8KB
- whale_fade.log: age=121s, size=23KB
- arena_ticks.jsonl: age=40s, size=157896KB
- orderbook_snapshots.jsonl: age=8s, size=188410KB
- live_validator.json: age=98s, size=1091KB
- arena_results.json: age=77s, size=893KB
- political_skeptic.json: age=129s, size=5KB
- council.json: age=130s, size=61KB
- theta_decay.json: age=222s, size=2KB
- whale_follower.json: age=153s, size=20KB
- whale_fade_grid.json: age=119s, size=32630KB
- maker.json: age=53s, size=3KB

## 2. Live Validator

- **Variants:** 206 total, 84 alive, 122 retired
- **Positions:** 2109 open, 28190 closed
- **Realized PnL:** $-3.8114 (actual $0.01 bets)
- **Aggregate equity:** $86595 / $84000 starting = **+3.09%**
- **Exit reasons:** {'tp': 16190, 'sl': 8928, 'htr': 3072}
- **Win3 skips:** 8549
- **Alive families:** {'BO': 1, 'BB': 21, 'RS': 11, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 1}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BB_p10_sd15_follow|plt70|stight|fany` | 268 | 69% | +4.6% | +31.5% | $1620 |
| `RS_p21_t80_fade|plt30|sany|fany` | 118 | 73% | +9.0% | +20.6% | $1531 |
| `BB_p10_sd15_follow|pgt30|sany|fany` | 675 | 65% | +1.2% | +13.3% | $1410 |
| `BB_p20_sd20_follow|pgt30|sany|fany` | 497 | 67% | +1.4% | +17.7% | $1352 |
| `RS_p21_t80_fade|plt50|swide|fany` | 126 | 72% | +1.6% | +28.4% | $1102 |
| `RS_p21_t80_fade|plt70|swide|fany` | 128 | 72% | +1.5% | +28.4% | $1098 |
| `RS_p21_t80_fade|pany|swide|fany` | 128 | 72% | +1.5% | +28.3% | $1098 |
| `BB_p20_sd25_fade|plt30|swide|fany` | 76 | 75% | +2.5% | +48.8% | $1094 |
| `RS_p21_t80_fade|plt30|swide|fany` | 87 | 72% | +1.7% | +29.3% | $1072 |
| `BB_p10_sd20_follow|plt70|stight|fany` | 48 | 75% | +2.9% | +35.8% | $1068 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 143 active, 685 profitable
- **Family distribution:** {'mean_re': 174, 'zscore': 34, 'rsi': 39, 'bolling': 37, 'forest_': 960, 'breakou': 19, 'ensembl': 70, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S306|mean_re|p10|e0.02|sl-25%|tp10%|free|s` | $3646 | 1411/373 | 79% |
| `S976|zscore|p50|e2.00|sl-20%|tp5%|free|b` | $2804 | 2087/559 | 79% |
| `S977|zscore|p50|e2.00|sl-20%|tp10%|free|b` | $2761 | 2024/567 | 78% |
| `S252|zscore|p50|e2.00|sl-10%|tp5%|free|b` | $2648 | 2185/665 | 77% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $2460 | 789/261 | 75% |
| `S305|mean_re|p5|e0.01|sl-25%|tp10%|free|s` | $2303 | 2537/697 | 78% |
| `S979|zscore|p50|e2.50|sl-20%|tp10%|free|b` | $2260 | 1578/450 | 78% |
| `S213|bolling|p20|e2.50|sl-25%|tp10%|free|b` | $2222 | 2511/830 | 75% |
| `S982|zscore|p20|e2.50|sl-20%|tp5%|free|b` | $2214 | 2529/827 | 75% |
| `S1000|bolling|p20|e2.50|sl-20%|tp5%|free|b` | $2214 | 2529/827 | 75% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=5, W/L=10/0 (WR 100%), realized=$+0.0144, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=29/6 (WR 83%), pnl=$+0.4076
- v2_with_fees: W/L=901/1278 (WR 41%), pnl=$-0.4904
- v3_aggressive: W/L=30/8 (WR 79%), pnl=$+0.3521
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=49/15 (WR 77%), pnl=$+0.5134

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-29T00:19:30.013764+00:00 (0.4h ago)
- **Pairs:** 482,130 from 400 markets
- **Correlations:**
  - mu_dev_k1: +0.0575
  - mu_dev_k10: +0.0518
  - mu_dev_k2: +0.0575
  - mu_dev_k5: +0.0553
  - obi_l1_k1: +0.0841
  - obi_l1_k10: +0.1027
  - obi_l1_k2: +0.0912
  - obi_l1_k5: +0.0982
  - obi_l3_k1: +0.0525
  - obi_l3_k10: +0.0699
  - obi_l3_k2: +0.0566
  - obi_l3_k5: +0.0636

## 6. News pipeline

- **News signals captured:** 13685

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 65340, Closed: 108280, Realized: $-2.6375
