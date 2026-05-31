# Polymarket Arena Daily Report — 2026-05-10

_Generated: 2026-05-10 00:49 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=227s, size=4KB
- orderbook_collector.log: age=78s, size=9KB
- oil_iran.log: age=31s, size=25KB
- always_no.log: age=208s, size=7KB
- whale_fade.log: age=139s, size=21KB
- arena_ticks.jsonl: age=85s, size=95674KB
- orderbook_snapshots.jsonl: age=78s, size=324311KB
- live_validator.json: age=197s, size=1411KB
- arena_results.json: age=83s, size=884KB
- political_skeptic.json: age=123s, size=5KB
- council.json: age=261s, size=55KB
- theta_decay.json: age=315s, size=3KB
- whale_follower.json: age=1480s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=56s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 143 alive, 63 retired
- **Positions:** 3026 open, 10070 closed
- **Realized PnL:** $-1.6325 (actual $0.01 bets)
- **Aggregate equity:** $144061 / $143000 starting = **+0.74%**
- **Exit reasons:** {'tp': 5272, 'sl': 3112, 'htr': 1686}
- **Win3 skips:** 3084
- **Alive families:** {'RS': 40, 'BO': 17, 'BB': 30, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 4, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `RS_p14_t70_follow|pgt30|sany|fany` | 118 | 63% | +4.1% | +18.1% | $1243 |
| `MV5_follow|pgt30|sany|fany` | 67 | 69% | +6.1% | +0.0% | $1206 |
| `RS_p21_t80_follow|plt70|stight|fany` | 27 | 81% | +12.4% | +22.8% | $1168 |
| `BB_p30_sd20_follow|pgt30|sany|fany` | 85 | 68% | +3.8% | +18.0% | $1160 |
| `RS_p14_t80_follow|pgt30|stight|fany` | 36 | 75% | +8.4% | +17.8% | $1150 |
| `BB_p30_sd25_follow|pgt30|sany|fany` | 50 | 68% | +6.0% | +20.9% | $1150 |
| `RS_p21_t80_fade|plt30|sany|fany` | 24 | 62% | +11.4% | +20.6% | $1137 |
| `BB_p20_sd20_fade|plt30|sany|fany` | 23 | 78% | +11.9% | +27.3% | $1137 |
| `RS_p14_t80_follow|pgt50|sany|fany` | 36 | 72% | +7.5% | +17.5% | $1135 |
| `RS_p14_t80_follow|plt70|stight|fany` | 29 | 79% | +8.7% | +22.5% | $1126 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 438 active, 739 profitable
- **Family distribution:** {'forest_': 960, 'rsi': 39, 'mean_re': 174, 'ensembl': 70, 'breakou': 19, 'zscore': 34, 'bolling': 37, 'wavelet': 68, 'momentu': 24, 'hybrid_': 92, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S1048|forest_|p39|e0.69|sl-25%|tp5%|free|b` | $1110 | 62/11 | 85% |
| `S1510|forest_|p42|e0.53|sloff|tp20%|free|b` | $1108 | 86/19 | 82% |
| `S1526|forest_|p43|e0.51|sl-25%|tp5%|free|b` | $1104 | 70/13 | 84% |
| `S1049|forest_|p6|e0.52|sl-20%|tp5%|free|b` | $1100 | 45/37 | 55% |
| `S1488|forest_|p40|e0.60|sloff|tp10%|free|b` | $1093 | 55/14 | 80% |
| `S1440|forest_|p16|e0.55|sl-25%|tp10%|free|b` | $1091 | 85/29 | 75% |
| `S618|forest_|p45|e0.52|sl-25%|tp10%|free|b` | $1091 | 59/9 | 87% |
| `S1191|forest_|p46|e0.54|sl-25%|tp5%|free|b` | $1090 | 70/11 | 86% |
| `S1386|forest_|p41|e0.64|sloff|tp5%|free|b` | $1090 | 55/11 | 83% |
| `S1534|forest_|p39|e0.63|sloff|tp10%|free|b` | $1090 | 44/8 | 85% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=2, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=8, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=22/1 (WR 96%), pnl=$+0.2663
- v2_with_fees: W/L=104/133 (WR 44%), pnl=$-0.0426
- v3_aggressive: W/L=22/3 (WR 88%), pnl=$+0.2454
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=42/9 (WR 82%), pnl=$+0.3821

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-09T23:54:37.850314+00:00 (0.9h ago)
- **Pairs:** 825,596 from 756 markets
- **Correlations:**
  - mu_dev_k1: +0.0498
  - mu_dev_k10: +0.0611
  - mu_dev_k2: +0.0577
  - mu_dev_k5: +0.0621
  - obi_l1_k1: +0.0767
  - obi_l1_k10: +0.0964
  - obi_l1_k2: +0.0834
  - obi_l1_k5: +0.0915
  - obi_l3_k1: +0.0496
  - obi_l3_k10: +0.0605
  - obi_l3_k2: +0.0523
  - obi_l3_k5: +0.0574

## 6. News pipeline

- **News signals captured:** 4003

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 95740, Closed: 175770, Realized: $+5.1635
