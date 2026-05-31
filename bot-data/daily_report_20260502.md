# Polymarket Arena Daily Report — 2026-05-02

_Generated: 2026-05-02 04:53 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=2s, size=0KB
- orderbook_collector.log: age=2s, size=0KB
- oil_iran.log: age=0s, size=0KB
- always_no.log: age=0s, size=0KB
- whale_fade.log: age=2s, size=0KB
- arena_ticks.jsonl: age=55s, size=61528KB
- orderbook_snapshots.jsonl: age=12s, size=99795KB
- live_validator.json: age=10s, size=723KB
- arena_results.json: age=301s, size=546KB
- political_skeptic.json: age=298s, size=5KB
- council.json: age=308s, size=20KB
- theta_decay.json: age=291s, size=1KB
- whale_follower.json: age=310s, size=18KB
- whale_fade.json: age=missings, size=-KB

## 2. Live Validator

- **Variants:** 206 total, 199 alive, 7 retired
- **Positions:** 1293 open, 7318 closed
- **Realized PnL:** $-0.6062 (actual $0.01 bets)
- **Aggregate equity:** $197706 / $199000 starting = **-0.65%**
- **Exit reasons:** {'sl': 2777, 'tp': 3814, 'htr': 727}
- **Win3 skips:** 2728
- **Alive families:** {'BO': 32, 'RS': 79, 'BB': 32, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 4, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BB_p30_sd25_fade|plt30|swide|fany` | 16 | 75% | +17.2% | +56.1% | $1138 |
| `BB_p30_sd25_fade|plt70|swide|fany` | 17 | 71% | +15.0% | +53.1% | $1128 |
| `BB_p30_sd25_fade|plt50|swide|fany` | 17 | 71% | +15.0% | +53.5% | $1128 |
| `BB_p30_sd25_fade|pany|swide|fany` | 17 | 71% | +15.0% | +52.6% | $1128 |
| `BB_p30_sd25_fade|plt30|sany|fany` | 14 | 71% | +16.1% | +39.9% | $1113 |
| `BB_p30_sd20_follow|pgt30|sany|fany` | 46 | 70% | +4.2% | +18.0% | $1096 |
| `RS_p14_t70_follow|pgt30|sany|fany` | 73 | 64% | +2.6% | +18.1% | $1095 |
| `BB_p20_sd20_follow|pgt30|sany|fany` | 45 | 69% | +4.0% | +17.7% | $1090 |
| `BB_p10_sd15_follow|pgt30|sany|fany` | 68 | 63% | +2.5% | +13.3% | $1084 |
| `RS_p7_t80_follow|pgt30|sany|fany` | 78 | 60% | +2.1% | +19.5% | $1080 |

## 3. Arena (multi_strategy)

- **Total:** 1023 strategies, 848 active, 420 profitable
- **Family distribution:** {'mean_re': 170, 'wavelet': 64, 'ensembl': 70, 'bolling': 36, 'zscore': 34, 'forest_': 460, 'hybrid_': 92, 'breakou': 18, 'rsi': 37, 'macd': 18, 'momentu': 24}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S311|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $1398 | 178/38 | 82% |
| `S332|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1330 | 288/48 | 86% |
| `S335|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1317 | 248/39 | 86% |
| `S331|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1300 | 292/55 | 84% |
| `S334|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1287 | 252/46 | 85% |
| `S1003|wavelet|p3|e0.01|sl-20%|tp10%|free|b` | $1287 | 255/50 | 84% |
| `S298|mean_re|p10|e0.01|sloff|tp10%|free|b` | $1286 | 217/31 | 88% |
| `S120|mean_re|p5|e0.01|sloff|tp10%|free|b` | $1284 | 289/43 | 87% |
| `S123|mean_re|p5|e0.01|sloff|tp5%|free|b` | $1280 | 292/43 | 87% |
| `S1002|wavelet|p3|e0.01|sl-20%|tp5%|free|b` | $1267 | 256/50 | 84% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=1, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=4, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=21/0 (WR 100%), pnl=$+0.2673
- v2_with_fees: W/L=108/103 (WR 51%), pnl=$+0.5191
- v3_aggressive: W/L=21/2 (WR 91%), pnl=$+0.2464
- v4_conservative: W/L=0/0 (WR 0%), pnl=$+0.0000
- v5_all_cats: W/L=39/6 (WR 87%), pnl=$+0.3858

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-02T03:27:20.848835+00:00 (0.4h ago)
- **Pairs:** 253,784 from 241 markets
- **Correlations:**
  - mu_dev_k1: +0.0351
  - mu_dev_k10: +0.0577
  - mu_dev_k2: +0.0452
  - mu_dev_k5: +0.0513
  - obi_l1_k1: +0.0728
  - obi_l1_k10: +0.0978
  - obi_l1_k2: +0.0801
  - obi_l1_k5: +0.0897
  - obi_l3_k1: +0.0401
  - obi_l3_k10: +0.0587
  - obi_l3_k2: +0.0452
  - obi_l3_k5: +0.0529

## 6. News pipeline

- **News signals captured:** 88

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 62150, Closed: 93970, Realized: $+29.5940
