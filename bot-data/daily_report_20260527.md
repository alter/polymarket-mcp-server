# Polymarket Arena Daily Report — 2026-05-27

_Generated: 2026-05-27 00:26 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=561s, size=211KB
- orderbook_collector.log: age=20s, size=150KB
- oil_iran.log: age=18s, size=408KB
- always_no.log: age=114s, size=103KB
- whale_fade.log: age=231s, size=324KB
- arena_ticks.jsonl: age=60s, size=139247KB
- orderbook_snapshots.jsonl: age=20s, size=500550KB
- live_validator.json: age=261s, size=1168KB
- arena_results.json: age=375s, size=893KB
- political_skeptic.json: age=519s, size=5KB
- council.json: age=13s, size=61KB
- theta_decay.json: age=0s, size=2KB
- whale_follower.json: age=1461s, size=20KB
- whale_fade_grid.json: age=64s, size=65497KB
- maker.json: age=51s, size=9KB

## 2. Live Validator

- **Variants:** 206 total, 112 alive, 94 retired
- **Positions:** 2316 open, 24093 closed
- **Realized PnL:** $-3.4397 (actual $0.01 bets)
- **Aggregate equity:** $112976 / $112000 starting = **+0.87%**
- **Exit reasons:** {'tp': 13906, 'sl': 7456, 'htr': 2731}
- **Win3 skips:** 7325
- **Alive families:** {'RS': 26, 'BO': 9, 'BB': 26, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 1}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BB_p10_sd15_follow|plt70|stight|fany` | 177 | 71% | +11.1% | +31.5% | $1986 |
| `RS_p14_t80_follow|pany|sany|fany` | 250 | 59% | +6.0% | +11.4% | $1744 |
| `BB_p10_sd15_follow|pgt30|sany|fany` | 399 | 66% | +3.4% | +13.3% | $1674 |
| `RS_p21_t80_fade|plt30|sany|fany` | 71 | 69% | +3.6% | +20.6% | $1128 |
| `BO_p50_follow|plt30|sany|fany` | 74 | 76% | +3.4% | +19.5% | $1126 |
| `BB_p10_sd20_follow|plt70|stight|fany` | 33 | 85% | +5.5% | +35.8% | $1090 |
| `RS_p14_t70_follow|pgt30|stight|fany` | 179 | 64% | +0.8% | +14.6% | $1069 |
| `MO_p30_t20_fade|pgt70|sany|ffree_only` | 25 | 84% | +5.2% | +110.6% | $1065 |
| `ME_p100_t10_fade|pgt70|sany|ffree_only` | 30 | 80% | +4.0% | +108.6% | $1060 |
| `ME_p5_t10_fade|pgt70|sany|ffree_only` | 17 | 88% | +6.5% | +149.0% | $1055 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 145 active, 681 profitable
- **Family distribution:** {'mean_re': 174, 'rsi': 39, 'zscore': 34, 'bolling': 37, 'forest_': 960, 'breakou': 19, 'ensembl': 70, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S306|mean_re|p10|e0.02|sl-25%|tp10%|free|s` | $3450 | 1039/277 | 79% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $2460 | 789/261 | 75% |
| `S977|zscore|p50|e2.00|sl-20%|tp10%|free|b` | $2397 | 1487/422 | 78% |
| `S976|zscore|p50|e2.00|sl-20%|tp5%|free|b` | $2393 | 1531/415 | 79% |
| `S252|zscore|p50|e2.00|sl-10%|tp5%|free|b` | $2308 | 1607/493 | 77% |
| `S305|mean_re|p5|e0.01|sl-25%|tp10%|free|s` | $2057 | 1866/537 | 78% |
| `S983|zscore|p20|e2.50|sl-20%|tp10%|free|b` | $1990 | 1848/605 | 75% |
| `S1001|bolling|p20|e2.50|sl-20%|tp10%|free|b` | $1990 | 1848/605 | 75% |
| `S213|bolling|p20|e2.50|sl-25%|tp10%|free|b` | $1983 | 1843/602 | 75% |
| `S982|zscore|p20|e2.50|sl-20%|tp5%|free|b` | $1978 | 1855/601 | 76% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=5, W/L=10/0 (WR 100%), realized=$+0.0144, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=27/6 (WR 82%), pnl=$+0.3624
- v2_with_fees: W/L=803/1137 (WR 41%), pnl=$-0.5602
- v3_aggressive: W/L=28/8 (WR 78%), pnl=$+0.3431
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=47/15 (WR 76%), pnl=$+0.4682

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-26T23:27:18.083234+00:00 (1.0h ago)
- **Pairs:** 1,273,550 from 1461 markets
- **Correlations:**
  - mu_dev_k1: +0.0563
  - mu_dev_k10: +0.0596
  - mu_dev_k2: +0.0619
  - mu_dev_k5: +0.0624
  - obi_l1_k1: +0.0801
  - obi_l1_k10: +0.0977
  - obi_l1_k2: +0.0869
  - obi_l1_k5: +0.0939
  - obi_l3_k1: +0.0523
  - obi_l3_k10: +0.0643
  - obi_l3_k2: +0.0553
  - obi_l3_k5: +0.0601

## 6. News pipeline

- **News signals captured:** 10582

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 142350, Closed: 282980, Realized: $+42.7990
