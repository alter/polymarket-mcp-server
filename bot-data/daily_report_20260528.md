# Polymarket Arena Daily Report — 2026-05-28

_Generated: 2026-05-28 00:13 UTC_

## 1. System health

- **stalled**: True, alerts: ['whale_fade_grid.json not updated in 1745s']
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=504s, size=14KB
- orderbook_collector.log: age=27s, size=9KB
- oil_iran.log: age=30s, size=25KB
- always_no.log: age=509s, size=7KB
- whale_fade.log: age=537s, size=2KB
- arena_ticks.jsonl: age=59s, size=149116KB
- orderbook_snapshots.jsonl: age=27s, size=157440KB
- live_validator.json: age=204s, size=1130KB
- arena_results.json: age=477s, size=893KB
- political_skeptic.json: age=538s, size=5KB
- council.json: age=551s, size=60KB
- theta_decay.json: age=246s, size=2KB
- whale_follower.json: age=1769s, size=20KB
- whale_fade_grid.json: age=1745s, size=17481KB
- maker.json: age=37s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 89 alive, 117 retired
- **Positions:** 2214 open, 26386 closed
- **Realized PnL:** $-3.2777 (actual $0.01 bets)
- **Aggregate equity:** $93501 / $89000 starting = **+5.06%**
- **Exit reasons:** {'tp': 15166, 'sl': 8247, 'htr': 2973}
- **Win3 skips:** 8011
- **Alive families:** {'BO': 1, 'RS': 13, 'BB': 24, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 1}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BB_p10_sd15_follow|pgt30|sany|fany` | 523 | 66% | +3.1% | +13.3% | $1808 |
| `RS_p14_t80_follow|pany|sany|fany` | 312 | 59% | +5.0% | +11.4% | $1786 |
| `BB_p10_sd15_follow|plt70|stight|fany` | 223 | 68% | +3.9% | +31.5% | $1436 |
| `RS_p21_t80_fade|plt30|sany|fany` | 89 | 73% | +9.6% | +20.6% | $1426 |
| `RS_p21_t70_follow|plt50|sany|fany` | 276 | 66% | +2.2% | +9.0% | $1308 |
| `BB_p20_sd20_follow|pgt30|sany|fany` | 397 | 66% | +0.8% | +17.7% | $1153 |
| `BB_p50_sd25_follow|pgt30|sany|fany` | 253 | 63% | +1.0% | +22.9% | $1126 |
| `BB_p20_sd25_fade|plt30|swide|fany` | 60 | 77% | +3.0% | +48.8% | $1088 |
| `BB_p30_sd25_follow|pgt30|sany|fany` | 235 | 62% | +0.7% | +20.9% | $1084 |
| `BB_p20_sd25_fade|plt70|swide|fany` | 84 | 73% | +1.8% | +46.5% | $1074 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 144 active, 684 profitable
- **Family distribution:** {'mean_re': 174, 'zscore': 34, 'rsi': 39, 'forest_': 960, 'bolling': 37, 'breakou': 19, 'ensembl': 70, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S306|mean_re|p10|e0.02|sl-25%|tp10%|free|s` | $3543 | 1222/341 | 78% |
| `S976|zscore|p50|e2.00|sl-20%|tp5%|free|b` | $2576 | 1865/513 | 78% |
| `S977|zscore|p50|e2.00|sl-20%|tp10%|free|b` | $2564 | 1810/521 | 78% |
| `S252|zscore|p50|e2.00|sl-10%|tp5%|free|b` | $2492 | 1955/602 | 76% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $2460 | 789/261 | 75% |
| `S305|mean_re|p5|e0.01|sl-25%|tp10%|free|s` | $2169 | 2206/639 | 78% |
| `S979|zscore|p50|e2.50|sl-20%|tp10%|free|b` | $2096 | 1408/414 | 77% |
| `S1179|forest_|p37|e0.50|sl-25%|tp10%|free|b` | $2084 | 1047/411 | 72% |
| `S213|bolling|p20|e2.50|sl-25%|tp10%|free|b` | $2060 | 2242/761 | 75% |
| `S983|zscore|p20|e2.50|sl-20%|tp10%|free|b` | $2055 | 2248/764 | 75% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=5, W/L=10/0 (WR 100%), realized=$+0.0144, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=29/6 (WR 83%), pnl=$+0.4076
- v2_with_fees: W/L=849/1201 (WR 41%), pnl=$-0.5403
- v3_aggressive: W/L=30/8 (WR 79%), pnl=$+0.3521
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=49/15 (WR 77%), pnl=$+0.5134

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-27T23:13:34.256382+00:00 (1.0h ago)
- **Pairs:** 400,764 from 374 markets
- **Correlations:**
  - mu_dev_k1: +0.0708
  - mu_dev_k10: +0.0648
  - mu_dev_k2: +0.0723
  - mu_dev_k5: +0.0694
  - obi_l1_k1: +0.0850
  - obi_l1_k10: +0.1025
  - obi_l1_k2: +0.0922
  - obi_l1_k5: +0.0996
  - obi_l3_k1: +0.0516
  - obi_l3_k10: +0.0711
  - obi_l3_k2: +0.0564
  - obi_l3_k5: +0.0637

## 6. News pipeline

- **News signals captured:** 13031

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 36020, Closed: 2790, Realized: $-9.7695
