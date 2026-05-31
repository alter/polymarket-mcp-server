# Polymarket Arena Daily Report — 2026-05-26

_Generated: 2026-05-26 01:26 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=475s, size=60KB
- orderbook_collector.log: age=6s, size=39KB
- oil_iran.log: age=9s, size=106KB
- always_no.log: age=524s, size=30KB
- whale_fade.log: age=167s, size=92KB
- arena_ticks.jsonl: age=69s, size=128859KB
- orderbook_snapshots.jsonl: age=6s, size=462717KB
- live_validator.json: age=174s, size=1167KB
- arena_results.json: age=274s, size=893KB
- political_skeptic.json: age=516s, size=5KB
- council.json: age=564s, size=63KB
- theta_decay.json: age=277s, size=2KB
- whale_follower.json: age=1759s, size=20KB
- whale_fade_grid.json: age=197s, size=69919KB
- maker.json: age=44s, size=5KB

## 2. Live Validator

- **Variants:** 206 total, 120 alive, 86 retired
- **Positions:** 2316 open, 22411 closed
- **Realized PnL:** $-3.2165 (actual $0.01 bets)
- **Aggregate equity:** $120990 / $120000 starting = **+0.83%**
- **Exit reasons:** {'tp': 13011, 'sl': 6846, 'htr': 2554}
- **Win3 skips:** 6869
- **Alive families:** {'RS': 32, 'BO': 10, 'BB': 26, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 1, 'MV4': 1}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BB_p10_sd15_follow|pgt30|sany|fany` | 312 | 68% | +7.0% | +13.3% | $2090 |
| `RS_p14_t80_follow|pany|sany|fany` | 204 | 60% | +9.4% | +11.4% | $1956 |
| `BB_p10_sd15_follow|plt70|stight|fany` | 130 | 70% | +10.9% | +31.5% | $1711 |
| `BO_p50_follow|plt30|sany|fany` | 60 | 78% | +17.9% | +19.5% | $1538 |
| `BB_p20_sd20_follow|pgt30|sany|fany` | 243 | 67% | +3.2% | +17.7% | $1392 |
| `RS_p21_t80_fade|plt30|sany|fany` | 56 | 71% | +7.6% | +20.6% | $1214 |
| `RS_p21_t70_follow|plt50|sany|fany` | 175 | 67% | +1.9% | +9.0% | $1166 |
| `BO_p100_follow|plt30|sany|fany` | 48 | 77% | +6.7% | +28.1% | $1160 |
| `BB_p50_sd25_follow|pgt30|sany|fany` | 158 | 62% | +1.5% | +22.9% | $1117 |
| `BB_p30_sd25_follow|pgt30|sany|fany` | 151 | 62% | +1.5% | +20.9% | $1113 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 148 active, 685 profitable
- **Family distribution:** {'mean_re': 174, 'rsi': 39, 'zscore': 34, 'forest_': 960, 'bolling': 37, 'breakou': 19, 'ensembl': 70, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S306|mean_re|p10|e0.02|sl-25%|tp10%|free|s` | $3307 | 792/222 | 78% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $2460 | 789/261 | 75% |
| `S977|zscore|p50|e2.00|sl-20%|tp10%|free|b` | $2048 | 1012/315 | 76% |
| `S976|zscore|p50|e2.00|sl-20%|tp5%|free|b` | $2037 | 1052/310 | 77% |
| `S252|zscore|p50|e2.00|sl-10%|tp5%|free|b` | $1940 | 1110/383 | 74% |
| `S1236|forest_|p15|e0.59|sloff|tp10%|free|b` | $1903 | 118/1 | 99% |
| `S1483|forest_|p28|e0.53|sl-20%|tp10%|free|b` | $1893 | 555/224 | 71% |
| `S305|mean_re|p5|e0.01|sl-25%|tp10%|free|s` | $1818 | 1358/426 | 76% |
| `S1386|forest_|p41|e0.64|sloff|tp5%|free|b` | $1775 | 486/155 | 76% |
| `S1179|forest_|p37|e0.50|sl-25%|tp10%|free|b` | $1715 | 641/267 | 71% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=6, W/L=9/0 (WR 100%), realized=$+0.0130, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=24/6 (WR 80%), pnl=$+0.2344
- v2_with_fees: W/L=759/1077 (WR 41%), pnl=$-0.7071
- v3_aggressive: W/L=25/8 (WR 76%), pnl=$+0.2151
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=44/15 (WR 75%), pnl=$+0.3402

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-26T00:42:06.149242+00:00 (0.7h ago)
- **Pairs:** 1,177,896 from 1424 markets
- **Correlations:**
  - mu_dev_k1: +0.0556
  - mu_dev_k10: +0.0591
  - mu_dev_k2: +0.0617
  - mu_dev_k5: +0.0626
  - obi_l1_k1: +0.0802
  - obi_l1_k10: +0.0970
  - obi_l1_k2: +0.0868
  - obi_l1_k5: +0.0937
  - obi_l3_k1: +0.0533
  - obi_l3_k10: +0.0652
  - obi_l3_k2: +0.0566
  - obi_l3_k5: +0.0614

## 6. News pipeline

- **News signals captured:** 9190

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 153400, Closed: 201630, Realized: $+4.6135
