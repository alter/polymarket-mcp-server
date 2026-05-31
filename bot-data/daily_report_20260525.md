# Polymarket Arena Daily Report — 2026-05-25

_Generated: 2026-05-25 01:24 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=443s, size=73KB
- orderbook_collector.log: age=1s, size=65KB
- oil_iran.log: age=5s, size=172KB
- always_no.log: age=527s, size=47KB
- whale_fade.log: age=123s, size=141KB
- arena_ticks.jsonl: age=36s, size=121513KB
- orderbook_snapshots.jsonl: age=1s, size=431890KB
- live_validator.json: age=142s, size=1254KB
- arena_results.json: age=293s, size=889KB
- political_skeptic.json: age=7s, size=5KB
- council.json: age=37s, size=61KB
- theta_decay.json: age=300s, size=3KB
- whale_follower.json: age=114s, size=20KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=1s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 148 alive, 58 retired
- **Positions:** 2555 open, 19920 closed
- **Realized PnL:** $+1.3619 (actual $0.01 bets)
- **Aggregate equity:** $164772 / $148000 starting = **+11.33%**
- **Exit reasons:** {'tp': 11662, 'sl': 6066, 'htr': 2192}
- **Win3 skips:** 6176
- **Alive families:** {'RS': 58, 'BO': 10, 'BB': 27, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 1, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `RS_p14_t80_follow|pany|sany|fany` | 169 | 65% | +27.6% | +11.4% | $3336 |
| `RS_p21_t70_follow|plt50|sany|fany` | 145 | 73% | +17.1% | +9.0% | $2242 |
| `RS_p14_t70_follow|pany|sany|fany` | 287 | 70% | +8.2% | +7.5% | $2171 |
| `RS_p14_t70_follow|pgt30|sany|fany` | 223 | 71% | +10.0% | +18.1% | $2114 |
| `BB_p10_sd15_follow|pgt30|sany|fany` | 255 | 67% | +6.2% | +13.3% | $1788 |
| `RS_p14_t65_follow|pany|sany|fany` | 361 | 67% | +4.2% | +5.1% | $1758 |
| `RS_p14_t70_follow|plt70|sany|fany` | 252 | 69% | +5.6% | +9.2% | $1700 |
| `RS_p7_t80_follow|pany|sany|fany` | 274 | 68% | +4.4% | +7.3% | $1610 |
| `RS_p14_t75_follow|pany|sany|fany` | 226 | 66% | +5.1% | +8.9% | $1579 |
| `RS_p14_t65_follow|plt70|sany|fany` | 321 | 66% | +3.5% | +6.8% | $1569 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 304 active, 799 profitable
- **Family distribution:** {'mean_re': 174, 'rsi': 39, 'forest_': 960, 'zscore': 34, 'bolling': 37, 'breakou': 19, 'hybrid_': 92, 'ensembl': 70, 'wavelet': 68, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S306|mean_re|p10|e0.02|sl-25%|tp10%|free|s` | $3078 | 568/162 | 78% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $2460 | 789/261 | 75% |
| `S174|rsi|p14|e30.00|sl-25%|tp10%|free|b` | $1715 | 746/322 | 70% |
| `S175|rsi|p14|e30.00|sloff|tp10%|free|b` | $1713 | 720/298 | 71% |
| `S1236|forest_|p15|e0.59|sloff|tp10%|free|b` | $1663 | 88/0 | 100% |
| `S177|rsi|p14|e25.00|sl-25%|tp10%|free|b` | $1636 | 677/295 | 70% |
| `S178|rsi|p14|e25.00|sloff|tp10%|free|b` | $1623 | 648/276 | 70% |
| `S184|rsi|p14|e20.00|sloff|tp10%|free|b` | $1586 | 601/255 | 70% |
| `S976|zscore|p50|e2.00|sl-20%|tp5%|free|b` | $1586 | 694/226 | 75% |
| `S1062|forest_|p5|e0.54|sloff|tp5%|free|b` | $1569 | 745/245 | 75% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=7, W/L=8/0 (WR 100%), realized=$+0.0109, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/5 (WR 82%), pnl=$+0.2361
- v2_with_fees: W/L=717/1031 (WR 41%), pnl=$-0.7875
- v3_aggressive: W/L=24/7 (WR 77%), pnl=$+0.2205
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/14 (WR 75%), pnl=$+0.3419

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-25T00:57:17.640269+00:00 (0.5h ago)
- **Pairs:** 1,099,155 from 1378 markets
- **Correlations:**
  - mu_dev_k1: +0.0540
  - mu_dev_k10: +0.0602
  - mu_dev_k2: +0.0604
  - mu_dev_k5: +0.0628
  - obi_l1_k1: +0.0795
  - obi_l1_k10: +0.0956
  - obi_l1_k2: +0.0858
  - obi_l1_k5: +0.0924
  - obi_l3_k1: +0.0530
  - obi_l3_k10: +0.0627
  - obi_l3_k2: +0.0560
  - obi_l3_k5: +0.0597

## 6. News pipeline

- **News signals captured:** 8328

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 134710, Closed: 146810, Realized: $-70.6180
