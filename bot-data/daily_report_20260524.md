# Polymarket Arena Daily Report — 2026-05-24

_Generated: 2026-05-24 02:57 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=71s, size=71KB
- orderbook_collector.log: age=-242s, size=51KB
- oil_iran.log: age=-223s, size=141KB
- always_no.log: age=56s, size=50KB
- whale_fade.log: age=440s, size=140KB
- arena_ticks.jsonl: age=33s, size=115598KB
- orderbook_snapshots.jsonl: age=-242s, size=401547KB
- live_validator.json: age=41s, size=683KB
- arena_results.json: age=443s, size=886KB
- political_skeptic.json: age=427s, size=5KB
- council.json: age=62s, size=41KB
- theta_decay.json: age=46s, size=4KB
- whale_follower.json: age=443s, size=20KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=38s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 192 alive, 14 retired
- **Positions:** 1111 open, 10241 closed
- **Realized PnL:** $-1.8416 (actual $0.01 bets)
- **Aggregate equity:** $186164 / $192000 starting = **-3.04%**
- **Exit reasons:** {'tp': 5880, 'sl': 3272, 'htr': 1089}
- **Win3 skips:** 3024
- **Alive families:** {'BO': 36, 'RS': 70, 'BB': 30, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 4, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `MV5_follow|pgt30|stight|fany` | 32 | 75% | +12.4% | +0.0% | $1199 |
| `MV5_follow|pany|stight|fany` | 34 | 74% | +11.4% | +0.0% | $1194 |
| `RS_p14_t70_follow|pgt30|sany|fany` | 106 | 73% | +3.4% | +18.1% | $1178 |
| `RS_p14_t70_follow|pany|sany|fany` | 136 | 70% | +2.3% | +7.5% | $1154 |
| `BB_p10_sd15_follow|plt70|stight|fany` | 43 | 72% | +6.4% | +31.5% | $1138 |
| `BB_p10_sd15_follow|pgt30|sany|fany` | 118 | 69% | +2.1% | +13.3% | $1124 |
| `RS_p21_t80_fade|plt30|sany|fany` | 18 | 61% | +13.2% | +20.6% | $1118 |
| `RS_p21_t70_follow|plt50|sany|fany` | 67 | 72% | +3.3% | +9.0% | $1110 |
| `BO_p100_follow|plt30|sany|fany` | 23 | 87% | +9.5% | +28.1% | $1109 |
| `BO_p20_fade|plt70|sany|fany` | 140 | 59% | +1.4% | +5.2% | $1098 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 392 active, 808 profitable
- **Family distribution:** {'rsi': 39, 'mean_re': 174, 'forest_': 960, 'breakou': 19, 'bolling': 37, 'zscore': 34, 'hybrid_': 92, 'ensembl': 70, 'wavelet': 68, 'macd': 18, 'momentu': 24}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $2868 | 732/232 | 76% |
| `S306|mean_re|p10|e0.02|sl-25%|tp10%|free|s` | $2731 | 315/97 | 76% |
| `S174|rsi|p14|e30.00|sl-25%|tp10%|free|b` | $2238 | 723/303 | 70% |
| `S175|rsi|p14|e30.00|sloff|tp10%|free|b` | $2203 | 698/282 | 71% |
| `S177|rsi|p14|e25.00|sl-25%|tp10%|free|b` | $2107 | 658/281 | 70% |
| `S178|rsi|p14|e25.00|sloff|tp10%|free|b` | $2088 | 632/265 | 70% |
| `S183|rsi|p14|e20.00|sl-25%|tp10%|free|b` | $2032 | 609/260 | 70% |
| `S184|rsi|p14|e20.00|sloff|tp10%|free|b` | $2007 | 589/244 | 71% |
| `S173|rsi|p14|e30.00|sl-10%|tp5%|free|b` | $1570 | 807/354 | 70% |
| `S179|rsi|p14|e35.00|sl-10%|tp5%|free|b` | $1527 | 891/413 | 68% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=9, W/L=6/0 (WR 100%), realized=$+0.0083, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/5 (WR 82%), pnl=$+0.2361
- v2_with_fees: W/L=653/969 (WR 40%), pnl=$-1.1411
- v3_aggressive: W/L=24/7 (WR 77%), pnl=$+0.2205
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/14 (WR 75%), pnl=$+0.3419

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-23T22:47:20.724627+00:00 (4.2h ago)
- **Pairs:** 1,023,263 from 1304 markets
- **Correlations:**
  - mu_dev_k1: +0.0526
  - mu_dev_k10: +0.0595
  - mu_dev_k2: +0.0591
  - mu_dev_k5: +0.0617
  - obi_l1_k1: +0.0782
  - obi_l1_k10: +0.0946
  - obi_l1_k2: +0.0843
  - obi_l1_k5: +0.0908
  - obi_l3_k1: +0.0520
  - obi_l3_k10: +0.0605
  - obi_l3_k2: +0.0545
  - obi_l3_k5: +0.0578

## 6. News pipeline

- **News signals captured:** 7233

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 65430, Closed: 75770, Realized: $-21.3445
