# Polymarket Arena Daily Report — 2026-05-09

_Generated: 2026-05-09 00:35 UTC_

## 1. System health

- **stalled**: True, alerts: ['arena_ticks.jsonl not updated in 3638s', 'orderbook_snapshots.jsonl not updated in 3677s', 'live_validator.json not updated in 3642s', 'arena_results.json not updated in 7315s', 'political_skeptic.json not updated in 3675s', 'theta_decay.json not updated in 3677s', 'whale_follower.json not updated in 3675s', 'maker.json not updated in 3651s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=0s, size=0KB
- orderbook_collector.log: age=0s, size=0KB
- oil_iran.log: age=0s, size=0KB
- always_no.log: age=0s, size=0KB
- whale_fade.log: age=0s, size=0KB
- arena_ticks.jsonl: age=3638s, size=88994KB
- orderbook_snapshots.jsonl: age=3677s, size=298356KB
- live_validator.json: age=3642s, size=1226KB
- arena_results.json: age=7315s, size=862KB
- political_skeptic.json: age=3675s, size=5KB
- council.json: age=13s, size=51KB
- theta_decay.json: age=3677s, size=3KB
- whale_follower.json: age=3675s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=3651s, size=0KB

## 2. Live Validator

- **Variants:** 206 total, 206 alive, 0 retired
- **Positions:** 2567 open, 4800 closed
- **Realized PnL:** $-0.4964 (actual $0.01 bets)
- **Aggregate equity:** $203518 / $206000 starting = **-1.20%**
- **Exit reasons:** {'tp': 2855, 'sl': 1442, 'htr': 503}
- **Win3 skips:** 1183
- **Alive families:** {'BO': 37, 'RS': 81, 'BB': 32, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 4, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `RS_p7_t65_follow|plt70|stight|fany` | 41 | 80% | +8.2% | +19.7% | $1169 |
| `RS_p7_t65_follow|pany|stight|fany` | 64 | 75% | +4.4% | +11.7% | $1142 |
| `RS_p7_t65_follow|pgt50|stight|fany` | 48 | 75% | +5.8% | +13.0% | $1140 |
| `RS_p7_t65_follow|pgt50|sany|fany` | 48 | 73% | +4.4% | +12.1% | $1105 |
| `RS_p7_t75_follow|pgt30|stight|fany` | 32 | 78% | +5.9% | +14.7% | $1094 |
| `RS_p7_t80_follow|plt70|stight|fany` | 25 | 80% | +7.4% | +23.9% | $1092 |
| `RS_p7_t75_follow|plt70|stight|fany` | 28 | 79% | +6.6% | +21.9% | $1092 |
| `RS_p7_t70_follow|plt70|stight|fany` | 34 | 76% | +5.4% | +20.2% | $1092 |
| `RS_p14_t70_follow|pgt30|stight|fany` | 25 | 80% | +7.2% | +14.6% | $1090 |
| `RS_p7_t80_follow|pany|stight|fany` | 37 | 76% | +4.8% | +15.6% | $1089 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 1212 active, 807 profitable
- **Family distribution:** {'forest_': 960, 'breakou': 19, 'mean_re': 174, 'ensembl': 70, 'rsi': 39, 'zscore': 34, 'bolling': 37, 'momentu': 24, 'wavelet': 68, 'hybrid_': 92, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S1510|forest_|p42|e0.53|sloff|tp20%|free|b` | $1112 | 59/8 | 88% |
| `S1048|forest_|p39|e0.69|sl-25%|tp5%|free|b` | $1099 | 46/8 | 85% |
| `S1526|forest_|p43|e0.51|sl-25%|tp5%|free|b` | $1093 | 49/5 | 91% |
| `S618|forest_|p45|e0.52|sl-25%|tp10%|free|b` | $1091 | 59/9 | 87% |
| `S1386|forest_|p41|e0.64|sloff|tp5%|free|b` | $1090 | 39/3 | 93% |
| `S1191|forest_|p46|e0.54|sl-25%|tp5%|free|b` | $1090 | 70/11 | 86% |
| `S634|forest_|p45|e0.52|sl-25%|tp10%|free|b` | $1086 | 73/18 | 80% |
| `S1049|forest_|p6|e0.52|sl-20%|tp5%|free|b` | $1084 | 33/27 | 55% |
| `S280|breakou|p10|e1.00|sl-10%|tp15%|free|b` | $1083 | 12/11 | 52% |
| `S1488|forest_|p40|e0.60|sloff|tp10%|free|b` | $1083 | 37/6 | 86% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=2, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=7, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=22/1 (WR 96%), pnl=$+0.2663
- v2_with_fees: W/L=48/68 (WR 41%), pnl=$-0.0737
- v3_aggressive: W/L=22/3 (WR 88%), pnl=$+0.2454
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=42/9 (WR 82%), pnl=$+0.3821

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-08T22:50:05.589489+00:00 (0.8h ago)
- **Pairs:** 762,763 from 663 markets
- **Correlations:**
  - mu_dev_k1: +0.0495
  - mu_dev_k10: +0.0640
  - mu_dev_k2: +0.0588
  - mu_dev_k5: +0.0648
  - obi_l1_k1: +0.0766
  - obi_l1_k10: +0.0990
  - obi_l1_k2: +0.0843
  - obi_l1_k5: +0.0940
  - obi_l3_k1: +0.0489
  - obi_l3_k10: +0.0625
  - obi_l3_k2: +0.0527
  - obi_l3_k5: +0.0592

## 6. News pipeline

- **News signals captured:** 3489

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 56400, Closed: 67800, Realized: $-62.6510
