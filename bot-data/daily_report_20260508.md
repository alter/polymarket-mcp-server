# Polymarket Arena Daily Report — 2026-05-08

_Generated: 2026-05-08 00:34 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=165s, size=38KB
- orderbook_collector.log: age=76s, size=23KB
- oil_iran.log: age=84s, size=53KB
- always_no.log: age=269s, size=19KB
- whale_fade.log: age=130s, size=16KB
- arena_ticks.jsonl: age=319s, size=86316KB
- orderbook_snapshots.jsonl: age=76s, size=272813KB
- live_validator.json: age=71s, size=896KB
- arena_results.json: age=97s, size=859KB
- political_skeptic.json: age=522s, size=5KB
- council.json: age=574s, size=48KB
- theta_decay.json: age=59s, size=3KB
- whale_follower.json: age=1494s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=49s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 206 alive, 0 retired
- **Positions:** 1845 open, 1869 closed
- **Realized PnL:** $-0.2272 (actual $0.01 bets)
- **Aggregate equity:** $204864 / $206000 starting = **-0.55%**
- **Exit reasons:** {'tp': 1301, 'sl': 467, 'htr': 101}
- **Win3 skips:** 258
- **Alive families:** {'BO': 37, 'RS': 81, 'BB': 32, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 4, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BO_p10_fade|plt70|sany|fany` | 36 | 83% | +5.4% | +7.6% | $1098 |
| `BO_p10_fade|pany|sany|fany` | 40 | 80% | +4.4% | +4.0% | $1088 |
| `RS_p7_t80_follow|pany|stight|fany` | 19 | 89% | +6.8% | +15.6% | $1065 |
| `RS_p7_t75_follow|pany|stight|fany` | 22 | 86% | +5.9% | +14.1% | $1065 |
| `RS_p7_t80_follow|plt70|stight|fany` | 15 | 93% | +8.0% | +23.9% | $1060 |
| `BB_p10_sd15_follow|plt70|stight|fany` | 15 | 93% | +8.0% | +31.5% | $1060 |
| `RS_p7_t65_follow|plt70|stight|fany` | 17 | 88% | +6.7% | +19.7% | $1057 |
| `RS_p7_t65_follow|pany|stight|fany` | 26 | 81% | +4.4% | +11.7% | $1057 |
| `BO_p10_fade|pgt30|sany|fany` | 23 | 83% | +4.8% | +17.1% | $1055 |
| `RS_p7_t75_follow|plt70|stight|fany` | 16 | 88% | +6.2% | +21.9% | $1050 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 1212 active, 375 profitable
- **Family distribution:** {'breakou': 19, 'forest_': 960, 'momentu': 24, 'ensembl': 70, 'rsi': 39, 'zscore': 34, 'mean_re': 174, 'bolling': 37, 'macd': 18, 'wavelet': 68, 'hybrid_': 92}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S283|breakou|p10|e1.00|sl-15%|tp20%|all|b` | $1100 | 41/6 | 87% |
| `S289|breakou|p20|e1.00|sl-15%|tp20%|all|b` | $1072 | 37/3 | 92% |
| `S281|breakou|p10|e1.00|sl-10%|tp15%|all|b` | $1069 | 44/9 | 83% |
| `S1048|forest_|p39|e0.69|sl-25%|tp5%|free|b` | $1065 | 18/1 | 95% |
| `S287|breakou|p20|e1.00|sl-10%|tp15%|all|b` | $1065 | 40/5 | 89% |
| `S636|forest_|p45|e0.52|sl-25%|tp10%|free|b` | $1065 | 17/1 | 94% |
| `S1526|forest_|p43|e0.51|sl-25%|tp5%|free|b` | $1062 | 18/0 | 100% |
| `S158|momentu|p20|e0.03|sl-20%|tp15%|free|b` | $1053 | 23/36 | 39% |
| `S1335|forest_|p48|e0.60|sloff|tp20%|free|b` | $1052 | 8/0 | 100% |
| `S1049|forest_|p6|e0.52|sl-20%|tp5%|free|b` | $1052 | 21/6 | 78% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=2, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=7, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=22/1 (WR 96%), pnl=$+0.2663
- v2_with_fees: W/L=13/19 (WR 41%), pnl=$+0.0278
- v3_aggressive: W/L=22/3 (WR 88%), pnl=$+0.2454
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=42/9 (WR 82%), pnl=$+0.3821

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-08T00:23:11.743486+00:00 (0.2h ago)
- **Pairs:** 695,952 from 631 markets
- **Correlations:**
  - mu_dev_k1: +0.0491
  - mu_dev_k10: +0.0628
  - mu_dev_k2: +0.0584
  - mu_dev_k5: +0.0638
  - obi_l1_k1: +0.0769
  - obi_l1_k10: +0.0989
  - obi_l1_k2: +0.0847
  - obi_l1_k5: +0.0944
  - obi_l3_k1: +0.0490
  - obi_l3_k10: +0.0624
  - obi_l3_k2: +0.0529
  - obi_l3_k5: +0.0595

## 6. News pipeline

- **News signals captured:** 2354

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 51110, Closed: 17060, Realized: $-22.7205
