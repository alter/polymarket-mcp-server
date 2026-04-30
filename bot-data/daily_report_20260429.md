# Polymarket Arena Daily Report — 2026-04-29

_Generated: 2026-04-29 19:12 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=467s, size=6KB
- orderbook_collector.log: age=25s, size=3KB
- oil_iran.log: age=7s, size=10KB
- always_no.log: age=425s, size=3KB
- whale_fade.log: age=549s, size=1KB
- arena_ticks.jsonl: age=63s, size=54401KB
- orderbook_snapshots.jsonl: age=25s, size=66858KB
- live_validator.json: age=135s, size=1233KB
- arena_results.json: age=429s, size=540KB
- political_skeptic.json: age=455s, size=5KB

## 2. Live Validator

- **Variants:** 200 total, 156 alive, 44 retired
- **Positions:** 2651 open, 17695 closed
- **Realized PnL:** $-0.4472 (actual $0.01 bets)
- **Aggregate equity:** $173112 / $156000 starting = **+10.97%**
- **Exit reasons:** {'tp': 10335, 'sl': 5448, 'htr': 1680}
- **Win3 skips:** 4687
- **Alive families:** {'RS': 79, 'BO': 11, 'BB': 16, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `RS_p7_t75_follow|pany|sany|fany` | 240 | 72% | +7.4% | +6.1% | $1888 |
| `RS_p7_t70_follow|plt70|sany|fany` | 228 | 73% | +7.0% | +7.3% | $1801 |
| `RS_p7_t70_follow|pany|sany|fany` | 277 | 71% | +5.6% | +5.3% | $1778 |
| `RS_p7_t75_follow|plt70|sany|fany` | 193 | 73% | +7.6% | +8.2% | $1736 |
| `RS_p14_t65_follow|pany|sany|fany` | 245 | 71% | +5.8% | +5.1% | $1712 |
| `RS_p21_t65_follow|plt70|sany|fany` | 156 | 74% | +9.0% | +9.7% | $1698 |
| `RS_p14_t65_follow|pgt30|sany|fany` | 177 | 71% | +7.3% | +14.7% | $1644 |
| `RS_p14_t65_follow|plt70|sany|fany` | 200 | 71% | +6.3% | +6.8% | $1628 |
| `RS_p14_t70_follow|pany|sany|fany` | 208 | 71% | +6.0% | +7.5% | $1623 |
| `RS_p14_t70_follow|plt70|sany|fany` | 163 | 72% | +7.4% | +9.2% | $1605 |

## 3. Arena (multi_strategy)

- **Total:** 1023 strategies, 1017 active, 313 profitable
- **Family distribution:** {'wavelet': 64, 'mean_re': 170, 'rsi': 37, 'forest_': 460, 'bolling': 36, 'ensembl': 70, 'zscore': 34, 'hybrid_': 92, 'breakou': 18, 'macd': 18, 'momentu': 24}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S1003|wavelet|p3|e0.01|sl-20%|tp10%|free|b` | $1226 | 107/27 | 80% |
| `S331|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1218 | 130/32 | 80% |
| `S334|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1216 | 106/24 | 82% |
| `S1002|wavelet|p3|e0.01|sl-20%|tp5%|free|b` | $1212 | 108/27 | 80% |
| `S129|mean_re|p10|e0.01|sl-25%|tp10%|free|b` | $1211 | 84/16 | 84% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1210 | 183/40 | 82% |
| `S132|mean_re|p10|e0.01|sl-25%|tp5%|free|b` | $1186 | 85/17 | 83% |
| `S337|wavelet|p3|e0.02|sl-25%|tp10%|free|b` | $1184 | 67/19 | 78% |
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1179 | 177/38 | 82% |
| `S297|mean_re|p10|e0.01|sl-25%|tp10%|free|b` | $1174 | 100/19 | 84% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=1, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=2, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=2/0 (WR 100%), pnl=$+0.0313
- v2_with_fees: W/L=221/264 (WR 46%), pnl=$+0.2664
- v3_aggressive: W/L=2/2 (WR 50%), pnl=$+0.0109
- v4_conservative: W/L=0/0 (WR 0%), pnl=$+0.0000
- v5_all_cats: W/L=2/0 (WR 100%), pnl=$+0.0309

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-04-29T18:24:02.141563+00:00 (0.8h ago)
- **Pairs:** 167,326 from 136 markets
- **Correlations:**
  - mu_dev_k1: +0.0576
  - mu_dev_k10: +0.0740
  - mu_dev_k2: +0.0781
  - mu_dev_k5: +0.0734
  - obi_l1_k1: +0.0808
  - obi_l1_k10: +0.1022
  - obi_l1_k2: +0.0900
  - obi_l1_k5: +0.0980
  - obi_l3_k1: +0.0457
  - obi_l3_k10: +0.0533
  - obi_l3_k2: +0.0506
  - obi_l3_k5: +0.0537

## 6. News pipeline

- **News signals captured:** 45

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 16340, Closed: 0, Realized: $+0.0000
