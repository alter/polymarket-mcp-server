# Polymarket Arena Daily Report — 2026-05-31

_Generated: 2026-05-31 03:09 UTC_

## 1. System health

- **stalled**: True, alerts: ['arena_ticks.jsonl not updated in 2972s', 'orderbook_snapshots.jsonl not updated in 2499s', 'maker.json not updated in 58979s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=26s, size=6KB
- orderbook_collector.log: age=24s, size=0KB
- oil_iran.log: age=16s, size=2KB
- always_no.log: age=85s, size=3KB
- whale_fade.log: age=90s, size=9KB
- arena_ticks.jsonl: age=2972s, size=173642KB
- orderbook_snapshots.jsonl: age=2499s, size=232058KB
- live_validator.json: age=26s, size=929KB
- arena_results.json: age=77s, size=1146KB
- political_skeptic.json: age=17s, size=5KB
- council.json: age=11s, size=52KB
- theta_decay.json: age=117s, size=2KB
- whale_follower.json: age=90s, size=20KB
- whale_fade_grid.json: age=88s, size=39300KB
- maker.json: age=58979s, size=5KB

## 2. Live Validator

- **Variants:** 206 total, 73 alive, 133 retired
- **Positions:** 1662 open, 30943 closed
- **Realized PnL:** $-5.2387 (actual $0.01 bets)
- **Aggregate equity:** $73343 / $73000 starting = **+0.47%**
- **Exit reasons:** {'tp': 17769, 'sl': 9927, 'htr': 3247}
- **Win3 skips:** 9170
- **Alive families:** {'BO': 1, 'BB': 16, 'RS': 5, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 1}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `MO_p30_t20_fade|pgt70|sany|ffree_only` | 46 | 80% | +4.1% | +110.6% | $1095 |
| `ME_p100_t10_fade|pgt70|sany|ffree_only` | 55 | 78% | +3.5% | +108.6% | $1095 |
| `ME_p100_t20_fade|pgt70|stight|ffree_only` | 42 | 81% | +4.3% | +113.6% | $1090 |
| `BB_p20_sd25_fade|pany|swide|fany` | 144 | 70% | +1.1% | +46.1% | $1081 |
| `ME_p50_t20_fade|pgt70|stight|ffree_only` | 43 | 79% | +3.7% | +138.6% | $1080 |
| `BB_p20_sd25_fade|plt70|swide|fany` | 143 | 70% | +1.1% | +46.5% | $1076 |
| `BB_p10_sd20_follow|pgt30|sany|fany` | 159 | 70% | +0.9% | +16.2% | $1071 |
| `ME_p100_t20_fade|pgt70|sany|ffree_only` | 50 | 76% | +2.8% | +146.9% | $1070 |
| `BB_p20_sd20_follow|pgt30|sany|fany` | 625 | 67% | +0.2% | +17.7% | $1070 |
| `BB_p20_sd25_fade|plt30|swide|fany` | 102 | 75% | +1.3% | +48.8% | $1066 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 141 active, 687 profitable
- **Family distribution:** {'mean_re': 174, 'zscore': 34, 'rsi': 39, 'forest_': 960, 'bolling': 37, 'breakou': 19, 'ensembl': 70, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S306|mean_re|p10|e0.02|sl-25%|tp10%|free|s` | $3768 | 1583/428 | 79% |
| `S976|zscore|p50|e2.00|sl-20%|tp5%|free|b` | $2856 | 2343/669 | 78% |
| `S977|zscore|p50|e2.00|sl-20%|tp10%|free|b` | $2814 | 2273/678 | 77% |
| `S252|zscore|p50|e2.00|sl-10%|tp5%|free|b` | $2638 | 2445/796 | 75% |
| `S305|mean_re|p5|e0.01|sl-25%|tp10%|free|s` | $2473 | 2838/804 | 78% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $2460 | 789/261 | 75% |
| `S979|zscore|p50|e2.50|sl-20%|tp10%|free|b` | $2280 | 1782/544 | 77% |
| `S1236|forest_|p15|e0.59|sloff|tp10%|free|b` | $2277 | 187/6 | 97% |
| `S1386|forest_|p41|e0.64|sloff|tp5%|free|b` | $2259 | 1008/326 | 76% |
| `S257|zscore|p50|e2.50|sloff|tp10%|free|b` | $2256 | 1742/518 | 77% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=5, W/L=10/0 (WR 100%), realized=$+0.0144, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=29/9 (WR 76%), pnl=$+0.3776
- v2_with_fees: W/L=1019/1433 (WR 42%), pnl=$-0.4149
- v3_aggressive: W/L=30/11 (WR 73%), pnl=$+0.3221
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=49/18 (WR 73%), pnl=$+0.4834

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-30T23:52:07.474611+00:00 (3.3h ago)
- **Pairs:** 595,430 from 445 markets
- **Correlations:**
  - mu_dev_k1: +0.0580
  - mu_dev_k10: +0.0548
  - mu_dev_k2: +0.0590
  - mu_dev_k5: +0.0579
  - obi_l1_k1: +0.0830
  - obi_l1_k10: +0.1009
  - obi_l1_k2: +0.0890
  - obi_l1_k5: +0.0958
  - obi_l3_k1: +0.0489
  - obi_l3_k10: +0.0657
  - obi_l3_k2: +0.0524
  - obi_l3_k5: +0.0596

## 6. News pipeline

- **News signals captured:** 14292

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 80610, Closed: 276010, Realized: $+136.8840
