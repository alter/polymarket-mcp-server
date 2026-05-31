# Polymarket Arena Daily Report — 2026-05-30

_Generated: 2026-05-30 00:44 UTC_

## 1. System health

- **stalled**: True, alerts: ['whale_fade_grid.json not updated in 1759s']
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=558s, size=7KB
- orderbook_collector.log: age=12s, size=4KB
- oil_iran.log: age=12s, size=13KB
- always_no.log: age=555s, size=4KB
- whale_fade.log: age=544s, size=10KB
- arena_ticks.jsonl: age=11s, size=167599KB
- orderbook_snapshots.jsonl: age=12s, size=223213KB
- live_validator.json: age=257s, size=1038KB
- arena_results.json: age=519s, size=1132KB
- political_skeptic.json: age=573s, size=5KB
- council.json: age=576s, size=57KB
- theta_decay.json: age=126s, size=2KB
- whale_follower.json: age=1787s, size=20KB
- whale_fade_grid.json: age=1759s, size=45881KB
- maker.json: age=52s, size=3KB

## 2. Live Validator

- **Variants:** 206 total, 74 alive, 132 retired
- **Positions:** 1951 open, 30505 closed
- **Realized PnL:** $-4.9268 (actual $0.01 bets)
- **Aggregate equity:** $74303 / $74000 starting = **+0.41%**
- **Exit reasons:** {'tp': 17555, 'sl': 9746, 'htr': 3204}
- **Win3 skips:** 9144
- **Alive families:** {'BO': 1, 'BB': 16, 'RS': 6, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 1}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `MO_p30_t20_fade|pgt70|sany|ffree_only` | 46 | 80% | +4.1% | +110.6% | $1095 |
| `ME_p100_t10_fade|pgt70|sany|ffree_only` | 55 | 78% | +3.5% | +108.6% | $1095 |
| `ME_p100_t20_fade|pgt70|stight|ffree_only` | 42 | 81% | +4.3% | +113.6% | $1090 |
| `ME_p50_t20_fade|pgt70|stight|ffree_only` | 43 | 79% | +3.7% | +138.6% | $1080 |
| `ME_p100_t20_fade|pgt70|sany|ffree_only` | 50 | 76% | +2.8% | +146.9% | $1070 |
| `BB_p20_sd25_fade|pany|swide|fany` | 139 | 70% | +1.0% | +46.1% | $1066 |
| `BB_p20_sd25_fade|plt70|swide|fany` | 138 | 70% | +0.9% | +46.5% | $1062 |
| `BB_p10_sd20_follow|plt70|stight|fany` | 55 | 73% | +2.1% | +35.8% | $1058 |
| `BB_p10_sd20_follow|pgt30|sany|fany` | 153 | 69% | +0.7% | +16.2% | $1056 |
| `ME_p5_t10_fade|pgt70|sany|ffree_only` | 29 | 79% | +3.8% | +149.0% | $1055 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 141 active, 687 profitable
- **Family distribution:** {'mean_re': 174, 'zscore': 34, 'rsi': 39, 'bolling': 37, 'forest_': 960, 'breakou': 19, 'ensembl': 70, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S306|mean_re|p10|e0.02|sl-25%|tp10%|free|s` | $3767 | 1577/417 | 79% |
| `S976|zscore|p50|e2.00|sl-20%|tp5%|free|b` | $3013 | 2342/609 | 79% |
| `S977|zscore|p50|e2.00|sl-20%|tp10%|free|b` | $2972 | 2272/618 | 79% |
| `S252|zscore|p50|e2.00|sl-10%|tp5%|free|b` | $2834 | 2443/727 | 77% |
| `S305|mean_re|p5|e0.01|sl-25%|tp10%|free|s` | $2504 | 2835/773 | 79% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $2460 | 789/261 | 75% |
| `S979|zscore|p50|e2.50|sl-20%|tp10%|free|b` | $2417 | 1781/491 | 78% |
| `S982|zscore|p20|e2.50|sl-20%|tp5%|free|b` | $2383 | 2826/906 | 76% |
| `S1000|bolling|p20|e2.50|sl-20%|tp5%|free|b` | $2383 | 2826/906 | 76% |
| `S983|zscore|p20|e2.50|sl-20%|tp10%|free|b` | $2380 | 2813/913 | 75% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=5, W/L=10/0 (WR 100%), realized=$+0.0144, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=29/9 (WR 76%), pnl=$+0.3776
- v2_with_fees: W/L=946/1344 (WR 41%), pnl=$-0.5455
- v3_aggressive: W/L=30/11 (WR 73%), pnl=$+0.3221
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=49/18 (WR 73%), pnl=$+0.4834

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-30T00:30:05.266521+00:00 (0.2h ago)
- **Pairs:** 571,902 from 423 markets
- **Correlations:**
  - mu_dev_k1: +0.0581
  - mu_dev_k10: +0.0553
  - mu_dev_k2: +0.0595
  - mu_dev_k5: +0.0583
  - obi_l1_k1: +0.0842
  - obi_l1_k10: +0.1031
  - obi_l1_k2: +0.0913
  - obi_l1_k5: +0.0984
  - obi_l3_k1: +0.0517
  - obi_l3_k10: +0.0695
  - obi_l3_k2: +0.0562
  - obi_l3_k5: +0.0635

## 6. News pipeline

- **News signals captured:** 14063

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 97040, Closed: 173980, Realized: $+16.0385
