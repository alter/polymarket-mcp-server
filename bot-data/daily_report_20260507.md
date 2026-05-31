# Polymarket Arena Daily Report — 2026-05-07

_Generated: 2026-05-07 03:07 UTC_

## 1. System health

- **stalled**: True, alerts: ['arena_ticks.jsonl not updated in 5319s', 'orderbook_snapshots.jsonl not updated in 1699s', 'arena_results.json not updated in 8971s', 'council.json not updated in 1686s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=1672s, size=67KB
- orderbook_collector.log: age=1670s, size=46KB
- oil_iran.log: age=14s, size=111KB
- always_no.log: age=1691s, size=37KB
- whale_fade.log: age=19s, size=102KB
- arena_ticks.jsonl: age=5319s, size=84934KB
- orderbook_snapshots.jsonl: age=1699s, size=257318KB
- live_validator.json: age=6s, size=1220KB
- arena_results.json: age=8971s, size=870KB
- political_skeptic.json: age=24s, size=5KB
- council.json: age=1686s, size=42KB
- theta_decay.json: age=16s, size=3KB
- whale_follower.json: age=1662s, size=18KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=20s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 119 alive, 87 retired
- **Positions:** 2478 open, 28121 closed
- **Realized PnL:** $+3.2908 (actual $0.01 bets)
- **Aggregate equity:** $147358 / $119000 starting = **+23.83%**
- **Exit reasons:** {'sl': 8862, 'tp': 14748, 'htr': 4511}
- **Win3 skips:** 8944
- **Alive families:** {'RS': 44, 'BO': 8, 'BB': 17, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `RS_p14_t80_follow|plt70|sany|fany` | 316 | 61% | +18.1% | +12.9% | $3860 |
| `RS_p14_t70_follow|pany|sany|fany` | 606 | 65% | +8.6% | +7.5% | $3594 |
| `RS_p21_t70_follow|plt70|sany|fany` | 391 | 62% | +12.2% | +12.1% | $3392 |
| `RS_p14_t70_follow|plt70|sany|fany` | 506 | 63% | +8.6% | +9.2% | $3184 |
| `RS_p14_t75_follow|plt70|sany|fany` | 413 | 61% | +10.0% | +10.6% | $3070 |
| `RS_p21_t70_follow|pany|sany|fany` | 461 | 64% | +8.8% | +10.7% | $3017 |
| `RS_p21_t75_follow|pany|sany|fany` | 353 | 61% | +11.3% | +11.8% | $3000 |
| `RS_p21_t65_follow|plt70|sany|fany` | 497 | 63% | +7.2% | +9.7% | $2799 |
| `RS_p21_t75_follow|plt70|sany|fany` | 305 | 60% | +10.6% | +13.2% | $2624 |
| `RS_p21_t80_follow|pany|sany|fany` | 276 | 58% | +11.6% | +14.3% | $2596 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 1228 active, 1000 profitable
- **Family distribution:** {'mean_re': 174, 'wavelet': 68, 'ensembl': 70, 'zscore': 34, 'forest_': 960, 'bolling': 37, 'rsi': 39, 'breakou': 19, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S311|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $3433 | 802/233 | 77% |
| `S310|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $2540 | 819/237 | 78% |
| `S123|mean_re|p5|e0.01|sloff|tp5%|free|b` | $2031 | 1130/283 | 80% |
| `S120|mean_re|p5|e0.01|sloff|tp10%|free|b` | $2019 | 1124/283 | 80% |
| `S332|wavelet|p3|e0.01|sloff|tp10%|free|b` | $2019 | 1086/283 | 79% |
| `S323|wavelet|p2|e0.01|sloff|tp10%|free|b` | $1982 | 1297/360 | 78% |
| `S331|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1976 | 1106/295 | 79% |
| `S335|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1973 | 952/250 | 79% |
| `S334|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1950 | 973/261 | 79% |
| `S122|mean_re|p5|e0.01|sl-25%|tp5%|free|b` | $1941 | 1149/291 | 80% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=2, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=7, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=22/1 (WR 96%), pnl=$+0.2663
- v2_with_fees: W/L=335/373 (WR 47%), pnl=$+0.9045
- v3_aggressive: W/L=22/3 (WR 88%), pnl=$+0.2454
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=42/9 (WR 82%), pnl=$+0.3821

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-06T21:37:30.588547+00:00 (5.5h ago)
- **Pairs:** 653,223 from 571 markets
- **Correlations:**
  - mu_dev_k1: +0.0473
  - mu_dev_k10: +0.0630
  - mu_dev_k2: +0.0566
  - mu_dev_k5: +0.0625
  - obi_l1_k1: +0.0756
  - obi_l1_k10: +0.0977
  - obi_l1_k2: +0.0833
  - obi_l1_k5: +0.0932
  - obi_l3_k1: +0.0479
  - obi_l3_k10: +0.0604
  - obi_l3_k2: +0.0514
  - obi_l3_k5: +0.0579

## 6. News pipeline

- **News signals captured:** 1861

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 64470, Closed: 105830, Realized: $-3.7865
