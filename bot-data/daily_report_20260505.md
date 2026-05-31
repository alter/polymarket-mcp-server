# Polymarket Arena Daily Report — 2026-05-05

_Generated: 2026-05-05 03:02 UTC_

## 1. System health

- **stalled**: True, alerts: ['arena_ticks.jsonl not updated in 3633s', 'live_validator.json not updated in 3645s', 'arena_results.json not updated in 3629s', 'maker.json not updated in 3624s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=3645s, size=6KB
- orderbook_collector.log: age=10s, size=1KB
- oil_iran.log: age=3s, size=5KB
- always_no.log: age=3630s, size=3KB
- whale_fade.log: age=3s, size=7KB
- arena_ticks.jsonl: age=3633s, size=81034KB
- orderbook_snapshots.jsonl: age=10s, size=205763KB
- live_validator.json: age=3645s, size=1334KB
- arena_results.json: age=3629s, size=867KB
- political_skeptic.json: age=11s, size=5KB
- council.json: age=4s, size=43KB
- theta_decay.json: age=6s, size=3KB
- whale_follower.json: age=11s, size=18KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=3624s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 143 alive, 63 retired
- **Positions:** 2788 open, 23124 closed
- **Realized PnL:** $+3.5265 (actual $0.01 bets)
- **Aggregate equity:** $170483 / $143000 starting = **+19.22%**
- **Exit reasons:** {'sl': 7391, 'tp': 12444, 'htr': 3289}
- **Win3 skips:** 7625
- **Alive families:** {'RS': 57, 'BO': 11, 'BB': 20, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 3, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `RS_p21_t70_follow|plt70|sany|fany` | 283 | 65% | +16.7% | +12.1% | $3360 |
| `RS_p21_t65_follow|plt70|sany|fany` | 371 | 65% | +10.7% | +9.7% | $2976 |
| `RS_p21_t70_follow|pany|sany|fany` | 337 | 66% | +11.0% | +10.7% | $2851 |
| `RS_p14_t80_follow|plt70|sany|fany` | 227 | 62% | +14.4% | +12.9% | $2634 |
| `RS_p21_t80_follow|pgt30|sany|fany` | 140 | 62% | +23.2% | +23.1% | $2628 |
| `RS_p21_t75_follow|pany|sany|fany` | 256 | 63% | +11.8% | +11.8% | $2516 |
| `RS_p21_t75_follow|pgt30|sany|fany` | 182 | 64% | +15.1% | +20.4% | $2378 |
| `RS_p14_t70_follow|pany|sany|fany` | 449 | 65% | +6.1% | +7.5% | $2368 |
| `RS_p21_t75_follow|plt70|sany|fany` | 221 | 62% | +11.6% | +13.2% | $2285 |
| `RS_p14_t70_follow|plt70|sany|fany` | 370 | 64% | +6.6% | +9.2% | $2215 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 1298 active, 979 profitable
- **Family distribution:** {'mean_re': 174, 'wavelet': 68, 'ensembl': 70, 'bolling': 37, 'zscore': 34, 'forest_': 960, 'hybrid_': 92, 'rsi': 39, 'breakou': 19, 'macd': 18, 'momentu': 24}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S311|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $3186 | 616/160 | 79% |
| `S310|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $2375 | 633/164 | 79% |
| `S332|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1934 | 858/201 | 81% |
| `S335|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1906 | 757/175 | 81% |
| `S331|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1893 | 867/210 | 81% |
| `S334|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1884 | 767/183 | 81% |
| `S120|mean_re|p5|e0.01|sloff|tp10%|free|b` | $1879 | 873/192 | 82% |
| `S123|mean_re|p5|e0.01|sloff|tp5%|free|b` | $1875 | 877/192 | 82% |
| `S1003|wavelet|p3|e0.01|sl-20%|tp10%|free|b` | $1855 | 774/191 | 80% |
| `S297|mean_re|p10|e0.01|sl-25%|tp10%|free|b` | $1821 | 681/164 | 81% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=2, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=7, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=22/1 (WR 96%), pnl=$+0.2663
- v2_with_fees: W/L=249/263 (WR 49%), pnl=$+0.8252
- v3_aggressive: W/L=22/3 (WR 88%), pnl=$+0.2454
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=41/9 (WR 82%), pnl=$+0.3721

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-04T22:40:13.243060+00:00 (4.4h ago)
- **Pairs:** 524,136 from 456 markets
- **Correlations:**
  - mu_dev_k1: +0.0467
  - mu_dev_k10: +0.0619
  - mu_dev_k2: +0.0564
  - mu_dev_k5: +0.0607
  - obi_l1_k1: +0.0744
  - obi_l1_k10: +0.0956
  - obi_l1_k2: +0.0815
  - obi_l1_k5: +0.0903
  - obi_l3_k1: +0.0449
  - obi_l3_k10: +0.0566
  - obi_l3_k2: +0.0482
  - obi_l3_k5: +0.0541

## 6. News pipeline

- **News signals captured:** 317

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 76820, Closed: 128660, Realized: $-14.6500
