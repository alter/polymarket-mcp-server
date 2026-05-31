# Polymarket Arena Daily Report — 2026-05-04

_Generated: 2026-05-04 01:55 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=379s, size=33KB
- orderbook_collector.log: age=9s, size=24KB
- oil_iran.log: age=12s, size=68KB
- always_no.log: age=439s, size=18KB
- whale_fade.log: age=214s, size=52KB
- arena_ticks.jsonl: age=91s, size=76675KB
- orderbook_snapshots.jsonl: age=9s, size=172665KB
- live_validator.json: age=78s, size=1288KB
- arena_results.json: age=550s, size=863KB
- political_skeptic.json: age=147s, size=5KB
- council.json: age=461s, size=35KB
- theta_decay.json: age=206s, size=2KB
- whale_follower.json: age=1750s, size=18KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=49s, size=2KB

## 2. Live Validator

- **Variants:** 206 total, 145 alive, 61 retired
- **Positions:** 2668 open, 20011 closed
- **Realized PnL:** $+3.0526 (actual $0.01 bets)
- **Aggregate equity:** $169920 / $145000 starting = **+17.19%**
- **Exit reasons:** {'sl': 6341, 'tp': 10769, 'htr': 2901}
- **Win3 skips:** 6813
- **Alive families:** {'RS': 57, 'BO': 11, 'BB': 21, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 4, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `RS_p21_t70_follow|plt70|sany|fany` | 219 | 65% | +18.5% | +12.1% | $3028 |
| `RS_p21_t70_follow|pany|sany|fany` | 266 | 65% | +13.0% | +10.7% | $2732 |
| `RS_p21_t75_follow|plt70|sany|fany` | 174 | 63% | +19.4% | +13.2% | $2688 |
| `RS_p14_t80_follow|plt70|sany|fany` | 179 | 63% | +18.0% | +12.9% | $2610 |
| `RS_p21_t75_follow|pany|sany|fany` | 204 | 63% | +15.5% | +11.8% | $2582 |
| `RS_p21_t65_follow|plt70|sany|fany` | 299 | 64% | +10.2% | +9.7% | $2521 |
| `RS_p21_t80_follow|pgt30|sany|fany` | 111 | 63% | +27.1% | +23.1% | $2502 |
| `RS_p21_t75_follow|pgt30|sany|fany` | 142 | 63% | +19.9% | +20.4% | $2413 |
| `RS_p21_t80_follow|pany|sany|fany` | 157 | 60% | +17.7% | +14.3% | $2387 |
| `RS_p21_t80_follow|plt70|sany|fany` | 137 | 58% | +20.0% | +15.8% | $2370 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 1352 active, 1006 profitable
- **Family distribution:** {'mean_re': 174, 'wavelet': 68, 'ensembl': 70, 'bolling': 37, 'zscore': 34, 'forest_': 960, 'hybrid_': 92, 'rsi': 39, 'breakou': 19, 'macd': 18, 'momentu': 24}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S311|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $2656 | 431/101 | 81% |
| `S310|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $2022 | 448/105 | 81% |
| `S332|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1738 | 603/131 | 82% |
| `S335|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1734 | 542/112 | 83% |
| `S334|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1702 | 548/119 | 82% |
| `S120|mean_re|p5|e0.01|sloff|tp10%|free|b` | $1691 | 607/123 | 83% |
| `S1003|wavelet|p3|e0.01|sl-20%|tp10%|free|b` | $1690 | 554/125 | 82% |
| `S123|mean_re|p5|e0.01|sloff|tp5%|free|b` | $1688 | 611/123 | 83% |
| `S331|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1686 | 609/139 | 81% |
| `S298|mean_re|p10|e0.01|sloff|tp10%|free|b` | $1666 | 487/95 | 84% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=2, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=21/1 (WR 95%), pnl=$+0.2573
- v2_with_fees: W/L=203/220 (WR 48%), pnl=$+0.6316
- v3_aggressive: W/L=21/3 (WR 88%), pnl=$+0.2364
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=39/8 (WR 83%), pnl=$+0.3658

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-04T01:26:26.047671+00:00 (0.5h ago)
- **Pairs:** 439,566 from 409 markets
- **Correlations:**
  - mu_dev_k1: +0.0459
  - mu_dev_k10: +0.0606
  - mu_dev_k2: +0.0560
  - mu_dev_k5: +0.0580
  - obi_l1_k1: +0.0744
  - obi_l1_k10: +0.0917
  - obi_l1_k2: +0.0808
  - obi_l1_k5: +0.0872
  - obi_l3_k1: +0.0439
  - obi_l3_k10: +0.0544
  - obi_l3_k2: +0.0468
  - obi_l3_k5: +0.0517

## 6. News pipeline

- **News signals captured:** 172

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 51290, Closed: 72830, Realized: $-18.5710
