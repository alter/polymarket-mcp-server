# Polymarket Arena Daily Report — 2026-05-06

_Generated: 2026-05-06 00:58 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=195s, size=60KB
- orderbook_collector.log: age=20s, size=41KB
- oil_iran.log: age=29s, size=99KB
- always_no.log: age=38s, size=31KB
- whale_fade.log: age=28s, size=36KB
- arena_ticks.jsonl: age=42s, size=82535KB
- orderbook_snapshots.jsonl: age=20s, size=220789KB
- live_validator.json: age=195s, size=1254KB
- arena_results.json: age=198s, size=868KB
- political_skeptic.json: age=37s, size=5KB
- council.json: age=44s, size=40KB
- theta_decay.json: age=39s, size=3KB
- whale_follower.json: age=201s, size=18KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=53s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 131 alive, 75 retired
- **Positions:** 2577 open, 25001 closed
- **Realized PnL:** $+4.3428 (actual $0.01 bets)
- **Aggregate equity:** $160242 / $131000 starting = **+22.32%**
- **Exit reasons:** {'sl': 7844, 'tp': 13291, 'htr': 3866}
- **Win3 skips:** 8081
- **Alive families:** {'RS': 50, 'BO': 8, 'BB': 20, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 1, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `RS_p21_t70_follow|plt70|sany|fany` | 315 | 63% | +19.4% | +12.1% | $4050 |
| `RS_p21_t65_follow|plt70|sany|fany` | 412 | 64% | +13.3% | +9.7% | $3742 |
| `RS_p21_t70_follow|pany|sany|fany` | 376 | 65% | +14.0% | +10.7% | $3630 |
| `RS_p21_t75_follow|pany|sany|fany` | 290 | 63% | +17.2% | +11.8% | $3493 |
| `RS_p14_t80_follow|plt70|sany|fany` | 257 | 61% | +19.1% | +12.9% | $3455 |
| `RS_p14_t70_follow|pany|sany|fany` | 502 | 65% | +9.4% | +7.5% | $3360 |
| `RS_p21_t75_follow|plt70|sany|fany` | 249 | 61% | +17.1% | +13.2% | $3132 |
| `RS_p14_t70_follow|plt70|sany|fany` | 416 | 63% | +10.2% | +9.2% | $3116 |
| `RS_p21_t80_follow|pany|sany|fany` | 226 | 59% | +18.6% | +14.3% | $3107 |
| `RS_p21_t80_follow|plt70|sany|fany` | 200 | 56% | +18.5% | +15.8% | $2852 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 1256 active, 957 profitable
- **Family distribution:** {'mean_re': 174, 'wavelet': 68, 'ensembl': 70, 'bolling': 37, 'zscore': 34, 'forest_': 960, 'breakou': 19, 'hybrid_': 92, 'rsi': 39, 'macd': 18, 'momentu': 24}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S311|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $3194 | 640/169 | 79% |
| `S310|mean_re|p5|e0.01|sl-25%|tp10%|free|b` | $2380 | 657/173 | 79% |
| `S332|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1968 | 898/209 | 81% |
| `S335|wavelet|p3|e0.01|sloff|tp10%|free|b` | $1935 | 789/183 | 81% |
| `S331|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1914 | 907/219 | 81% |
| `S120|mean_re|p5|e0.01|sloff|tp10%|free|b` | $1911 | 913/201 | 82% |
| `S123|mean_re|p5|e0.01|sloff|tp5%|free|b` | $1908 | 917/201 | 82% |
| `S334|wavelet|p3|e0.01|sl-25%|tp10%|free|b` | $1900 | 799/192 | 81% |
| `S1003|wavelet|p3|e0.01|sl-20%|tp10%|free|b` | $1871 | 806/200 | 80% |
| `S323|wavelet|p2|e0.01|sloff|tp10%|free|b` | $1848 | 1028/260 | 80% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=2, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=7, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=22/1 (WR 96%), pnl=$+0.2663
- v2_with_fees: W/L=283/310 (WR 48%), pnl=$+0.7923
- v3_aggressive: W/L=22/3 (WR 88%), pnl=$+0.2454
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=42/9 (WR 82%), pnl=$+0.3821

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-05T23:10:39.426050+00:00 (1.8h ago)
- **Pairs:** 561,378 from 511 markets
- **Correlations:**
  - mu_dev_k1: +0.0469
  - mu_dev_k10: +0.0613
  - mu_dev_k2: +0.0563
  - mu_dev_k5: +0.0607
  - obi_l1_k1: +0.0738
  - obi_l1_k10: +0.0941
  - obi_l1_k2: +0.0811
  - obi_l1_k5: +0.0900
  - obi_l3_k1: +0.0447
  - obi_l3_k10: +0.0555
  - obi_l3_k2: +0.0480
  - obi_l3_k5: +0.0539

## 6. News pipeline

- **News signals captured:** 582

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 32560, Closed: 32390, Realized: $-28.0465
