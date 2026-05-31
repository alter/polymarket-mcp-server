# Polymarket Arena Daily Report — 2026-05-14

_Generated: 2026-05-14 01:04 UTC_

## 1. System health

- **stalled**: True, alerts: ['maker.json not updated in 1166s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=1164s, size=5KB
- orderbook_collector.log: age=10s, size=0KB
- oil_iran.log: age=10s, size=2KB
- always_no.log: age=1156s, size=5KB
- whale_fade.log: age=6s, size=9KB
- arena_ticks.jsonl: age=3s, size=110113KB
- orderbook_snapshots.jsonl: age=10s, size=380695KB
- live_validator.json: age=1164s, size=980KB
- arena_results.json: age=3s, size=886KB
- political_skeptic.json: age=19s, size=5KB
- council.json: age=21s, size=49KB
- theta_decay.json: age=1s, size=5KB
- whale_follower.json: age=1146s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=1166s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 82 alive, 124 retired
- **Positions:** 1858 open, 17035 closed
- **Realized PnL:** $-6.8712 (actual $0.01 bets)
- **Aggregate equity:** $86828 / $82000 starting = **+5.89%**
- **Exit reasons:** {'tp': 8750, 'sl': 5239, 'htr': 3046}
- **Win3 skips:** 4945
- **Alive families:** {'BO': 11, 'BB': 16, 'RS': 5, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BO_p50_follow|plt30|sany|fany` | 82 | 76% | +31.9% | +19.5% | $2307 |
| `BB_p50_sd25_fade|plt30|sany|fany` | 63 | 71% | +30.5% | +45.4% | $1961 |
| `BB_p50_sd25_fade|plt70|swide|fany` | 58 | 74% | +25.2% | +63.6% | $1730 |
| `BO_p100_follow|plt30|sany|fany` | 54 | 78% | +26.9% | +28.1% | $1726 |
| `BB_p50_sd25_fade|plt50|swide|fany` | 54 | 74% | +22.4% | +64.0% | $1604 |
| `BO_p100_follow|plt30|swide|fany` | 42 | 79% | +24.8% | +33.6% | $1521 |
| `BO_p100_follow|plt50|swide|fany` | 55 | 78% | +16.9% | +33.3% | $1464 |
| `BO_p100_follow|pany|swide|fany` | 58 | 76% | +11.2% | +33.1% | $1324 |
| `BO_p100_follow|plt70|swide|fany` | 58 | 76% | +11.2% | +33.2% | $1324 |
| `BB_p20_sd25_fade|plt30|sany|fany` | 30 | 83% | +5.9% | +35.7% | $1089 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 401 active, 729 profitable
- **Family distribution:** {'rsi': 39, 'forest_': 960, 'mean_re': 174, 'bolling': 37, 'zscore': 34, 'ensembl': 70, 'breakou': 19, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1226 | 631/252 | 71% |
| `S199|rsi|p7|e30.00|sloff|tp10%|free|b` | $1225 | 645/258 | 71% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1225 | 650/261 | 71% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $1224 | 476/120 | 80% |
| `S202|rsi|p7|e25.00|sloff|tp10%|free|b` | $1223 | 623/250 | 71% |
| `S203|rsi|p7|e35.00|sl-10%|tp5%|free|b` | $1208 | 720/281 | 72% |
| `S207|rsi|p7|e20.00|sl-25%|tp10%|free|b` | $1205 | 608/246 | 71% |
| `S200|rsi|p7|e25.00|sl-10%|tp5%|free|b` | $1197 | 643/261 | 71% |
| `S197|rsi|p7|e30.00|sl-10%|tp5%|free|b` | $1192 | 663/270 | 71% |
| `S206|rsi|p7|e20.00|sl-10%|tp5%|free|b` | $1170 | 621/255 | 71% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/2 (WR 92%), pnl=$+0.2661
- v2_with_fees: W/L=290/382 (WR 43%), pnl=$-0.1441
- v3_aggressive: W/L=23/4 (WR 85%), pnl=$+0.2452
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/11 (WR 80%), pnl=$+0.3719

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-13T21:01:11.717253+00:00 (3.1h ago)
- **Pairs:** 971,674 from 957 markets
- **Correlations:**
  - mu_dev_k1: +0.0517
  - mu_dev_k10: +0.0588
  - mu_dev_k2: +0.0586
  - mu_dev_k5: +0.0607
  - obi_l1_k1: +0.0781
  - obi_l1_k10: +0.0950
  - obi_l1_k2: +0.0839
  - obi_l1_k5: +0.0906
  - obi_l3_k1: +0.0519
  - obi_l3_k10: +0.0606
  - obi_l3_k2: +0.0542
  - obi_l3_k5: +0.0580

## 6. News pipeline

- **News signals captured:** 5472

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 90400, Closed: 469020, Realized: $+193.4195
