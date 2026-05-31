# Polymarket Arena Daily Report — 2026-05-23

_Generated: 2026-05-23 03:01 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=105s, size=2KB
- orderbook_collector.log: age=17s, size=1KB
- oil_iran.log: age=13s, size=5KB
- always_no.log: age=4s, size=9KB
- whale_fade.log: age=29s, size=17KB
- arena_ticks.jsonl: age=0s, size=111238KB
- orderbook_snapshots.jsonl: age=17s, size=384808KB
- live_validator.json: age=105s, size=133KB
- arena_results.json: age=31s, size=885KB
- political_skeptic.json: age=28s, size=5KB
- council.json: age=7s, size=36KB
- theta_decay.json: age=16s, size=4KB
- whale_follower.json: age=116s, size=20KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=121s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 206 alive, 0 retired
- **Positions:** 44 open, 91 closed
- **Realized PnL:** $-0.2150 (actual $0.01 bets)
- **Aggregate equity:** $204925 / $206000 starting = **-0.52%**
- **Exit reasons:** {'htr': 24, 'sl': 14, 'tp': 53}
- **Win3 skips:** 0
- **Alive families:** {'BO': 37, 'RS': 81, 'BB': 32, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 4, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 400 active, 723 profitable
- **Family distribution:** {'rsi': 39, 'mean_re': 174, 'forest_': 960, 'bolling': 37, 'zscore': 34, 'ensembl': 70, 'breakou': 19, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1248 | 647/254 | 72% |
| `S199|rsi|p7|e30.00|sloff|tp10%|free|b` | $1248 | 661/260 | 72% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1247 | 666/263 | 72% |
| `S202|rsi|p7|e25.00|sloff|tp10%|free|b` | $1245 | 639/252 | 72% |
| `S305|mean_re|p5|e0.01|sl-25%|tp10%|free|s` | $1239 | 286/67 | 81% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $1229 | 482/121 | 80% |
| `S207|rsi|p7|e20.00|sl-25%|tp10%|free|b` | $1227 | 624/248 | 72% |
| `S203|rsi|p7|e35.00|sl-10%|tp5%|free|b` | $1221 | 740/285 | 72% |
| `S200|rsi|p7|e25.00|sl-10%|tp5%|free|b` | $1213 | 663/265 | 71% |
| `S197|rsi|p7|e30.00|sl-10%|tp5%|free|b` | $1208 | 683/274 | 71% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=9, W/L=5/0 (WR 100%), realized=$+0.0072, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/5 (WR 82%), pnl=$+0.2361
- v2_with_fees: W/L=610/913 (WR 40%), pnl=$-1.1442
- v3_aggressive: W/L=24/7 (WR 77%), pnl=$+0.2205
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/14 (WR 75%), pnl=$+0.3419

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-22T22:56:25.816528+00:00 (4.1h ago)
- **Pairs:** 980,742 from 1245 markets
- **Correlations:**
  - mu_dev_k1: +0.0519
  - mu_dev_k10: +0.0589
  - mu_dev_k2: +0.0586
  - mu_dev_k5: +0.0613
  - obi_l1_k1: +0.0781
  - obi_l1_k10: +0.0945
  - obi_l1_k2: +0.0839
  - obi_l1_k5: +0.0906
  - obi_l3_k1: +0.0518
  - obi_l3_k10: +0.0602
  - obi_l3_k2: +0.0540
  - obi_l3_k5: +0.0575

## 6. News pipeline

- **News signals captured:** 6311

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 37630, Closed: 15230, Realized: $-41.2875
