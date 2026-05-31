# Polymarket Arena Daily Report — 2026-05-19

_Generated: 2026-05-19 03:09 UTC_

## 1. System health

- **stalled**: True, alerts: ['orderbook_snapshots.jsonl not updated in 553s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=563s, size=2KB
- orderbook_collector.log: age=-2301s, size=2KB
- oil_iran.log: age=-2307s, size=7KB
- always_no.log: age=533s, size=12KB
- whale_fade.log: age=147s, size=26KB
- arena_ticks.jsonl: age=540s, size=110920KB
- orderbook_snapshots.jsonl: age=553s, size=383561KB
- live_validator.json: age=124s, size=142KB
- arena_results.json: age=539s, size=885KB
- political_skeptic.json: age=542s, size=5KB
- council.json: age=134s, size=37KB
- theta_decay.json: age=27s, size=4KB
- whale_follower.json: age=543s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=58s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 206 alive, 0 retired
- **Positions:** 78 open, 0 closed
- **Realized PnL:** $+0.0000 (actual $0.01 bets)
- **Aggregate equity:** $206000 / $206000 starting = **+0.00%**
- **Exit reasons:** {}
- **Win3 skips:** 0
- **Alive families:** {'BO': 37, 'RS': 81, 'BB': 32, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 4, 'MV4': 2}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 400 active, 723 profitable
- **Family distribution:** {'rsi': 39, 'forest_': 960, 'mean_re': 174, 'bolling': 37, 'zscore': 34, 'ensembl': 70, 'breakou': 19, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $1229 | 482/121 | 80% |
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1228 | 640/254 | 72% |
| `S199|rsi|p7|e30.00|sloff|tp10%|free|b` | $1228 | 654/260 | 72% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1227 | 659/263 | 71% |
| `S202|rsi|p7|e25.00|sloff|tp10%|free|b` | $1225 | 632/252 | 71% |
| `S207|rsi|p7|e20.00|sl-25%|tp10%|free|b` | $1207 | 617/248 | 71% |
| `S203|rsi|p7|e35.00|sl-10%|tp5%|free|b` | $1201 | 729/284 | 72% |
| `S200|rsi|p7|e25.00|sl-10%|tp5%|free|b` | $1193 | 653/264 | 71% |
| `S197|rsi|p7|e30.00|sl-10%|tp5%|free|b` | $1188 | 673/273 | 71% |
| `S1236|forest_|p15|e0.59|sloff|tp10%|free|b` | $1169 | 24/0 | 100% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=9, W/L=3/0 (WR 100%), realized=$+0.0034, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/5 (WR 82%), pnl=$+0.2361
- v2_with_fees: W/L=474/689 (WR 41%), pnl=$-0.8807
- v3_aggressive: W/L=24/7 (WR 77%), pnl=$+0.2205
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/14 (WR 75%), pnl=$+0.3419

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-18T23:54:23.348024+00:00 (3.2h ago)
- **Pairs:** 978,171 from 1129 markets
- **Correlations:**
  - mu_dev_k1: +0.0518
  - mu_dev_k10: +0.0590
  - mu_dev_k2: +0.0586
  - mu_dev_k5: +0.0613
  - obi_l1_k1: +0.0780
  - obi_l1_k10: +0.0946
  - obi_l1_k2: +0.0838
  - obi_l1_k5: +0.0905
  - obi_l3_k1: +0.0517
  - obi_l3_k10: +0.0602
  - obi_l3_k2: +0.0539
  - obi_l3_k5: +0.0575

## 6. News pipeline

- **News signals captured:** 5899

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 77810, Closed: 532880, Realized: $+106.2145
