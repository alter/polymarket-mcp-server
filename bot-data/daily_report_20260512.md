# Polymarket Arena Daily Report — 2026-05-12

_Generated: 2026-05-12 00:30 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=100s, size=60KB
- orderbook_collector.log: age=16s, size=41KB
- oil_iran.log: age=24s, size=111KB
- always_no.log: age=101s, size=30KB
- whale_fade.log: age=101s, size=94KB
- arena_ticks.jsonl: age=39s, size=108212KB
- orderbook_snapshots.jsonl: age=16s, size=373552KB
- live_validator.json: age=100s, size=1071KB
- arena_results.json: age=106s, size=885KB
- political_skeptic.json: age=104s, size=5KB
- council.json: age=101s, size=49KB
- theta_decay.json: age=104s, size=5KB
- whale_follower.json: age=103s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=40s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 97 alive, 109 retired
- **Positions:** 2102 open, 16327 closed
- **Realized PnL:** $-6.0704 (actual $0.01 bets)
- **Aggregate equity:** $100984 / $97000 starting = **+4.11%**
- **Exit reasons:** {'tp': 8422, 'sl': 5076, 'htr': 2829}
- **Win3 skips:** 4803
- **Alive families:** {'BO': 11, 'BB': 22, 'RS': 14, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BO_p50_follow|plt30|sany|fany` | 66 | 76% | +36.0% | +19.5% | $2190 |
| `RS_p14_t80_follow|plt70|stight|fany` | 95 | 74% | +16.8% | +22.5% | $1797 |
| `BB_p50_sd25_fade|plt70|swide|fany` | 43 | 72% | +23.7% | +63.6% | $1510 |
| `BB_p50_sd25_fade|plt50|swide|fany` | 41 | 73% | +23.7% | +64.0% | $1486 |
| `RS_p14_t80_follow|pany|stight|fany` | 134 | 70% | +7.2% | +17.5% | $1480 |
| `BO_p100_follow|plt30|sany|fany` | 41 | 78% | +20.3% | +28.1% | $1416 |
| `RS_p14_t80_follow|pgt30|stight|fany` | 125 | 70% | +5.4% | +17.8% | $1336 |
| `BB_p50_sd25_fade|plt30|sany|fany` | 47 | 72% | +13.4% | +45.4% | $1314 |
| `RS_p14_t80_follow|pgt50|stight|fany` | 111 | 69% | +3.3% | +17.8% | $1182 |
| `BO_p100_follow|plt50|swide|fany` | 41 | 76% | +8.6% | +33.3% | $1176 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 403 active, 713 profitable
- **Family distribution:** {'rsi': 39, 'forest_': 960, 'mean_re': 174, 'bolling': 37, 'zscore': 34, 'ensembl': 70, 'breakou': 19, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1207 | 601/246 | 71% |
| `S199|rsi|p7|e30.00|sloff|tp10%|free|b` | $1206 | 615/252 | 71% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $1206 | 451/119 | 79% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1205 | 620/255 | 71% |
| `S202|rsi|p7|e25.00|sloff|tp10%|free|b` | $1204 | 593/244 | 71% |
| `S203|rsi|p7|e35.00|sl-10%|tp5%|free|b` | $1188 | 689/275 | 71% |
| `S207|rsi|p7|e20.00|sl-25%|tp10%|free|b` | $1185 | 578/240 | 71% |
| `S200|rsi|p7|e25.00|sl-10%|tp5%|free|b` | $1177 | 613/255 | 71% |
| `S197|rsi|p7|e30.00|sl-10%|tp5%|free|b` | $1173 | 633/264 | 71% |
| `S206|rsi|p7|e20.00|sl-10%|tp5%|free|b` | $1151 | 591/249 | 70% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=3, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=22/2 (WR 92%), pnl=$+0.2563
- v2_with_fees: W/L=214/273 (WR 44%), pnl=$+0.0436
- v3_aggressive: W/L=22/4 (WR 85%), pnl=$+0.2354
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=42/11 (WR 79%), pnl=$+0.3621

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-11T23:38:08.612093+00:00 (0.9h ago)
- **Pairs:** 953,836 from 888 markets
- **Correlations:**
  - mu_dev_k1: +0.0512
  - mu_dev_k10: +0.0594
  - mu_dev_k2: +0.0583
  - mu_dev_k5: +0.0609
  - obi_l1_k1: +0.0784
  - obi_l1_k10: +0.0953
  - obi_l1_k2: +0.0841
  - obi_l1_k5: +0.0909
  - obi_l3_k1: +0.0516
  - obi_l3_k10: +0.0606
  - obi_l3_k2: +0.0538
  - obi_l3_k5: +0.0580

## 6. News pipeline

- **News signals captured:** 5030

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 92800, Closed: 401960, Realized: $+175.1010
