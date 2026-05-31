# Polymarket Arena Daily Report — 2026-05-13

_Generated: 2026-05-13 00:44 UTC_

## 1. System health

- **stalled**: False, alerts: []
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=274s, size=8KB
- orderbook_collector.log: age=8s, size=2KB
- oil_iran.log: age=1s, size=5KB
- always_no.log: age=112s, size=4KB
- whale_fade.log: age=270s, size=12KB
- arena_ticks.jsonl: age=16s, size=110023KB
- orderbook_snapshots.jsonl: age=8s, size=380346KB
- live_validator.json: age=274s, size=1039KB
- arena_results.json: age=286s, size=886KB
- political_skeptic.json: age=51s, size=5KB
- council.json: age=265s, size=50KB
- theta_decay.json: age=28s, size=5KB
- whale_follower.json: age=290s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=56s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 83 alive, 123 retired
- **Positions:** 2013 open, 16880 closed
- **Realized PnL:** $-6.7760 (actual $0.01 bets)
- **Aggregate equity:** $88163 / $83000 starting = **+6.22%**
- **Exit reasons:** {'tp': 8695, 'sl': 5201, 'htr': 2984}
- **Win3 skips:** 4945
- **Alive families:** {'BO': 11, 'BB': 16, 'RS': 6, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BO_p50_follow|plt30|sany|fany` | 79 | 77% | +33.2% | +19.5% | $2310 |
| `BB_p50_sd25_fade|plt30|sany|fany` | 60 | 72% | +30.9% | +45.4% | $1927 |
| `BO_p100_follow|plt30|sany|fany` | 51 | 80% | +28.6% | +28.1% | $1728 |
| `BB_p50_sd25_fade|plt70|swide|fany` | 56 | 73% | +25.4% | +63.6% | $1710 |
| `BB_p50_sd25_fade|plt50|swide|fany` | 52 | 73% | +22.5% | +64.0% | $1586 |
| `BO_p100_follow|plt30|swide|fany` | 41 | 80% | +27.9% | +33.6% | $1571 |
| `BO_p100_follow|plt50|swide|fany` | 53 | 79% | +19.0% | +33.3% | $1504 |
| `BO_p100_follow|pany|swide|fany` | 56 | 77% | +13.0% | +33.1% | $1364 |
| `BO_p100_follow|plt70|swide|fany` | 56 | 77% | +13.0% | +33.2% | $1364 |
| `BO_p50_follow|plt30|swide|fany` | 91 | 68% | +1.8% | +19.2% | $1080 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 401 active, 729 profitable
- **Family distribution:** {'rsi': 39, 'forest_': 960, 'mean_re': 174, 'bolling': 37, 'zscore': 34, 'ensembl': 70, 'breakou': 19, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1226 | 630/251 | 72% |
| `S199|rsi|p7|e30.00|sloff|tp10%|free|b` | $1226 | 644/257 | 71% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1225 | 649/260 | 71% |
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $1224 | 476/120 | 80% |
| `S202|rsi|p7|e25.00|sloff|tp10%|free|b` | $1223 | 622/249 | 71% |
| `S203|rsi|p7|e35.00|sl-10%|tp5%|free|b` | $1208 | 718/280 | 72% |
| `S207|rsi|p7|e20.00|sl-25%|tp10%|free|b` | $1205 | 607/245 | 71% |
| `S200|rsi|p7|e25.00|sl-10%|tp5%|free|b` | $1197 | 642/260 | 71% |
| `S197|rsi|p7|e30.00|sl-10%|tp5%|free|b` | $1192 | 662/269 | 71% |
| `S206|rsi|p7|e20.00|sl-10%|tp5%|free|b` | $1171 | 620/254 | 71% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=4, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/2 (WR 92%), pnl=$+0.2661
- v2_with_fees: W/L=251/330 (WR 43%), pnl=$-0.1062
- v3_aggressive: W/L=23/4 (WR 85%), pnl=$+0.2452
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/11 (WR 80%), pnl=$+0.3719

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-12T22:07:08.217235+00:00 (2.6h ago)
- **Pairs:** 970,212 from 918 markets
- **Correlations:**
  - mu_dev_k1: +0.0517
  - mu_dev_k10: +0.0588
  - mu_dev_k2: +0.0586
  - mu_dev_k5: +0.0607
  - obi_l1_k1: +0.0780
  - obi_l1_k10: +0.0949
  - obi_l1_k2: +0.0838
  - obi_l1_k5: +0.0906
  - obi_l3_k1: +0.0519
  - obi_l3_k10: +0.0605
  - obi_l3_k2: +0.0541
  - obi_l3_k5: +0.0580

## 6. News pipeline

- **News signals captured:** 5389

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 97310, Closed: 449000, Realized: $+244.2535
