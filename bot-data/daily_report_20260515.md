# Polymarket Arena Daily Report — 2026-05-15

_Generated: 2026-05-15 02:09 UTC_

## 1. System health

- **stalled**: True, alerts: ['maker.json not updated in 705s']
- **phase3 status**: phase3 backtest LAUNCHED

**File ages:**
- live_validator.log: age=14s, size=20KB
- orderbook_collector.log: age=0s, size=9KB
- oil_iran.log: age=9s, size=24KB
- always_no.log: age=24s, size=14KB
- whale_fade.log: age=30s, size=29KB
- arena_ticks.jsonl: age=19s, size=110657KB
- orderbook_snapshots.jsonl: age=30s, size=382640KB
- live_validator.json: age=13s, size=957KB
- arena_results.json: age=18s, size=885KB
- political_skeptic.json: age=706s, size=5KB
- council.json: age=22s, size=50KB
- theta_decay.json: age=6s, size=5KB
- whale_follower.json: age=695s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=705s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 82 alive, 124 retired
- **Positions:** 1796 open, 17230 closed
- **Realized PnL:** $-6.5681 (actual $0.01 bets)
- **Aggregate equity:** $87946 / $82000 starting = **+7.25%**
- **Exit reasons:** {'tp': 8880, 'sl': 5239, 'htr': 3111}
- **Win3 skips:** 4977
- **Alive families:** {'BO': 11, 'BB': 16, 'RS': 5, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `BO_p50_follow|plt30|sany|fany` | 88 | 77% | +34.5% | +19.5% | $2520 |
| `BO_p100_follow|plt30|sany|fany` | 58 | 79% | +31.3% | +28.1% | $1908 |
| `BB_p50_sd25_fade|plt70|swide|fany` | 62 | 76% | +27.5% | +63.6% | $1852 |
| `BB_p50_sd25_fade|plt30|sany|fany` | 67 | 72% | +23.4% | +45.4% | $1784 |
| `BB_p50_sd25_fade|plt50|swide|fany` | 58 | 76% | +25.1% | +64.0% | $1728 |
| `BO_p100_follow|plt30|swide|fany` | 45 | 80% | +27.2% | +33.6% | $1612 |
| `BO_p100_follow|plt50|swide|fany` | 58 | 79% | +19.2% | +33.3% | $1556 |
| `BO_p100_follow|pany|swide|fany` | 61 | 77% | +13.6% | +33.1% | $1416 |
| `BO_p100_follow|plt70|swide|fany` | 61 | 77% | +13.6% | +33.2% | $1416 |
| `BB_p20_sd25_fade|plt30|sany|fany` | 34 | 85% | +10.4% | +35.7% | $1176 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 401 active, 729 profitable
- **Family distribution:** {'rsi': 39, 'forest_': 960, 'mean_re': 174, 'bolling': 37, 'zscore': 34, 'ensembl': 70, 'breakou': 19, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $1228 | 481/121 | 80% |
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1226 | 637/254 | 71% |
| `S199|rsi|p7|e30.00|sloff|tp10%|free|b` | $1225 | 651/260 | 71% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1224 | 656/263 | 71% |
| `S202|rsi|p7|e25.00|sloff|tp10%|free|b` | $1222 | 629/252 | 71% |
| `S203|rsi|p7|e35.00|sl-10%|tp5%|free|b` | $1207 | 726/283 | 72% |
| `S207|rsi|p7|e20.00|sl-25%|tp10%|free|b` | $1204 | 614/248 | 71% |
| `S200|rsi|p7|e25.00|sl-10%|tp5%|free|b` | $1196 | 649/263 | 71% |
| `S197|rsi|p7|e30.00|sl-10%|tp5%|free|b` | $1192 | 669/272 | 71% |
| `S206|rsi|p7|e20.00|sl-10%|tp5%|free|b` | $1170 | 627/257 | 71% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=5, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=23/2 (WR 92%), pnl=$+0.2661
- v2_with_fees: W/L=329/448 (WR 42%), pnl=$-0.3666
- v3_aggressive: W/L=23/4 (WR 85%), pnl=$+0.2452
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=43/11 (WR 80%), pnl=$+0.3719

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-14T23:06:01.121218+00:00 (3.1h ago)
- **Pairs:** 976,404 from 995 markets
- **Correlations:**
  - mu_dev_k1: +0.0518
  - mu_dev_k10: +0.0591
  - mu_dev_k2: +0.0586
  - mu_dev_k5: +0.0613
  - obi_l1_k1: +0.0779
  - obi_l1_k10: +0.0947
  - obi_l1_k2: +0.0837
  - obi_l1_k5: +0.0905
  - obi_l3_k1: +0.0517
  - obi_l3_k10: +0.0603
  - obi_l3_k2: +0.0539
  - obi_l3_k5: +0.0576

## 6. News pipeline

- **News signals captured:** 5621

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 93220, Closed: 496260, Realized: $+160.8210
