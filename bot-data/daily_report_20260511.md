# Polymarket Arena Daily Report — 2026-05-11

_Generated: 2026-05-11 04:03 UTC_

## 1. System health

- **stalled**: True, alerts: ['arena_ticks.jsonl not updated in 3186s', 'orderbook_snapshots.jsonl not updated in 14590s', 'live_validator.json not updated in 3329s', 'arena_results.json not updated in 3354s', 'political_skeptic.json not updated in 3356s', 'theta_decay.json not updated in 3353s', 'whale_follower.json not updated in 3355s']
- **phase3 status**: phase3 result fresh

**File ages:**
- live_validator.log: age=3359s, size=96KB
- orderbook_collector.log: age=22s, size=119KB
- oil_iran.log: age=9s, size=326KB
- always_no.log: age=3351s, size=86KB
- whale_fade.log: age=30s, size=261KB
- arena_ticks.jsonl: age=3186s, size=103988KB
- orderbook_snapshots.jsonl: age=14590s, size=357738KB
- live_validator.json: age=3329s, size=1193KB
- arena_results.json: age=3354s, size=885KB
- political_skeptic.json: age=3356s, size=5KB
- council.json: age=0s, size=56KB
- theta_decay.json: age=3353s, size=4KB
- whale_follower.json: age=3355s, size=19KB
- whale_fade.json: age=missings, size=-KB
- maker.json: age=27s, size=4KB

## 2. Live Validator

- **Variants:** 206 total, 114 alive, 92 retired
- **Positions:** 2429 open, 14927 closed
- **Realized PnL:** $-3.6817 (actual $0.01 bets)
- **Aggregate equity:** $119404 / $114000 starting = **+4.74%**
- **Exit reasons:** {'tp': 7721, 'sl': 4655, 'htr': 2551}
- **Win3 skips:** 4526
- **Alive families:** {'BO': 11, 'BB': 26, 'RS': 23, 'WF': 16, 'ME': 22, 'MO': 8, 'SR': 4, 'MV5': 3, 'MV4': 1}

**Top 10 alive variants (n>=10 closes):**

| Variant | n | WR | LIVE ROI | BT | Equity |
|---------|----|------|---------|-----|--------|
| `RS_p14_t80_follow|plt70|stight|fany` | 58 | 78% | +25.4% | +22.5% | $1736 |
| `RS_p14_t80_follow|pany|stight|fany` | 85 | 73% | +16.1% | +17.5% | $1684 |
| `RS_p14_t80_follow|pgt30|stight|fany` | 79 | 72% | +14.7% | +17.8% | $1580 |
| `RS_p14_t80_follow|pgt50|sany|fany` | 82 | 71% | +11.2% | +17.5% | $1459 |
| `RS_p14_t80_follow|pgt50|stight|fany` | 76 | 71% | +11.6% | +17.8% | $1441 |
| `RS_p7_t80_follow|pgt30|stight|fany` | 156 | 67% | +5.6% | +16.0% | $1440 |
| `RS_p7_t80_follow|pany|stight|fany` | 169 | 68% | +4.8% | +15.6% | $1404 |
| `RS_p7_t75_follow|pgt30|stight|fany` | 198 | 66% | +4.0% | +14.7% | $1392 |
| `MV5_follow|plt70|stight|fany` | 58 | 71% | +10.1% | +0.0% | $1294 |
| `BO_p50_follow|plt30|sany|fany` | 48 | 77% | +11.9% | +19.5% | $1285 |

## 3. Arena (multi_strategy)

- **Total:** 1535 strategies, 415 active, 686 profitable
- **Family distribution:** {'rsi': 39, 'forest_': 960, 'mean_re': 174, 'ensembl': 70, 'bolling': 37, 'breakou': 19, 'zscore': 34, 'wavelet': 68, 'hybrid_': 92, 'momentu': 24, 'macd': 18}

**Top 10 by equity:**

| Strategy | Equity | W/L | WR |
|----------|--------|------|----|
| `S307|rsi|p14|e30.00|sl-25%|tp10%|free|s` | $1186 | 399/108 | 79% |
| `S199|rsi|p7|e30.00|sloff|tp10%|free|b` | $1175 | 549/237 | 70% |
| `S198|rsi|p7|e30.00|sl-25%|tp10%|free|b` | $1175 | 554/240 | 70% |
| `S201|rsi|p7|e25.00|sl-25%|tp10%|free|b` | $1171 | 535/232 | 70% |
| `S202|rsi|p7|e25.00|sloff|tp10%|free|b` | $1168 | 527/230 | 70% |
| `S203|rsi|p7|e35.00|sl-10%|tp5%|free|b` | $1157 | 621/260 | 70% |
| `S207|rsi|p7|e20.00|sl-25%|tp10%|free|b` | $1149 | 512/226 | 69% |
| `S197|rsi|p7|e30.00|sl-10%|tp5%|free|b` | $1144 | 567/249 | 69% |
| `S200|rsi|p7|e25.00|sl-10%|tp5%|free|b` | $1141 | 547/241 | 69% |
| `S1236|forest_|p15|e0.59|sloff|tp10%|free|b` | $1138 | 20/0 | 100% |

## 4. Sub-bots

- **Political Skeptic (Strategy A+B):** open=11, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **News Trader (Strategy C):** open=2, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000
- **Theta Decay:** open=10, W/L=0/0 (WR 0%), realized=$+0.0000, equity=$1000

**Always-NO variants:**

- v1_fee_free: W/L=22/2 (WR 92%), pnl=$+0.2563
- v2_with_fees: W/L=171/221 (WR 44%), pnl=$+0.0094
- v3_aggressive: W/L=22/4 (WR 85%), pnl=$+0.2354
- v4_conservative: W/L=0/1 (WR 0%), pnl=$-0.0100
- v5_all_cats: W/L=42/10 (WR 81%), pnl=$+0.3721

## 5. Phase 3 OBI/microprice

- **Last run:** 2026-05-10T23:45:40.201874+00:00 (4.3h ago)
- **Pairs:** 912,764 from 846 markets
- **Correlations:**
  - mu_dev_k1: +0.0508
  - mu_dev_k10: +0.0588
  - mu_dev_k2: +0.0577
  - mu_dev_k5: +0.0608
  - obi_l1_k1: +0.0778
  - obi_l1_k10: +0.0951
  - obi_l1_k2: +0.0836
  - obi_l1_k5: +0.0905
  - obi_l3_k1: +0.0509
  - obi_l3_k10: +0.0601
  - obi_l3_k2: +0.0530
  - obi_l3_k5: +0.0573

## 6. News pipeline

- **News signals captured:** 4576

## 7. Whale Fade Grid

- **Variants:** 4900
- **Open:** 91780, Closed: 339920, Realized: $+176.2080
