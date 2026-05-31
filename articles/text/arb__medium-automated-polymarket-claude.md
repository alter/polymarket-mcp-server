---
title: "Building an Automated Polymarket Trading System with Claude Code"
url: "https://medium.com/@rvarkarlsson/building-an-automated-polymarket-trading-system-with-claude-code-1982ff60cc74"
source: "medium.com"
date: "2026-01-01"
type: "blog"
theme: "arb"
lang: "en"
---

# Building an Automated Polymarket Trading System with Claude Code

**Author:** Örvar Karlsson

**URL:** https://medium.com/@rvarkarlsson/building-an-automated-polymarket-trading-system-with-claude-code-1982ff60cc74

---

## Project Overview

Complete trading automation framework built in a single Claude Code session for Polymarket. Implemented NegRisk arbitrage as primary strategy, with real trading results.

---

## System Architecture

**Six Core Components:**

1. **CLI Wrapper Foundation:** Wraps Polymarket's Rust CLI binary using "cli-anything methodology" — converts stateless commands into persistent Python SDK with session management and structured error handling

2. **Monitoring Layer:** Price monitor on 5-minute loop tracking: watchlist prices, portfolio P&L, alert thresholds, top opportunities

3. **Bracket Scanner:** Detects monotonicity violations in ladder markets (lower strike must price >= higher strike for same direction)

4. **NegRisk Scanner:** Identifies multi-outcome markets where Σ(YES prices) < $1.00, creating "guaranteed profit" opportunities

5. **Trade Executor:** With guardrails — maximum $20 per trade, $50 daily limit, 5-share minimum

6. **Learning Loop:** Extracts lessons from resolved trades, accumulates strategy scorecards for future decisions

---

## Scanning Results

Initial scan of Polymarket found:
- **182 arbitrage opportunities** across 440 market groups
- Vermont Governor Democratic Primary: **43% guaranteed ROI** (Σ YES prices = 0.57)
- Most opportunities: small ROI (0.5-3%) with adequate liquidity

---

## Trading Performance

**Period:** Single session experiment
**Bankroll:** $500
**Deployed:** $148.60 (8 resolved trades)

| Metric | Value |
|---|---|
| Total trades | 8 resolved |
| Win rate | 75% |
| Realized P&L | +$14.22 |
| ROI on deployed | 9.6% |
| NegRisk arb win rate | **100%** |
| NegRisk arb ROI | **36.2%** |

**Key finding:** "The boring strategies work best" — guaranteed-profit arbitrage substantially outperformed conviction-based directional bets.

---

## Technical Implementation Details

**Strategy selection logic:**
1. Scan all NegRisk market groups via Gamma API
2. Sum all YES prices in each group
3. If sum < $1.00 - fees: flag as opportunity
4. Rank by ROI = (1.00 - sum) / sum
5. Execute via standard market orders (NOT NegRisk convert — that's for capital efficiency only)

**Risk controls:**
- Max $20 per trade (prevents oversizing in illiquid markets)
- $50 daily limit (capital preservation during learning phase)
- 5-share minimum (avoid dust trades below fee threshold)

---

## Lessons Learned

1. NegRisk arbitrage is real and works — 100% win rate on small sample
2. ROIs (36.2%) are high because markets are genuinely inefficient in exotic/low-volume groups
3. Simple mathematical strategies beat opinion-based trades reliably
4. Automation makes the difference — manual scanning of 440+ market groups is infeasible
5. Learning loop compound effect: each resolved trade informs future strategy selection
