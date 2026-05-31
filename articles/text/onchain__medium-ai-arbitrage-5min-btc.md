---
title: "AI-Augmented Arbitrage in Short-Duration Prediction Markets: Live Trading Analysis of Polymarket's 5-Minute Bitcoin Binary Options"
url: "https://medium.com/@gwrx2005/ai-augmented-arbitrage-in-short-duration-prediction-markets-live-trading-analysis-of-polymarkets-8ce1b8c5f362"
source: "Medium (Jung-Hua Liu)"
date: "2025-01-01"
type: "research"
theme: "onchain"
lang: "en"
---

# AI-Augmented Arbitrage in Polymarket 5-Minute Bitcoin Binary Options

**Author:** Jung-Hua Liu
**Claim:** First detailed live trading results on Polymarket's 5-minute BTC markets

---

## Key Performance Metrics

### Session 1 (Signal Engine v2):
- Record: 4 wins, 11 losses
- Capital loss: $15.47 (-49.5% ROI)
- Starting balance: ~$17 USDC.e
- Duration: ~2 hours
- Critical flaw: 80% of trades bet UP during a downtrending market

### Session 2 (Signal Engine v3):
- Record: 2 wins, 2 losses
- Capital loss: $4.18 (-13.4% ROI)
- Starting balance: $31.19 USDC.e
- Duration: 1 hour
- **Improvement: 7× better capital preservation**

---

## Root Cause Analysis of v2 Failure (5 Compounding Factors)

1. **Short-timeframe bias**: Weights allocated 65% to final 60 seconds — captures micro-bounces within larger downtrends
2. **Insufficient threshold**: 0.01% BTC move triggers fell within Brownian noise ranges
3. **Missing medium-term context**: No awareness of 10+ minute price trajectory
4. **Resolver errors**: Using arbitrary spot prices instead of official resolution data
5. **Duplicate bets**: Absence of per-window deduplication amplified losses by $10.44

---

## Engineering Remediation (v3)

Three critical changes:
1. **10-minute trend filter**: Hard rule blocking counter-trend DISLOCATION signals
2. **Rebalanced momentum weights**: Shifted from [0.35, 0.30, 0.20, 0.15] to longer lookbacks
3. **Raised signal thresholds**: Reduced trade frequency by 73%, filtering noise-driven entries

---

## Market Microstructure Findings

- **Bid-ask spreads**: 2–5 cents; combined asks totaling ≥$1.00 after 2% taker fees
- **Fee structure**: ~1.56% at $0.50 entry price
- **Execution slippage**: 2–4 cents per token in live trading vs. zero in paper trading
- **Win rate reality**: 25–27% observed vs. ~53% needed for breakeven

---

## Three Signal Types

**DISLOCATION**: BTC moves >0.05% without corresponding token price adjustment. Fair probability estimated with 5-second decay factor.

**DIRECTIONAL**: Fires in final 30 seconds when composite confidence ≥0.45 and direction confirmed by >0.03%.

**MAKER**: Posts limit orders 2 cents below ask, requiring 0.45 confidence for 20% rebate capture.

---

## LLM Integration (OpenClaw v2)

Hybrid architecture submits 5-section structured briefings to Kimi (moonshot-v1-auto, temperature 0.3):
- BTC Trend, Recent Outcomes, Portfolio status, Market conditions, Signal assessment

**Six hard rules constraining LLM decisions:**
1. Block duplicate bets on same window
2. Reject signals opposing 15-minute trend (unless BTC move >0.10%)
3. Require edge >0.05 after 3+ consecutive losses
4. Filter moves <0.03% with >90 seconds remaining
5. Avoid betting against sides priced >60%
6. Restrict trading when cash <30% of starting balance

All LLM failures default to REJECT.

---

## Critical Finding: Paper-to-Live Gap

Same engine achieving 522× returns in simulation lost 49.5% live. This confirms backtest overfitting — realistic execution costs consume theoretical edges entirely.

---

## Efficient Markets Conclusion

The 25–27% live win rate suggests **5-minute BTC binary options are efficiently priced** at ultra-short horizons. Transaction costs and adverse selection eliminate positive expected value despite 2–6% theoretical edges.

---

## Limitations

- Only 19 total trades (insufficient statistical power)
- Single market regime (Bitcoin downtrend)
- LLM variability across temperature and model selection
- Using market-price takers rather than maker-only strategies

---

## Key Takeaway

Signal quality dominates LLM sophistication. Mechanical structural guards (trend filters, threshold increases) outperformed AI reasoning on biased data inputs, yielding 7× capital preservation improvement.
