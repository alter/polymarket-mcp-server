---
title: "Political Shocks and Price Discovery in Prediction Markets: Evidence from the 2024 U.S. Presidential Election"
url: "https://arxiv.org/abs/2603.03152"
source: "arXiv"
date: "2026-03-03"
type: "academic_paper"
theme: "election"
lang: "en"
---

# Political Shocks and Price Discovery in Prediction Markets: Evidence from the 2024 U.S. Presidential Election

**Authors:** Kwok Ping Tsang and Zichao Yang  
**ArXiv ID:** 2603.03152  
**Submitted:** March 3, 2026 (revised March 17, 2026)

## Abstract

Using transaction-level trade data from Polymarket's 2024 U.S. presidential election market, this paper studies how prediction markets process shocks, analyzing three events: the Biden-Trump debate, the assassination attempt on Trump, and Biden's dropout. The debate-induced price jump largely reverses, the assassination-attempt repricing persists, and Biden's dropout triggers two-sided trading with little net price change — patterns that link post-news price dynamics to liquidity and disagreement about how shocks map into election odds.

## Methodology

**Data Source:** 3.65 million matched trades from January–November 2024 across Trump, Biden, and Harris contracts

**Analytical Approach:**
- Event-study design around precisely timestamped shocks
- 5-minute time aggregation for price and volume analysis
- Kyle's lambda and Glosten-Harris decomposition for price impact
- Variance ratios to assess drift versus reversal patterns
- Two-sidedness index measuring buy/sell balance

**Events Examined:**
- June 28 debate (1:00 UTC)
- July 13 assassination attempt (22:11 UTC)
- July 21 Biden dropout (17:46 UTC)

## Major Findings

### Trading Response Heterogeneity

**Extensive Margin (New Entry):** The assassination attempt and dropout sparked clear spikes in first-time trader participation, while the debate did not, indicating that "heightened public salience and heightened incumbent trading are not the same object."

**Intensive Margin (Incumbent Activity):** All three shocks prompted significant increases in trading by existing market participants, with concentrated response among highly active traders whose portfolios were most exposed to relevant electoral outcomes.

### Trader Characteristics

Pre-event analysis reveals that traders with negative Trump WIN exposure (portfolios losing value if Trump odds rise) become disproportionately active after news arrival. These traders also exhibit substantially higher rates of position flips, indicating active portfolio adjustment rather than passive rebalancing.

### Price Adjustment Patterns

The three events produced markedly different price dynamics:

**Biden-Trump Debate:** Initial Trump YES price jump of approximately 11 cents largely reverses, ending only 2 cents above pre-event levels. The Glosten-Harris decomposition shows the transitory component is relatively large.

**Trump Assassination Attempt:** Similar 11-cent peak increase persists throughout the observation window, reflecting larger permanent repricing. Variance ratio analysis shows unusual drift in the immediate post-shock period.

**Biden Dropout:** Exceptionally heavy trading volume coincides with minimal net price movement (4-cent trough, 2-cent decline by window end). The two-sidedness index reveals balanced buy/sell pressure, suggesting substantial trader disagreement about how this shock should affect Trump's winning odds.

### Price Impact Mechanics

Kyle's lambda declined after the assassination attempt and dropout events (p-values: 0.002 and 0.022), indicating lower price sensitivity to signed flow. However, the composition of price impact differed fundamentally: assassination repricing appeared more informational, while dropout repricing reflected heavy offsetting trade.

## Practical Implications

The research suggests prediction markets remain valuable real-time information aggregators but "their short-run behavior has to be read through a microstructure lens." During high-salience events, the same shock can generate persistent repricing (assassination attempt), partial reversal (debate), or balanced offsetting trade (dropout), depending on factors including order flow composition and trader disagreement.

## Methodological Limitations

The authors acknowledge observing only executed trades rather than full order book depth, making all liquidity measures necessarily transaction-based.
