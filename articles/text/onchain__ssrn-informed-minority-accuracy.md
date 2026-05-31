---
title: "Prediction Market Accuracy: Crowd Wisdom or Informed Minority?"
url: "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6617059"
source: "SSRN"
date: "2026-04-20"
type: "academic_paper"
theme: "onchain"
lang: "en"
---

# Prediction Market Accuracy: Crowd Wisdom or Informed Minority?

**Authors:** Roberto Gomez-Cram, Yunhan Guo, Howard Kung (London Business School); Theis Ingerslev Jensen (Yale University)
**Published:** April 20, 2026 on SSRN; revised April 25, 2026
**Abstract ID:** 6617059

## Scale of Study

The researchers analyzed the complete transaction history on Polymarket, the world's largest prediction market by trading volume:
- 98,906 events
- 210,322 markets
- $13.76 billion in total trading volume
- 1.72 million accounts
- Trades from 2023–2025

## Core Finding: The "Informed Minority" Drives Accuracy

Prediction market accuracy on Polymarket comes from a small group of informed traders, not the broad crowd:
- Only **3.14% of accounts** qualify as "skilled winners" — traders whose positions consistently predict both short-term price movements and event outcomes
- Together with market makers, skilled winners capture **more than 30% of all profits**
- **68.8% of Polymarket users lost money**; top 1% captures 77% of gains

Trader classification breakdown:
- Skilled winners: 3.14% of accounts
- Lucky winners (insignificant profits): 29.0%
- Unlucky losers (insignificant losses): 61.4%

Key quote: "The remaining majority does not produce accuracy; rather, it funds it. Their trades generate most of the volume, but little of the information, and their losses flow as profits to the informed minority. Prediction market accuracy thus reflects the wisdom of an informed minority, not the wisdom of the crowd."

## Methodology: Separating Skill from Luck

The researchers used a **sign-randomization test** (coin-flip simulation): each trader's bets were rerun 10,000 times, keeping everything the same except the direction. Results:
- Only 12% of biggest winners by raw profit beat the random benchmark
- Approximately 60% of "lucky winners" became losers when tested against separate event samples
- Skilled traders consistently predict outcomes and react first to new information (e.g., FOMC announcements, corporate earnings releases)

## Skill Persistence

Among traders classified as skilled in training set, **44% retained that classification in the test set** (vs. only 10% for skilled mutual funds in a parallel test). For unskilled losers, 51% remained in that category.

## Suspected Insider Trading

- **1,950 accounts** identified that met timing and conviction criteria suggesting non-public information
- These insider accounts averaged roughly **$15,000 in profits each**
- Insider trades move prices **7–12 times more aggressively per dollar** than typical skilled trades
- Documented Maduro case: Three accounts collectively earned **$630,000** by positioning hours before a secret U.S. military operation on Jan. 3, 2026; CFTC filed charges April 23, 2026 against Army soldier Gannon Ken Van Dyke

## Platform Growth

Monthly trading volume:
- December 2023: $3.3 million
- December 2025: $1.98 billion (~600× increase in 2 years)

Active accounts: ~1,600 (Dec 2023) → 519,000+ (Dec 2025)

## Regulatory Implications

The study challenges the "wisdom of the crowd" narrative underpinning prediction market platforms. It provides regulators (CFTC) an additional argument that these platforms resemble classical financial markets with participant inequality and insider trading risks. Polymarket was reportedly in talks to raise $400M at a $15B valuation; lawmakers in Washington, New York, and California introduced bills targeting insider participation.

## Related Paper

"The Polymarket Paradox: Manipulation, Whale Concentration, and Predictive Accuracy in the World's Largest Prediction Market" by Muhammad Noraiz Abid (SSRN abstract_id=6670638, April 28, 2026) — documents boundary conditions where counter-party profit incentives correct distorted prices and where corrections fail.

Also related: "Exploring Decentralized Prediction Markets: Accuracy, Skill, and Bias on Polymarket" by Felix Reichenbach, Martin Walther (SSRN abstract_id=5910522).
