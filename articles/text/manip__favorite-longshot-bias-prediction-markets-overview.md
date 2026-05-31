---
title: "The Favorite-Longshot Bias in Prediction Markets: Overview and Evidence"
url: "https://laikalabs.ai/prediction-markets/prediction-market-biases-how-to-exploit-profit"
source: Multi-source synthesis (academic literature)
date: "2026-02"
type: literature_review
theme: manip
lang: en
---

# The Favorite-Longshot Bias in Prediction Markets

**Primary academic sources:**
- Griffith (1949): First documentation in horse racing
- Ali (1977): Theoretical framework
- Thaler & Ziemba (1988): Behavioral finance perspective
- Restocchi et al. (2018): Temporal evolution, ScienceDirect https://www.sciencedirect.com/science/article/abs/pii/S1544612318303349 (open access: https://eprints.soton.ac.uk/423232/1/TempEvol.pdf)
- Le (2026): Cross-platform calibration, arXiv:2602.19520
- CEPR analysis (2026): 300,000+ Kalshi contracts

## Definition

The **favorite-longshot bias (FLB)**: in betting markets, low-probability outcomes (longshots) are systematically overpriced while high-probability outcomes (favorites) are systematically underpriced.

- Longshots (5¢–20¢ implied probability): actual win rate **2–12%** vs. 5–20% implied
- Favorites (80¢–95¢): actual win rate **96–98%** vs. 80–95% implied

The FLB is described as *"the single most robust finding in prediction market research"* — documented across horse racing, sports betting, and modern prediction markets spanning decades and geographies.

## Evidence in Modern Prediction Markets

### CEPR / Kalshi Analysis (Feb 2026)
- **300,000+ Kalshi contracts** analyzed
- Low-price contracts (5¢–20¢): **consistent losses averaging 60% of capital**
- High-price contracts (80¢–95¢): **small positive returns**
- Favorite-longshot bias is systematic across domains

### Le (2026) — Kalshi + Polymarket
- Political contracts specifically show **persistent underconfidence** at nearly all time horizons
- 70¢ political contract one week before resolution → true probability ~83%
- Bias confirmed as "structural property of political prediction markets" not platform-specific

### Restocchi et al. (2018) — Temporal Dynamics
- Using 4,000 political prediction market prices (daily)
- FLB related to both **time left to expiration** and **market duration**
- Markets more efficient with longer duration on average; but in final days, shorter-duration markets more efficient
- Temporal dynamics consistent with **herding behavior**

## Competing Explanations

| Explanation | Mechanism |
|-------------|-----------|
| Risk-love (neoclassical) | Rational gamblers derive utility from variance; willingly accept negative EV on longshots |
| Probability misperception (behavioral) | Agents systematically overestimate low probabilities (Kahneman/Tversky) |
| Informed trading | Favorites are underpriced because informed traders prefer lower-variance bets |
| Strategic bookmaker behavior | Bookmakers manipulate morning-line odds to exploit naive bettors (The Midas effect) |

Research favors **behavioral probability misperception** over risk-love. Expected utility + prospect theory models both confirm the FLB arises from probability distortion rather than risk preference.

## Trading Implications

FLB is exploitable in theory:
- Sell/short longshots (buy "No" on low-probability outcomes)
- Buy favorites (buy "Yes" on high-probability outcomes)

Practical limits:
- Transaction costs reduce edge
- Position sizing constrained by liquidity on low-probability outcomes
- Opposite direction: "reverse FLB" (favorite bias) documented in in-play sports markets

## Cross-Platform Arbitrage

Consistent with Le (2026) and Gebele & Matthes (2026, arXiv:2601.01706): calibration biases differ by platform, creating cross-platform arbitrage that is difficult to execute due to semantic non-fungibility and oracle risk differentials.

## Related: "Reverse FLB" in In-Play Markets

Angelini, De Angelis & Singleton (2021): In-play Betfair soccer markets show **reverse favorite-longshot bias (favorite bias)**. Markets overestimated decisiveness of first goal by favorites, underestimated when scored by longshots. Mispricing strongest 20 seconds post-goal, still significant 5 minutes later.
