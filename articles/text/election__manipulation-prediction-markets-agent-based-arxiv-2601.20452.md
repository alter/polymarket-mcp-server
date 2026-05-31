---
title: "Manipulation in Prediction Markets: An Agent-based Modeling Experiment"
url: "https://arxiv.org/abs/2601.20452"
source: "arXiv"
date: "2026-01-28"
type: "academic_paper"
theme: "election"
lang: "en"
---

# Manipulation in Prediction Markets: An Agent-based Modeling Experiment

**Authors:** Bridget Smart, Ebba Mark, Anne Bastian, Josefina Waugh  
**ArXiv ID:** 2601.20452  
**Submitted:** January 28, 2026  
**Institutions:** Institute for New Economic Thinking, University of Oxford; IGDORE, Gothenburg; Pontifical Catholic University of Chile

## Abstract

Prediction markets mobilize financial incentives to forecast binary event outcomes through the aggregation of dispersed beliefs and heterogeneous information. Their growing popularity and demonstrated predictive accuracy in political elections have raised speculation and concern regarding their susceptibility to manipulation and the potential consequences for democratic processes. Using agent-based simulations combined with an analytic characterization of price dynamics, the authors study how high-budget agents can introduce price distortions in prediction markets.

## Methodology

The study employed agent-based modeling combined with theoretical analysis. The simulation featured:

- **Market Structure:** Binary election prediction market with random order matching
- **Agents:** 100 traders with heterogeneous attributes including expertise, risk aversion, stubbornness, and budgets
- **Price Dynamics:** Market price updates based on net demand
- **Agent Behavior:** Traders maximize expected utility functions while updating beliefs based on "fuzzy" signals of true outcomes

Key agent characteristics:
- **Expertise:** Affects signal clarity (variance 1-expertise)
- **Stubbornness:** Resistance to updating valuations
- **Herding:** Tendency to weight market price in belief formation
- **Risk Aversion:** Heterogeneous utility preferences

## Key Findings

### Market Resilience
The model demonstrated stability across broad parameter ranges. Mean squared error between market price and true outcome ranged from 0.006–0.1, comparable to deviations between Polymarket prices and The Economist's 2016–2020 election forecasts.

### Whale Manipulation Effects
- Whales required approximately 40% of total market capital to induce "meaningful error" into prices
- Distortion magnitude scaled proportionally to whale budget share × misvaluation
- **Theoretical result:** Steady-state price error = (whale budget proportion) × (whale valuation error)

### Herding Amplification
High herding behavior (hi values approaching 1.0) significantly prolonged price distortion recovery and introduced oscillatory patterns. Large herding weights increased "the strength of feedback from market price back into valuations," slowing correction mechanisms.

### Profitability Opportunities
Well-informed traders (expertise = 0.95) consistently gained profits from temporary whale-induced mispricings, regardless of whale budget proportion.

## Theoretical Analysis

Two scenarios were analyzed:

**Without Herding:** The formula δS = ρΔS showed that steady-state price error equals the whale's budget fraction times its valuation error.

**With Herding:** Individual agent error shrinks by the factor (1-hi)si, where herding intensity (hi) and stubbornness (si) interact to determine correction speed.

## Policy Implications

The research suggests regulatory frameworks should consider "restrictions on individual trade sizes" to protect election prediction markets from concentrated capital manipulation. This contrasts with recent deregulation trends; the CFTC approved Kalshi's platform allowing "$100 million" trades by eligible participants.

## Conclusions

While prediction markets demonstrate self-correcting properties under normal conditions, they become vulnerable to sustained manipulation when single traders control sufficient capital — particularly when coupled with widespread herding behavior. The researchers emphasized that "temporary distortion caused by a large, stubborn, or strategically biased whale could shape voter expectations, campaign donations, or media narratives," raising democratic integrity concerns.

The team released their model as open-source software on GitHub with a graphical interface for parameter exploration.
