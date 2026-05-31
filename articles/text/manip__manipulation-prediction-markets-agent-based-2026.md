---
title: "Manipulation in Prediction Markets: An Agent-based Modeling Experiment"
url: "https://arxiv.org/abs/2601.20452"
source: arXiv (econ.GN)
date: "2026-01-28"
type: academic_paper
theme: manip
lang: en
---

# Manipulation in Prediction Markets: An Agent-based Modeling Experiment

**Authors:** Bridget Smart, Ebba Mark, Anne Bastian, Josefina Waugh  
**Institution:** University of Oxford (INET)  
**arXiv:** 2601.20452  
**Submitted:** January 28, 2026  
**HTML:** https://arxiv.org/html/2601.20452v1  
**Publication:** INET Oxford Working Paper

## Abstract

Uses agent-based modeling combined with analytical characterization of price dynamics to study how high-budget agents ("whales") can introduce price distortions in prediction markets. Studies persistence and stability of distortions in the presence of herding/stubborn agents, and analyzes how agent expertise affects market-price variance.

## Model Design

- 100+ heterogeneous betting agents (expertise, risk aversion, stubbornness, bias)
- Binary election outcome modeled as random walk
- Price-responsive order-matching mechanism
- Optional herding behavior where agents weight market prices in valuations
- Validated against The Economist's election prediction model (MSE benchmark)

## Key Findings

### Market Resilience
- Markets show "meaningful resilience to manipulation by biased agents"
- Whales require approximately **40% of total market capital** to induce meaningful price distortion
- Price distortions persist longer with stubborn or herding agents

### Theoretical Price Distortion
> "Steady-state price error equals the biased agent's budget fraction times its valuation error"

Distortion scales proportionally to whale capital concentration × misvaluation magnitude.

### Herding Amplification
Strong herding compounds distortion magnitude and duration; can create oscillatory price patterns rather than smooth recovery.

### Real-World Reference
In 2024, Polymarket identified a **$45 million bet** by a French national to favor Donald Trump, temporarily pushing up his odds.

## Regulatory Implications

- Early prediction market platforms (Iowa Electronic Markets, PredictIt) had strict per-trader caps ($850/market for PredictIt) preventing whale manipulation
- Newer platforms (Polymarket) lack such protections
- Authors advocate for "thoughtfully considered regulatory approach" including **maximum order sizes**
- Democratic risk: if prediction market prices influence voter behavior or media narratives, temporary manipulations could have downstream electoral consequences

## Open Source

Code available on GitHub: `power_prediction` repository
