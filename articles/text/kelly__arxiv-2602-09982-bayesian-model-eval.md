---
title: "Kelly Betting as Bayesian Model Evaluation: A Framework for Time-Updating Probabilistic Forecasts"
url: "https://arxiv.org/abs/2602.09982"
source: "arXiv"
date: "2026-02-10"
type: "academic_paper"
theme: "kelly"
lang: "en"
---

# Kelly Betting as Bayesian Model Evaluation: A Framework for Time-Updating Probabilistic Forecasts

**Author:** Michael Beuoy  
**arXiv ID:** 2602.09982 [stat.ME]  
**Submitted:** February 10, 2026  
**Length:** 31 pages, 10 figures  
**License:** CC BY 4.0

## Abstract Summary

The paper introduces a novel evaluation method for time-evolving probabilistic forecasts. Each model functions as a "canonical Kelly bettor," competing iteratively with others. Their bankroll trajectories serve as performance metrics.

Key features include:
- Real-time market consensus and model credibility updates without waiting for final outcomes
- Simulation-based demonstration that this approach outperforms traditional log-loss and Brier score methods at identifying correct versus incorrect models
- Direct mathematical and conceptual parallels to Bayesian inference, where "bankroll serves as a proxy for Bayesian credibility"

## Application Areas

The framework applies to dynamic forecasting scenarios including:
- In-game win probability models
- Election forecasts (prediction market context)
- Other time-updating probabilistic predictions

## Key Insight

If we view bankroll as analogous to credibility in a Bayesian sense, a Bayesian prior that two forecasters were equally probable to have the correct model can be updated based on their relative betting performance.

Similar to Bayesian updating, an improbable outcome shifts model bankrolls based on the assessed relative likelihood of the observed result.

## Relevance

This paper provides a framework for using Kelly betting as a principled scoring rule for probabilistic forecasts — directly applicable to evaluating prediction market models and strategies, where multiple competing models need to be compared on their forecasting accuracy in real time.
