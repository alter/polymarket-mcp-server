---
title: "How does the Logarithmic Market Scoring Rule (LMSR) work?"
url: "https://www.cultivatelabs.com/crowdsourced-forecasting-guide/how-does-logarithmic-market-scoring-rule-lmsr-work"
source: "cultivatelabs.com"
date: "2022-01-01"
type: "blog"
theme: "amm"
lang: "en"
---

# How does the Logarithmic Market Scoring Rule (LMSR) work?

**Source:** Cultivate Labs Crowdsourced Forecasting Guide

## What is LMSR?

A market scoring rule is used to compute the current price of a stock in a prediction market. Consequently, a market scoring rule is also used to compute the cost of a trade between a trader and the market maker. LMSR was invented by Robin Hanson and is one of the most commonly used market scoring rules.

## Key Mechanisms

**Always provides a quote and instant fill** — no need to match with another trader. Liquidity is continuous by design, and price impact is predictable (bigger orders move prices more).

**Jointly coherent prices** — In a market with mutually exclusive outcomes, prices always sum to 100%. If one outcome's implied probability goes up, others automatically adjust downward.

**The liquidity parameter b** — Controls market depth. Too little liquidity makes prices fluctuate wildly after every trade; too much makes prices barely budge.

## LMSR Formula

Cost(q_1, q_2, ..., q_n) = b * ln(sum(e^(q_i / b)))

Price of outcome i = e^(q_i / b) / sum(e^(q_j / b))

## Practical Use Cases

- Political event prediction
- Building opening date prediction
- Product sales forecasting
- Instructor rating markets
- Internal corporate forecasting (Microsoft)

## Comparison with Order Books

Order books require matching buyers with sellers. LMSR provides infinite liquidity algorithmically. Order books are preferred for high-volume markets; LMSR is superior for thin, new, or niche markets.
