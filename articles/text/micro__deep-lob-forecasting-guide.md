---
title: "Deep Limit Order Book Forecasting: A Microstructural Guide"
url: https://arxiv.org/abs/2403.09267
source: arxiv
date: "2024-03-14"
type: paper
theme: micro
lang: en
---

# Deep Limit Order Book Forecasting: A Microstructural Guide

**Authors:** Antonio Briola, Silvia Bartolucci, Tomaso Aste

**Submitted:** March 14, 2024; revised June 4, 2024

**arXiv:** 2403.09267

**Published:** Quantitative Finance (Tandfonline), 2025

**PMC:** https://pmc.ncbi.nlm.nih.gov/articles/PMC12315853/

## Abstract

Advanced deep learning techniques employed to examine the predictability of high-frequency LOB mid-price movements across diverse NASDAQ-traded stocks. Authors introduce **LOBFrame** — an open-source toolkit for processing large-scale LOB data and evaluating deep learning models' forecasting performance.

## Key Findings

1. **Stocks' microstructural traits influence deep learning methodology effectiveness** — large-tick stocks have higher predictability
2. **High forecasting accuracy ≠ actionable trading signals** — traditional ML metrics fail in LOB context
3. **Novel evaluation framework** focused on likelihood of accurately predicting complete transactions (not just midprice direction)
4. **LOBFrame** is model-agnostic, supports integration of any forecasting architecture
5. DeepLOB, HLOB (Triangulated Maximally Filtered Graph + CNN + LSTM), and dual-stage attention models evaluated

## Architectures Reviewed

- **DeepLOB:** CNN + LSTM for NASDAQ stocks; strong baseline
- **HLOB:** TMFG graph structure captures inter-level dependencies; removes price columns
- **Dual-Stage Temporal Attention:** Highlights most valuable time dimensions
- **LOBFrame:** Open-source, reproducible benchmark

## Relevance to Polymarket CLOB Trading

- LOB mid-price forecasting methodology directly applicable to Polymarket markets (binary mid = implied probability)
- Operational evaluation framework (transaction probability) more relevant than classification accuracy for trading
- Large-tick finding maps to Polymarket: markets with round prices (0.50, 0.75) have more predictable dynamics
- DeepLOB-style models could be trained on Polymarket's full order book event stream

**Classification:** Quantitative Finance; Machine Learning
