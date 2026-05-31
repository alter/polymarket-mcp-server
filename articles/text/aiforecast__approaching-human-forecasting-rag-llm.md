---
title: "Approaching Human-Level Forecasting with Language Models"
url: "https://arxiv.org/abs/2402.18563"
source: "arxiv"
date: "2024-02-28"
type: "paper"
theme: "aiforecast"
lang: "en"
---

# Approaching Human-Level Forecasting with Language Models

**Authors:** Danny Halawi, Fred Zhang, Chen Yueh-Han, Jacob Steinhardt

**arXiv:** 2402.18563 | Submitted: February 2024

## Summary

Foundational paper demonstrating RAG-based LLM forecasting approaching human crowd performance. Creates an automated retrieval-augmented LM system that searches for relevant information, generates forecasts, and aggregates predictions. Tests on competitive forecasting platform questions published after models' knowledge cutoffs.

## Key Methods

- Retrieval-augmented generation (RAG) for automated information gathering
- Multiple forecast generation with aggregation
- Evaluation on post-knowledge-cutoff questions from competitive forecasting platforms (Metaculus, Polymarket, etc.)
- LLM-based reasoning pipeline for probabilistic estimation

## Key Results

- System achieves performance approximating human crowd forecasts on competitive forecasting platforms
- Occasionally surpasses human crowd performance
- Demonstrates viability of scalable, automated LLM-based forecasting for real-world events
- Information retrieval pipeline design remains influential in subsequent work (AIA, BLF, OpenForecaster)

## Relevance to Polymarket Trading

Foundational architecture for LLM forecasting agents: RAG + aggregation. This pipeline design is cited across virtually all subsequent Polymarket-specific papers. The news retrieval + multi-forecast aggregation pattern is the starting template.
