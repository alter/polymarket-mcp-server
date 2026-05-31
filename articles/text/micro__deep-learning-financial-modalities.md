---
title: "Deep Learning Models Meet Financial Data Modalities"
url: https://arxiv.org/abs/2504.13521
source: arxiv
date: "2025-04-18"
type: paper
theme: micro
lang: en
---

# Deep Learning Models Meet Financial Data Modalities

**Authors:** Kasymkhan Khubiev, Mikhail Semenov

**Submitted:** April 18, 2025; Revised April 21, 2025

**arXiv:** 2504.13521

## Abstract

Addresses the integration of deep learning models with financial data modalities, aiming to enhance predictive performance in trading strategies. Develops embedding techniques for limit order book analysis, treating sequential snapshots as image-based input channels.

## Key Findings

1. **LOB as image representation:** Sequential LOB snapshots treated as multi-channel images — enables CNN-based processing
2. Claims **state-of-the-art performance** in HFT algorithms via this approach
3. Covers multiple financial data modalities:
   - Candlestick charts
   - Order statistics
   - Volume data
   - Limit order books
   - News flow
4. Novel embedding techniques for LOB processing

## Data Modalities and Models

| Data Type | Representation | Architecture |
|-----------|---------------|--------------|
| Candlesticks | OHLCV time series | LSTM, Transformer |
| LOB snapshots | 2D image (price × time) | CNN |
| News text | Embedding | BERT, GPT |
| Volume profile | Histogram | MLP |
| Multi-modal | Combined embeddings | Fusion model |

## Relevance to Polymarket CLOB Trading

- **LOB as image on Polymarket:** Treat Polymarket order book snapshots as 2D arrays (price level × time) → CNN can learn spatial-temporal patterns
- **Multi-modal fusion:** Combine Polymarket LOB data with news embeddings (event descriptions) for better probability estimation
- **Order statistics modality:** Polymarket provides rich order statistics via CLOB API — integrate with price prediction
- **Practical pipeline:** LOB snapshot → CNN embedding → concatenate with news/event embedding → price direction classifier

**Classification:** Computer Science (Machine Learning); Quantitative Finance
