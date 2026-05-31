---
title: "Заработок на Polymarket при помощи ботов / OpenClaw AI bot trading on Polymarket"
url: https://36kr.com/p/3706584229360004
source: 36kr.com
date: "2025"
type: blog
theme: strategies
lang: ru
---

# OpenClaw: AI Bot на Polymarket (Russian/Chinese summary)

## Ключевые результаты / Key Results

- Аккаунт "0x8dxd": более 20,000 сделок, прибыль свыше **$1.7 млн**
- Weather bot: $1,000 → $24,000 менее чем за год
- Claude Sonnet 3.7: **20.54% кумулятивная доходность** за 50 торговых дней, максимальная просадка 10.65%

## Торговые стратегии / Trading Strategies

### Математический арбитраж
Exploit price discrepancies between Yes/No contract pairs. When YES+NO < $1.00, simultaneous purchase guarantees profit.

### Высокочастотная торговля на волатильных рынках
Trading on volatile crypto prediction markets using rapid buy-sell spread capture.

### Маркет-мейкинг
Rapid buy-sell spread capture without directional bets.

## Почему ИИ работает / Why AI Works

LLMs like Claude Sonnet have superior reasoning abilities to assess probabilities from scattered information. After executing trade, AI evaluates:
- Is this market correctly priced?
- What new information has emerged?
- What is the probability-weighted expected value?

Expected flow: LLMs handle judgment (compress scattered information into probability conclusions) → OpenClaw-type tools handle execution (convert conclusions into actual orders and position management).

## Предупреждения / Warnings

- Prediction accuracy does not guarantee profit
- Models struggle with sudden information shocks
- As more bots flood markets, arbitrage windows narrow considerably
- Security risks when granting trading permissions (private key exposure)
- "Humans must bear the consequences themselves"

## Как работает OpenClaw / How OpenClaw Works

OpenClaw runs a persistent daemon 24/7, monitors markets, sends trading signals to WhatsApp, Telegram, or Discord in real time. Designed to work with AI models for autonomous execution.

## Конкурентная среда / Competitive Landscape

Professional HFT systems dominate:
- Rust/C++ core for memory safety and execution speed
- Servers co-located near network backbone data centers
- Sub-millisecond latency
- Institutional capital

Individual traders can still participate but need specialization advantage over raw speed.
