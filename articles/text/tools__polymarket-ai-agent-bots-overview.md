---
title: "Polymarket AI Agent & LLM Trading Frameworks - GitHub Overview"
url: "https://github.com/topics/prediction-markets?l=python"
source: "github.com"
date: "2026-05-30"
type: "overview"
theme: "tools"
lang: "en"
---

# Polymarket AI Agent & LLM Trading Frameworks

## 1. Polymarket/agents (Official)

URL: https://github.com/Polymarket/agents
License: MIT

Official developer framework for building AI agents that autonomously trade on Polymarket. Modular components for community maintenance.

Modules:
- `gamma.py` — `GammaMarketClient` for market/event metadata
- `polymarket.py` — DEX interaction, order building, signing
- `cli.py` — Main user interface with commands like `get-all-markets`

## 2. Dhaiwat10/polymarket-ai — AI Agents Battle Arena

URL: https://github.com/Dhaiwat10/polymarket-ai

Multi-agent trading arena: multiple AI agents (OpenAI, Claude) compete simultaneously with independent portfolios. Real-time equity charts, news gathering, live dashboard.

## 3. polymarket-trading-ai-agent/polymarket-trading-ai-agent

URL: https://github.com/polymarket-trading-ai-agent/polymarket-trading-ai-agent

Autonomous trading agent powered by multiple LLMs (ChatGPT, DeepSeek, Claude, Gemini, Grok). Uses Kelly Criterion for position sizing, expected value analysis, news sentiment parsing.

## 4. xiaods/poly-agents

URL: https://github.com/xiaods/poly-agents

Fork of Polymarket/agents using minimax m2.

## 5. luuisotorres/polymarket-intelligence

URL: https://github.com/luuisotorres/polymarket-intelligence

Real-time market tracking dashboard: live price charts, news feeds, whale order tracking, price movement analysis. Multi-agent AI debate system (specialized agents analyze market from different perspectives). Stack: LangGraph, LangChain with Google Generative AI, Tavily, FastAPI, React 18, SQLite.

## 6. TauricResearch/TradingAgents

URL: https://github.com/tauricresearch/tradingagents

Multi-agent LLM financial trading framework. Specialized agents: fundamental analysts, sentiment experts, technical analysts, trader + risk management teams.

- LLM-agnostic: GPT-4, Claude, Gemini, DeepSeek, Grok, local Ollama
- Orchestration: LangGraph
- v0.2.5 (May 2026): Qwen/GLM/MiniMax support, remote Ollama, non-US alpha benchmarks

## Common Technology Stack

| Component | Tools |
|-----------|-------|
| LLMs | GPT-4/5, Claude, Gemini, DeepSeek, Grok |
| Orchestration | LangChain, LangGraph |
| Data/RAG | ChromaDB, NewsAPI, Tavily, Exa API |
| APIs | Polymarket Gamma API, CLOB |
| Infra | Docker, FastAPI, Python 3.12+, MongoDB |

## Key Prediction Market Dataset

**Jon-Becker/prediction-market-analysis** (2.3K stars): 36GB of historical data from Polymarket and Kalshi. Shows Polymarket is well-calibrated on high-liquidity markets (events priced at 70% happen ~70% of the time) but miscalibrated on thin markets.
