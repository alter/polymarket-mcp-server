---
title: "Polymarket Agents - AI Trading Framework README"
url: "https://raw.githubusercontent.com/Polymarket/agents/main/README.md"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

# Polymarket Agents

Official developer framework for building AI agents that autonomously trade on Polymarket prediction markets. MIT-licensed, publicly available.

## Core Features

- Integration with Polymarket API
- AI agent utilities for prediction markets
- Local and remote RAG support
- Data sourcing from betting services and news providers
- Comprehensive LLM tools

## Technical Setup

Requires Python 3.9. Clone repo, create virtualenv, install dependencies, configure environment variables (wallet private key, API credentials).

## Architecture Components

- **Chroma**: Vector database for RAG
- **Gamma** (`agents/polymarket/gamma.py`): Market metadata retrieval via `GammaMarketClient` class — interfaces with Polymarket Gamma API to fetch/parse market and event metadata, retrieve current and tradable markets
- **Polymarket** (`agents/polymarket/polymarket.py`): DEX interaction, order building, signing, API test examples

## Primary Interface

`cli.py` serves as the main user interface, supporting commands like `get-all-markets` with optional limit and sort parameters.

## Legal Notice

Terms of Service prohibit US persons and persons from certain other jurisdictions from trading on Polymarket (via UI & API and including agents developed by persons in restricted jurisdictions), though data and information are viewable globally.

## GitHub

https://github.com/Polymarket/agents
