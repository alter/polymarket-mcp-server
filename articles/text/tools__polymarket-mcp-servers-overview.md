---
title: "Polymarket MCP Servers for Claude - Overview"
url: "https://github.com/topics/polymarket"
source: "github.com"
date: "2026-05-30"
type: "overview"
theme: "tools"
lang: "en"
---

# Polymarket MCP Servers for Claude

Multiple open-source MCP (Model Context Protocol) servers enabling Claude to interact with Polymarket.

## 1. caiovicentino/polymarket-mcp-server

URL: https://github.com/caiovicentino/polymarket-mcp-server

Most feature-rich. 45 tools total:
- Market discovery (8 tools): search, trending, category filters
- Trading engine (12 tools): limit/market orders, batch ops, order management
- Market analysis (10 tools): orderbook, spreads, liquidity, AI recommendations
- Portfolio management (8 tools): positions, P&L, risk analysis
- Real-time (7 tools): WebSocket subscriptions, price monitoring

Features:
- Web dashboard (FastAPI + Jinja2) at http://localhost:8080
- DEMO mode (read-only, no credentials)
- Safety limits: max order $1,000, max exposure $5,000, max per-market $2,000, min liquidity $10,000, max spread 5%
- EIP-712 order signing
- Token-bucket rate limiting (7 endpoint categories)

## 2. guangxiangdebizi/PolyMarket-MCP

URL: https://github.com/guangxiangdebizi/PolyMarket-MCP

Comprehensive access to Polymarket APIs for AI assistants:
- Get Markets, Get Events
- Market Prices (real-time and historical)
- Order Book (live bid/ask)
- Trade History
- User Positions with P&L calculations
- Market Holders ownership analysis

## 3. berlinbra/polymarket-mcp

URL: https://github.com/berlinbra/polymarket-mcp

Four core tools:
- get-market-info
- list-markets
- get-market-prices
- get-market-history

## 4. fernandezpablo85/polymarket-mcp

URL: https://github.com/fernandezpablo85/polymarket-mcp

Lightweight Claude Desktop integration. Requires Claude Desktop App for Mac, Python 3.8+. Configured via `claude_desktop_config.json`.

## 5. amolkodan/polymarket-claude-mcp

URL: https://lobehub.com/mcp/amolkodan-polymarket-claude-mcp

Lets Claude: search markets, read order books/prices, place/cancel GTC limit orders, inspect USDC balance, positions, trade history on Polygon. Usable via Claude Desktop (local stdio) or Claude web/mobile (remote HTTPS endpoint).

## 6. ozgureyilmaz/polymarket-mcp

URL: https://github.com/ozgureyilmaz/polymarket-mcp

MCP server + CLI. Multiple output formats (JSON, pretty-printed, clean tables). Built-in smart caching with auto-retry logic.
