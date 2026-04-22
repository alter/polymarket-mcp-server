# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Polymarket MCP Server exposes 45 tools via the Model Context Protocol (MCP) that let Claude Desktop autonomously trade, analyze, and manage positions on Polymarket prediction markets. It communicates with Claude Desktop over stdio. Python 3.10+ is required.

## Common Commands

**Setup:**
```bash
python -m venv venv && source venv/bin/activate
pip install -e ".[dev]"
```

**Run the MCP server:**
```bash
polymarket-mcp          # or: python -m polymarket_mcp.server
DEMO_MODE=true polymarket-mcp   # read-only, no credentials needed
```

**Run the web dashboard:**
```bash
polymarket-web          # FastAPI dashboard at http://localhost:8080
```

**Tests:**
```bash
pytest                                                      # all tests
pytest -m "not integration and not slow and not real_api"  # unit tests only (fast)
pytest -m integration                                       # real API tests
pytest tests/test_issue_fixes.py                           # run a single file
pytest --cov=polymarket_mcp --cov-report=html              # with coverage (80% threshold enforced)
pytest -n auto                                             # parallel execution
```

**Lint & format:**
```bash
black src/ tests/             # format (100-char line limit)
ruff check src/ tests/ --fix  # lint with auto-fix
mypy src/ --ignore-missing-imports
bandit -r src/ -ll            # security scan
pre-commit run --all-files    # run all hooks at once
```

**Docker:**
```bash
make build    # build image
make up       # docker-compose up -d
make down
make logs
```

## Architecture

```
Claude Desktop (natural language)
        ↓  stdio / MCP Protocol
  server.py  ← MCP entry point, loads config, registers all tools
        ↓
  tools/  (5 modules, 45 tools total)
  ├── market_discovery.py   (8 tools)  – search, trending, category filters
  ├── market_analysis.py    (10 tools) – orderbook, spreads, liquidity, AI recommendations
  ├── trading.py            (12 tools) – limit/market orders, batch ops, order management
  ├── portfolio.py          (8 tools)  – positions, P&L, risk analysis
  └── realtime.py           (7 tools)  – WebSocket subscriptions, price monitoring
        ↓
  auth/client.py   – HTTP client with token-bucket rate limiting (7 endpoint categories)
  auth/signer.py   – EIP-712 order signing (L1 wallet + L2 API key)
        ↓
  Polymarket: CLOB API (orders) / Gamma API (market data) / WebSocket / Polygon chain
```

**Supporting modules:**
- `config.py` – Pydantic settings loaded from env; masks sensitive values in logs; supports DEMO_MODE
- `utils/safety_limits.py` – pre-trade validation: order size, exposure, per-market position, min liquidity, spread tolerance
- `utils/websocket_manager.py` – connection pooling, auto-reconnect
- `web/app.py` – FastAPI dashboard with Jinja2 templates

**Two operating modes:**
- **DEMO** (`DEMO_MODE=true`): read-only, fake credentials accepted, no real trades
- **Full**: requires `POLYGON_PRIVATE_KEY`, `POLYGON_ADDRESS`, and optionally API credentials (auto-generated if absent)

## Key Conventions

**No mocks in tests.** All tests hit real Polymarket APIs (Gamma, CLOB, WebSocket). Tests are marked with pytest marks: `integration`, `slow`, `real_api`, `performance`.

**Default safety limits** (all configurable via env):
- Max order: $1,000 | Max total exposure: $5,000 | Max per-market position: $2,000
- Min liquidity: $10,000 | Max spread: 5% | Confirmation threshold: $500

**Tool availability** in `server.py` is conditional: trading/portfolio tools are only registered when valid auth credentials are present.

**Async throughout** – all I/O (HTTP, WebSocket) is async. Use `pytest-asyncio` (`asyncio_mode='auto'`) for async test functions.

**Line length**: 100 characters (Black + Ruff configured).

## Workflow Rules

After every completed task, run /compact before starting the next one.

## Reporting Rules (MANDATORY for ALL strategy/portfolio status)

NEVER show PnL/ROI/WR numbers without full context. Every status report MUST include:

1. **Time window**: start date → current date, total duration (hours/days/weeks)
2. **Starting capital**: e.g. "$1,000 per strategy" or total pool size
3. **Position size**: $ per trade and max concurrent positions
4. **Total trades executed** (not just W/L)
5. **Max drawdown (max DD)** — worst peak-to-trough equity loss
6. **Mean/avg drawdown** if computable
7. **Calmar ratio** — annualized return / max DD (only meaningful with >30 days of data; otherwise state "insufficient history for Calmar")
8. **Sharpe ratio** if tick-level equity available, otherwise skip
9. **Annualized return** when duration >= 30 days, otherwise show raw-period return with explicit note

"+30%" alone is meaningless. "+30% over 5 days with starting $1000, $50/trade, max 20 positions, max DD -$80 (-8%), n=500 trades" is useful.

If metrics can't be computed due to missing equity curve / drawdown data, say so explicitly — don't silently omit.
