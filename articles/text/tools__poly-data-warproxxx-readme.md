---
title: "warproxxx/poly_data - Polymarket v2 On-Chain Data Pipeline"
url: "https://raw.githubusercontent.com/warproxxx/poly_data/main/README.md"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

# poly_data

Python pipeline for fetching and analyzing Polymarket v2 trading data. Reads order events directly from the Polymarket CTF Exchange V2 contract on Polygon via JSON-RPC, combines them with market metadata from the Polymarket Gamma API, and outputs structured trades to CSV.

Replaced previous subgraph-based approach after Polymarket migrated contracts in April 2026. Reads data directly from blockchain to avoid third-party indexing dependency.

## Key Components (run by update.py)

1. **Markets** — Retrieves all Polymarket markets via the Gamma keyset API (`/markets/keyset`), resumable from a saved cursor. Subsequent runs only fetch newly created markets.
2. **Chain** — Extracts `OrderFilled` events from CTF Exchange V2 contract using direct JSON-RPC calls. Resumable from last scanned block.
3. **Process** — Joins order events with market data to generate labeled trades with pricing and direction.

Stages 1 and 2 execute in parallel.

## Configuration

- `POLYGON_RPC_URL`: JSON-RPC endpoint (defaults to public node)
- `POLYGON_MAX_BLOCK_RANGE`: Query window size (increase for paid RPC plans)
- `PROCESS_CHUNK_SIZE`: Enables streaming for memory-constrained systems

## Installation & Usage

```bash
uv sync
uv run python update.py
```

First execution: several hours. Subsequent runs: seconds.

## Output Files

- `data/markets.csv` — Complete market listing with all API fields
- `data/orderFilled.csv` — Raw chain events with timestamps and participant details
- `processed/trades.csv` — Enriched trades: price, USD value, BUY/SELL direction

## Important Notes

To filter trades for a specific user: filter on `maker` field, not `taker`.

License: GPL-3.0

GitHub: https://github.com/warproxxx/poly_data
