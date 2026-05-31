---
title: "SII-WANGZJ/Polymarket_data - 1.1B Record Historical Dataset"
url: "https://raw.githubusercontent.com/SII-WANGZJ/Polymarket_data/main/README.md"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

# Polymarket Data (SII-WANGZJ)

Comprehensive toolkit and dataset for Polymarket prediction markets. Created by researchers from Shanghai Innovation Institute.

## Scale

- 107GB of trading data
- 1.1 billion records
- 268K+ markets
- All historical data before 2026 available for download from HuggingFace

## Key Features

### Data Access

- Direct fetching from Polygon blockchain and Gamma API
- Five analysis-ready datasets in parquet format
- Includes: `block_number`, `contract name`, `maker_fee / taker_fee / protocol_fee`, `order_hash` — details typically unavailable from third-party sources

### Operational Modes

- Continuous real-time fetching (updates every 2 seconds)
- Batch historical data retrieval
- Complete pipeline combining market metadata and on-chain processing

### Data Quality

- Covers all `OrderFilled` events from two official exchange contracts
- Regular automated updates
- Verification against Polygon RPC nodes

## Core Datasets (5 parquet files)

1. Raw blockchain events
2. Processed trades with market linkage
3. Market metadata
4. Normalized market data with unified perspective
5. User behavior records split by trading role

## Installation & Usage

```bash
pip install polymarket-data-toolkit
```

Data downloads: HuggingFace. Or use the toolkit to fetch latest data yourself.

Supports Python APIs, CLI commands, and shell scripts. Graceful shutdown ensures data integrity. Utility tools for merging and sorting parquet files.

## Use Cases

- Market research
- Behavioral studies
- Quantitative analysis

License: MIT

GitHub: https://github.com/SII-WANGZJ/Polymarket_data
