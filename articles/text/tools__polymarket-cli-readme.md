---
title: "Polymarket CLI - Official Rust CLI README"
url: "https://raw.githubusercontent.com/Polymarket/polymarket-cli/main/README.md"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

# Polymarket CLI

Rust CLI for Polymarket. Browse markets, place orders, manage positions, and interact with onchain contracts — from a terminal or as a JSON API for scripts and agents.

> Warning: Early, experimental software. Do not use with large funds. APIs and behavior may change.

## Install

```bash
# Homebrew (macOS/Linux)
brew tap Polymarket/polymarket-cli https://github.com/Polymarket/polymarket-cli
brew install polymarket

# Shell script
curl -sSL https://raw.githubusercontent.com/Polymarket/polymarket-cli/main/install.sh | sh

# Build from source
git clone https://github.com/Polymarket/polymarket-cli
cd polymarket-cli
cargo install --path .
```

## Quick Start

```bash
# No wallet needed — browse markets immediately
polymarket markets list --limit 5
polymarket markets search "election"
polymarket events list --tag politics

# Check a specific market
polymarket markets get will-trump-win-the-2024-election

# JSON output for scripts
polymarket -o json markets list --limit 3
```

## Configuration

```bash
polymarket wallet create       # Generate new random wallet
polymarket wallet import 0xKEY...
polymarket approve set         # Approve ERC-20 (pUSD) + ERC-1155 (CTF tokens)
```

Config file: `~/.config/polymarket/config.json`

Signature types: `proxy` (default), `eoa`, `gnosis-safe`.

## Commands

### Markets & Events

```bash
polymarket markets list --active true --order volume_num
polymarket markets get 12345
polymarket markets search "bitcoin"
polymarket events list --tag politics --active true
```

### Order Book & Prices (no wallet needed)

```bash
polymarket clob price TOKEN_ID --side buy
polymarket clob midpoint TOKEN_ID
polymarket clob spread TOKEN_ID
polymarket clob book TOKEN_ID
polymarket clob price-history TOKEN_ID --interval 1d --fidelity 30
polymarket clob tick-size TOKEN_ID
```

Interval options: `1m`, `1h`, `6h`, `1d`, `1w`, `max`

### Trading (authenticated)

```bash
# Limit order
polymarket clob create-order --token TOKEN_ID --side buy --price 0.50 --size 10

# Market order
polymarket clob market-order --token TOKEN_ID --side buy --amount 5

# Batch
polymarket clob post-orders --tokens "T1,T2" --side buy --prices "0.40,0.60" --sizes "10,10"

# Cancel
polymarket clob cancel ORDER_ID
polymarket clob cancel-all

# Portfolio
polymarket clob orders
polymarket clob trades
polymarket clob balance --asset-type collateral
```

Order types: `GTC` (default), `FOK`, `GTD`, `FAK`. Add `--post-only` for limit orders.

### Data (public, no wallet)

```bash
polymarket data positions 0xWALLET
polymarket data trades 0xWALLET --limit 50
polymarket data leaderboard --period month --order-by pnl --limit 10
polymarket data builder-leaderboard --period week
```

### Rewards & API Keys

```bash
polymarket clob rewards --date 2024-06-15
polymarket clob earnings --date 2024-06-15
polymarket clob order-scoring ORDER_ID
polymarket clob create-api-key
```

### CTF Operations

```bash
polymarket ctf split --condition 0xCONDITION --amount 10
polymarket ctf merge --condition 0xCONDITION --amount 10
polymarket ctf redeem --condition 0xCONDITION
```

### Bridge

```bash
polymarket bridge deposit 0xWALLET        # Get EVM, Solana, Bitcoin deposit addresses
polymarket bridge supported-assets
```

### Interactive Shell

```bash
polymarket shell
# polymarket> markets list --limit 3
# polymarket> clob book TOKEN_ID
# polymarket> exit
```

### Other

```bash
polymarket status
polymarket setup    # Guided first-time setup wizard
polymarket upgrade
```

## Script Integration (JSON)

```bash
polymarket -o json markets list --limit 100 | jq '.[].question'
polymarket -o json clob midpoint TOKEN_ID | jq '.mid'
```

## Architecture

```
src/
  main.rs        -- CLI entry point, clap parsing
  auth.rs        -- Wallet resolution, RPC provider, CLOB authentication
  config.rs      -- Config file (~/.config/polymarket/config.json)
  shell.rs       -- Interactive REPL
  commands/      -- One module per command group
  output/        -- Table and JSON rendering
```

License: MIT
