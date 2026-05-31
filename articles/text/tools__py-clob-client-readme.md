---
title: "Polymarket Python CLOB Client (py-clob-client) - README"
url: "https://raw.githubusercontent.com/Polymarket/py-clob-client/main/README.md"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

> WARNING: This repository has been archived and is no longer maintained.
> The client is no longer functional and should not be used for new or existing integrations.
> Please migrate to our new unified SDK: https://github.com/Polymarket/py-sdk

## Overview

Python client for trading on Polymarket's Central Limit Order Book (CLOB). The package supports various wallet types including EOA wallets, email/Magic wallets, and proxy wallets.

## Installation

Requires Python 3.9+:

```bash
pip install py-clob-client
```

## Core Usage Patterns

### Read-Only Access

```python
from py_clob_client.client import ClobClient

client = ClobClient("https://clob.polymarket.com")
ok = client.get_ok()
time = client.get_server_time()
```

### Trading Setup

Two authentication approaches:
- **Standard EOA** (signature_type=0): direct private key via MetaMask or hardware wallets.
- **Delegated Signatures**: signature_type=1 for email/Magic wallets, type=2 for browser wallet proxies.

Both require a funder address and private key.

### Market Data Operations

```python
token_id = "<token-id>"
mid = client.get_midpoint(token_id)
price = client.get_price(token_id, side="BUY")
book = client.get_order_book(token_id)
```

### Order Placement

- **Market Orders**: Execute immediately at available prices for a specified dollar amount.
- **Limit Orders**: Execute at a specified price for a given share quantity, persist until filled or cancelled.

### Trading Constraints

MetaMask/hardware wallet users must approve USDC (`0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`) and Conditional Tokens (`0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`) for three exchange contracts. Email/Magic wallet users do not require manual allowance configuration.

## Market Discovery

Token identifiers available through the Markets API Explorer at docs.polymarket.com. Prices range 0.00–1.00 (dollars), quantities can be whole or fractional.
