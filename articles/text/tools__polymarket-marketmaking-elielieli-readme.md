---
title: "polymarket-marketmaking - Band-Based Market Making Bot"
url: "https://raw.githubusercontent.com/elielieli909/polymarket-marketmaking/main/README.md"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

# polymarket-marketmaking

Completely automated market-making bot for Polymarket. Market making is a market-neutral trading strategy profiting by providing liquidity (placing orders above and below market price) to a CLOB marketplace.

## Configuration (bands.json)

```json
{
    "buyBands": [
        {
            "minMargin": 0.005,
            "avgMargin": 0.01,
            "maxMargin": 0.02,
            "minAmount": 20.0,
            "avgAmount": 30.0,
            "maxAmount": 40.0
        },
        {
            "minMargin": 0.02,
            "avgMargin": 0.025,
            "maxMargin": 0.05,
            "minAmount": 40.0,
            "avgAmount": 60.0,
            "maxAmount": 80.0
        }
    ],
    "buyLimits": [],
    "sellBands": [ ... ],
    "sellLimits": []
}
```

`buyBands`/`sellBands` = areas in the order book surrounding market price where resting orders are placed. `margins` = percent offset from market price.

## Example

Token trading at 0.50 USDC → 4 bands created:
1. Avg bid size 30 tokens between prices 0.4975–0.49 USDC
2. Avg bid size 60 tokens between 0.49–0.475 USDC
3. Avg offer size 30 tokens between 0.5025–0.51 USDC
4. Avg offer size 60 tokens between 0.51–0.525 USDC

## Logic

Every second: read market price → decide to cancel or place new orders.

### Cancel logic

1. Cancel orders outside any band
2. If a band's maxAmount breached:
   - Inner band: cancel orders closest to market price
   - Outer band: cancel orders furthest from market price
   - Middle band: cancel by size (smallest first)

### Add logic

For each band: if sum of order sizes < avgAmount → place new order at avgMargin.

GitHub: https://github.com/elielieli909/polymarket-marketmaking
