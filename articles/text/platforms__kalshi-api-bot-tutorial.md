---
title: "Kalshi API & Trading Bot Tutorial (2026, Python)"
url: "https://www.alphascope.app/blog/kalshi-api"
source: alphascope.app
date: "2026"
type: tutorial
theme: platforms
lang: en
---

# Kalshi API & Trading Bot Tutorial

## Authentication
API key authentication with Bearer tokens:
- Header: `"Authorization": "Bearer YOUR_API_KEY"`
- Trading operations require RSA private key signing
- Obtain credentials: Settings → API in Kalshi account

## Key Endpoints

### Public
- `GET /markets` — all available markets
- `GET /markets/{ticker}` — specific market
- `GET /markets/{ticker}/orderbook` — order book
- `GET /markets/{ticker}/trades` — recent trades

### Authenticated
- `POST /orders` — submit orders
- `DELETE /orders/{order_id}` — cancel orders
- `GET /orders` — list open positions
- `GET /positions` — current holdings
- `GET /balance` — account balance
- `GET /fills` — trade execution history

## Rate Limits
- Public endpoints: ~10 requests/second
- Authenticated endpoints: ~5 requests/second
- Exceeding limits: 429 error — implement exponential backoff

## Python Example
```python
import requests

BASE_URL = "https://trading-api.kalshi.com/trade-api/v2"
response = requests.get(f"{BASE_URL}/markets")
markets = response.json()

for market in markets["markets"][:5]:
    print(f"{market['ticker']}: {market['title']}")
```

## Common Bot Patterns
- Market making (bid/ask orders, profit from spread)
- Arbitrage monitoring (Kalshi vs Polymarket)
- News-triggered trading
- Portfolio automation
- Historical data collection for backtesting

## Best Practices
- Paper trading before real capital
- Comprehensive error handling
- Detailed logging
- Respect rate limits
- Continuous monitoring with anomaly alerts
