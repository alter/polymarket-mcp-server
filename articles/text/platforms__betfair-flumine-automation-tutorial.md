---
title: "How to Automate Betfair Trading with Flumine — Part 1"
url: "https://betfair-datascientists.github.io/tutorials/How_to_Automate_1/"
source: betfair-datascientists.github.io
date: "2025"
type: tutorial
theme: platforms
lang: en
---

# How to Automate Betfair Trading with Flumine — Part 1

Official Betfair data scientists tutorial on the Flumine framework.

## Understanding Betfair APIs

**Rest API (Polling):** Provides snapshots requiring new requests for updates; includes market catalogue data (runner names).

**Push API (Streaming):** Delivers real-time pricing via continuous connection; lacks catalogue information.

Flumine combines both automatically — real-time prices plus essential metadata without manual integration.

## Flumine Architecture
1. Login with credentials
2. Create strategy as Python class
3. Define market/sport filters
4. Add strategy to framework
5. Execute framework

## Strategy Implementation
Core strategy extends `BaseStrategy`. Key methods:
- **start()**: Executes once when strategy begins
- **check_market_book()**: Returns True to process markets; filters closed markets
- **process_market_book()**: Runs when check_market_book() returns True; handles primary betting logic

## Code Structure Example
```python
from flumine import Flumine, clients
from flumine.order.trade import Trade
from flumine.order.order import LimitOrder

class LayStrategy(BaseStrategy):
    def check_market_book(self, market, market_book):
        if market_book.status != "CLOSED":
            return True

    def process_market_book(self, market, market_book):
        for runner in market_book.runners:
            if runner.status == "ACTIVE" and runner.ex.available_to_lay:
                trade = Trade(market_id=market_book.market_id, ...)
                # place lay at 1.01 odds, $5 stake
```

## Market Filtering
Uses `streaming_market_filter()` to specify event types, countries, market types, and optional market IDs.

## Key Notes
- Betfair documentation uses camelCase (marketId); Flumine follows Python conventions (market_id)
- Market catalogue data returns `None` initially before polling API retrieves it
- Enable logging for debugging: two lines of code captures comprehensive error info

## Series Roadmap
- Part II: Backing/laying nth favorites
- Part III: Automating Betfair data science models
- Part IV: Custom model automation
- Part V: Exchange simulation and backtesting
