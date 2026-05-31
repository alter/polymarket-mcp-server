---
title: "Betfair Python Trading Bot — Green Book/Scalping Strategy"
url: "https://github.com/michaelvrxoj/betfair-python-trading-bot-automation"
source: github
date: "2025-2026"
type: tool
theme: platforms
lang: en
---

# Betfair Python Trading Bot — Green Book/Scalping Strategy

GitHub: michaelvrxoj/betfair-python-trading-bot-automation

## Strategy
"Green Book" / Scalping — secure profits before race starts by:
1. Automating entry into markets
2. Executing a tick-offset profit
3. "Greening Up" (hedging) to guarantee profit regardless of race outcome

## Execution Flow
1. Connect to Betfair Stream API
2. Monitor live market data
3. When trade condition met (e.g., weight of money crosses threshold), trigger bet
4. Process market data, calculate optimal trade actions
5. Place bets using Back-to-Lay or Lay-to-Back strategy
6. Green Up positions to lock in profits before race start
7. Log completed trade, send notifications (Email/Telegram)

## Bot Architecture
Three main parts:
1. **Trading & Help Routines**: Functions like `get_anchor_price`, `send_to_telegram`
2. **Strategy**: Python class with methods `check_market_book`, `process_market_book`, `process_orders`
3. Risk management: stop-loss limits, time-to-jump exit rules, fill-or-kill order management

## Performance
Execution speed: under 100ms latency — reacts in real-time to market conditions.

## API Notes
- **Streaming API**: Real-time pricing (recommended)
- **Polling REST API**: Slower, but includes metadata (runner names, events)
- "Delayed" API key: free for testing (1–60 second delay)
- "Live" API key: requires funded account + one-time £299 activation fee

## Tools Used
- Python 3.10+
- `betfairlightweight` library
- `Flumine` framework for strategy execution
