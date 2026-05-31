---
title: "al1enjesus/polymarket-whales - Whale Trade Tracker CLI"
url: "https://raw.githubusercontent.com/al1enjesus/polymarket-whales/main/README.md"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

# polymarket-whales

CLI tool that monitors the Polymarket CLOB API and fires an alert the moment a trade above a configurable threshold hits the books. No API keys, no sign-up, no infrastructure. Just Python.

MIT license. By Virixlabs.

## Setup (3 commands)

```bash
git clone <repo>
cd polymarket-whales
pip install -r requirements.txt
python main.py
```

## Configuration (.env or config.yaml)

- `MIN_TRADE_SIZE=500` (USD, only alert above this)
- `CHECK_INTERVAL=30` (seconds between polls)
- Optional: Telegram webhook
- Optional: Discord webhook

## Key Features

- Real-time API polling
- Color-coded terminal output
- Trade deduplication
- CSV/JSON export
- Graceful error handling
- No database or Docker required

## Community

Telegram channel: @polymarketwhales_ai — live whale trade feeds without self-hosting.

GitHub: https://github.com/al1enjesus/polymarket-whales
