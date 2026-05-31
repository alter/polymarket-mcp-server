---
title: "Polymarket API: Python SDK, боты и документация (обзор на русском)"
url: "https://github.com/polymarket"
source: "github.com"
date: "2026-05-30"
type: "overview"
theme: "tools"
lang: "ru"
---

# Polymarket API: Python SDK и боты — обзор на русском

## Официальные Python SDK

### py-clob-client (архивирован)

```bash
pip install py-clob-client
```

```python
from py_clob_client.client import ClobClient
client = ClobClient("https://clob.polymarket.com", key=PRIVATE_KEY, chain_id=137)
client.set_api_creds(client.create_or_derive_api_creds())
```

Мигрируйте на: https://github.com/Polymarket/py-sdk

### py-clob-client-v2 (текущий)

Новый унифицированный SDK. Рекомендуется использовать `Polymarket/py-sdk` для новых проектов. Поддерживает лимитные и рыночные ордера на Polygon (chain_id=137) или тестовую сеть Amoy.

### polymarket-us-python

Официальный Python SDK для Polymarket US API. Ed25519 подписи. API-ключи генерируются на polymarket.us/developer.

## Типы подписей (signature_type)

- `0` — EOA (MetaMask, аппаратные кошельки)
- `1` — Email/Magic wallet
- `2` — Browser wallet proxy (Gnosis Safe)
- `3` — EIP-1271 smart contract wallet (рекомендуется для новых пользователей)

## Аутентификация (2 уровня)

**L1** — EIP-712 подпись приватным ключом → получение API credentials  
**L2** — HMAC-SHA256 подписи с полученными credentials → торговые запросы

## Основные API эндпоинты

| API | URL | Авторизация |
|-----|-----|-------------|
| CLOB | clob.polymarket.com | EIP-712 + HMAC |
| Gamma | gamma-api.polymarket.com | Не требуется |
| Data | data-api.polymarket.com | HMAC |

## WebSocket каналы

- Market: `wss://ws-subscriptions-clob.polymarket.com/ws/market` — orderbook, цены (без авторизации)
- User: тот же хост `/ws/user` — заявки пользователя (требует API key)
- Sports: `wss://sports-api.polymarket.com/ws` — спортивные результаты (без авторизации)
- RTDS: `wss://ws-live-data.polymarket.com` — крипто-цены Binance/Chainlink

## Боты на GitHub

- **Polymarket/agents** — официальный AI-агент фреймворк (MIT)
- **aulekator/Polymarket-BTC-15-Minute-Trading-Bot** — 7-фазная архитектура для BTC 15-мин маркетов
- **GiordanoSouza/polymarket-copy-trading-bot** — копитрейдинг с Python + Supabase
- **MrFadiAi/Polymarket-bot** — 4 стратегии, Smart Money Filtering (60%+ winrate)
- **warproxxx/poly-maker** — маркет-мейкинг с Google Sheets конфигурацией
- **al1enjesus/polymarket-whales** — отслеживание крупных сделок в терминале
- **WrBug/PolyHermes** —跟单 (копитрейдинг), Docker, мультиаккаунт

## CLOB V2 (апрель 2026)

Критические изменения для разработчиков:
- Старые SDK (py-clob-client, @polymarket/clob-client) больше не работают с продакшном
- Новый стейблкоин: **pUSD** (заменяет USDC.e)
- Builder attribution через `builderCode` в структуре ордера
- Лимиты: POST /order — 5,000/10s burst + 48,000/10min sustained

## Официальная документация

https://docs.polymarket.com  
GitHub организация: https://github.com/polymarket (100+ репозиториев)
