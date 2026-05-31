---
title: "Polymarket SDK Ecosystem Overview - Official and Community SDKs"
url: "https://docs.polymarket.com/api-reference/clients-sdks"
source: "docs.polymarket.com"
date: "2026-05-30"
type: "documentation"
theme: "tools"
lang: "en"
---

# Polymarket SDK Ecosystem Overview

## Official SDKs (V2, Current)

### Python

- **py-sdk** (new unified): https://github.com/Polymarket/py-sdk — Recommended for new projects. Combines all REST APIs and WebSockets. MIT. ~8 stars, last updated 2026-05-27.
- **py-clob-client-v2**: https://github.com/Polymarket/py-clob-client-v2 — CLOB-specific V2 client
- **py-clob-client** (archived): https://github.com/Polymarket/py-clob-client — 1.2K stars, 381 forks. Latest v0.34.6. Archived, no longer functional.
- **polymarket-us-python**: https://github.com/Polymarket/polymarket-us-python — Official SDK for Polymarket US API. Ed25519 auth. API keys from polymarket.us/developer.
- **py-builder-signing-sdk**: https://github.com/Polymarket/py-builder-signing-sdk — Builder authentication and signing

### TypeScript/JavaScript

- **ts-sdk** (new unified): https://github.com/Polymarket/ts-sdk — Recommended. Last updated 2026-05-28.
- **clob-client-v2**: https://github.com/Polymarket/clob-client-v2 — npm: `@polymarket/clob-client-v2` v1.0.6. Uses viem.
- **clob-client** (archived): https://github.com/Polymarket/clob-client — npm: `@polymarket/clob-client` v5.8.1. Legacy/deprecated.
- **polymarket-us-typescript**: https://github.com/Polymarket/polymarket-us-typescript — Official US API TypeScript SDKs. npm: `polymarket-us`.
- **polymarket-sdk** (original): https://github.com/Polymarket/polymarket-sdk — SDK for proxy wallet interactions

### Rust

- **rs-clob-client-v2** (current): https://github.com/Polymarket/rs-clob-client-v2 — V2 CLOB client
- **rs-clob-client** (archived): https://github.com/Polymarket/rs-clob-client — Archived, migrate to v2
- crates.io: `polymarket-client-sdk` v0.3

### CLI

- **polymarket-cli**: https://github.com/Polymarket/polymarket-cli — Rust CLI (brew, shell script, or build from source)

## Community SDKs

### Python

- **polymarket-apis**: https://pypi.org/project/polymarket-apis/ — Unified wrapper: CLOB, Gamma, Data, Web3, WebSocket, GraphQL. Pydantic. Python >=3.12.
- **polymarket-gamma**: https://pypi.org/project/polymarket-gamma/ — Gamma API client. Import as `py_gamma`. Sync+async.
- **pascal-labs/polymarket-sdk**: https://github.com/pascal-labs/polymarket-sdk — Production SDK with connection pooling, position manager

### TypeScript/JavaScript

- **@polybased/sdk**: https://www.npmjs.com/package/@polybased/sdk — Complete TypeScript toolkit
- **@dicedhq/polymarket**: https://jsr.io/@dicedhq/polymarket — CLOB + Gamma client
- **polymarket-data**: https://www.npmjs.com/package/polymarket-data — Public data with type safety

### AI Agent Plugins

- **@theschein/plugin-polymarket** — ElizaOS agent integration
- **@goat-sdk/plugin-polymarket** — GOAT SDK plugin

### Rust (community)

- **TechieBoy/polymarket-rs-client**: https://github.com/TechieBoy/polymarket-rs-client — Mirrors Python client API
- **tdergouzi/rs-clob-client**: https://github.com/tdergouzi/rs-clob-client — Full TypeScript port to Rust

### Data

- **bitquery/polymarket-api**: https://github.com/bitquery/polymarket-api — GraphQL SDK via Bitquery

## Key API Endpoints

| API | URL | Auth |
|-----|-----|------|
| CLOB | clob.polymarket.com | EIP-712 + HMAC |
| Gamma | gamma-api.polymarket.com | None (public) |
| Data | data-api.polymarket.com | HMAC |
| Bridge | bridge.polymarket.com | None |
| WS Market | wss://ws-subscriptions-clob.polymarket.com/ws/market | None |
| WS User | wss://ws-subscriptions-clob.polymarket.com/ws/user | API key |
| WS Sports | wss://sports-api.polymarket.com/ws | None |
| RTDS | wss://ws-live-data.polymarket.com | Optional |
