---
title: "Migrating to Polymarket CLOB V2 - Official Documentation"
url: "https://docs.polymarket.com/v2-migration"
source: "docs.polymarket.com"
date: "2026-05-30"
type: "documentation"
theme: "tools"
lang: "en"
---

# Polymarket CLOB V2 Migration Guide

CLOB V2 went live April 28, 2026. V1 SDKs no longer work against production.

## What Changed

### Order Struct

V1 removed fields: `nonce`, `feeRateBps`, `taker`
V2 added fields: `timestamp`, `metadata`, `builder`

- Order uniqueness now from **timestamp (milliseconds)** — nonce system removed
- Fees collected on-chain at match time (not embedded in signed order)
- Builder attribution via `builderCode` field in order struct (replaces HMAC builder headers)

### EIP-712

Exchange domain version bumped from `"1"` to `"2"`.

### Collateral

New collateral: **pUSD** — ERC-20 backed 1:1 by USDC, replacing USDC.e.

### Builder Auth

V2 replaces old builder authentication (HMAC headers + separate signing SDK) with native builder code attached directly to each order.

`builderCode` is a `bytes32` identifier found at `polymarket.com/settings?tab=builder`. No HMAC signing, no separate API key, no special headers — just pass `builderCode` in every order struct.

### Rate Limits

POST /order: 5,000/10s burst + 48,000/10min sustained (updated)

## V2 SDK Packages

| Language | Package |
|----------|---------|
| TypeScript | `@polymarket/clob-client-v2` |
| Python | `py-clob-client-v2` |
| Rust | `polymarket_client_sdk_v2` (via `rs-clob-client-v2`) |

## Liquidity Rewards Program

$1M liquidity rewards program alongside V2 launch.

- Rewards: paid USDC for placing competitive bids
- Based on: size of orders, how close to midpoint, consistency of quoting relative to other LPs
- Minimum active time on book: **3.5 seconds** to be eligible

## New Builder Leaderboard Endpoints

- `GET /v1/builders/leaderboard` — includes `builderCode` string on each entry
- `GET /v1/builders/volume` — includes `builderCode` string on each entry
