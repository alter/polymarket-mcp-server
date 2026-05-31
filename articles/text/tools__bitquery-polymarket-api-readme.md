---
title: "bitquery/polymarket-api - GraphQL SDK for Polymarket on-chain data"
url: "https://raw.githubusercontent.com/bitquery/polymarket-api/main/README.md"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

# polymarket-api (Bitquery)

Node.js npm package for querying Polymarket prediction market data using Bitquery's GraphQL APIs. Provides access to new prediction markets, resolved predictions, position tokens, trading data, and real-time trade streams from Polymarket on Polygon blockchain.

## Installation

```bash
npm install polymarket-api
```

Requires a Bitquery OAuth token (free account at ide.bitquery.io).

## Query Functions

- `getNewQuestions(token, count)` — Get latest new prediction markets
- `getResolvedQuestions(token, count)` — Track resolved predictions and outcomes
- `getPositionSplits(token, count)` — Monitor position tokens and splits
- `getPayoutRecieved(token, address, count)` — Payout events for a specific address
- `getAllTrades(token, count)` — All USDC-based trades from Polymarket CTF exchange
- `getTradesByAddress(token, address, count)` — Trades for a specific position token/market
- `getTradesByUser(token, userAddress, count)` — Trades by a specific user

## Streaming Functions

- `streamNewQuestions(token, options)` — Real-time new markets
- `streamResolvedQuestions(token, options)` — Real-time resolutions
- `streamPositionSplits(token, options)` — Real-time position splits
- `streamPayoutRecieved(token, address, options)` — Real-time payouts
- `streamAllTrades(token, options)` — All live Polymarket trades
- `streamTradesByAddress(token, address, options)` — Live trades for a market
- `streamTradesByUser(token, userAddress, options)` — Live trades for a user

## Quick Start

```javascript
import { getNewQuestions, streamAllTrades } from 'polymarket-api';

const token = 'your-bitquery-oauth-token';

// Get latest new markets
const newMarkets = await getNewQuestions(token, 10);

// Stream all live trades
streamAllTrades(token, {
    onData: (trade) => {
        console.log('New trade:', trade.Trade.AmountInUSD);
    }
});
```

## About Polymarket Infrastructure

- Conditional Tokens Framework (CTF): ERC-1155 tokens representing market positions
- UMA Optimistic Oracle: Decentralized market resolution
- CTF Exchange: Trading venue for position tokens
- All on Polygon blockchain

Documentation: https://docs.bitquery.io/docs/examples/polymarket-api/

License: ISC
