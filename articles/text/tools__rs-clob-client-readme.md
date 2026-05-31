---
title: "Polymarket Rust CLOB Client (rs-clob-client) - README"
url: "https://raw.githubusercontent.com/Polymarket/rs-clob-client/main/README.md"
source: "github.com"
date: "2026-05-30"
type: "readme"
theme: "tools"
lang: "en"
---

> WARNING: This repository has been archived and is no longer maintained.
> Please migrate to the V2 client: https://github.com/Polymarket/rs-clob-client-v2

# Polymarket Rust Client

An ergonomic Rust client for interacting with Polymarket services, primarily the Central Limit Order Book (CLOB).
This crate provides strongly typed request builders, authenticated endpoints, `alloy` support and more.

## Feature Flags

| Feature      | Description |
|--------------|-------------|
| `clob`       | Core CLOB client for order placement, market data, and authentication |
| `tracing`    | Structured logging for HTTP requests, auth flows, and caching |
| `ws`         | WebSocket client for real-time orderbook, price, and user event streaming |
| `rtds`       | Real-time data streams for crypto prices (Binance, Chainlink) and comments |
| `data`       | Data API client for positions, trades, leaderboards, and analytics |
| `gamma`      | Gamma API client for market/event discovery, search, and metadata |
| `bridge`     | Bridge API client for cross-chain deposits (EVM, Solana, Bitcoin) |
| `rfq`        | RFQ API for submitting and querying quotes |
| `heartbeats` | Automatic heartbeat messages; if client disconnects all open orders cancelled |
| `ctf`        | CTF API client to perform split/merge/redeem on binary and neg risk markets |

## Getting Started

```toml
[dependencies]
polymarket-client-sdk = "0.3"
```

## Unauthenticated (read-only)

```rust
use polymarket_client_sdk::clob::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::default();
    let ok = client.ok().await?;
    Ok(())
}
```

## Authenticated EOA

```rust
let private_key = std::env::var(PRIVATE_KEY_VAR).expect("Need a private key");
let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
let client = Client::new("https://clob.polymarket.com", Config::default())?
    .authentication_builder(&signer)
    .authenticate()
    .await?;
```

## Proxy/Safe Wallets

Funder address automatically derived using CREATE2 from signer's EOA address:

```rust
let client = Client::new("https://clob.polymarket.com", Config::default())?
    .authentication_builder(&signer)
    .signature_type(SignatureType::GnosisSafe)
    .authenticate()
    .await?;
```

## Place Market Order

```rust
let order = client
    .market_order()
    .token_id("<token-id>")
    .amount(Amount::usdc(Decimal::ONE_HUNDRED)?)
    .side(Side::Buy)
    .order_type(OrderType::FOK)
    .build()
    .await?;
let signed_order = client.sign(&signer, order).await?;
let response = client.post_order(signed_order).await?;
```

## Place Limit Order

```rust
let order = client
    .limit_order()
    .token_id("<token-id>")
    .size(Decimal::ONE_HUNDRED)
    .price(dec!(0.1))
    .side(Side::Buy)
    .build()
    .await?;
let signed_order = client.sign(&signer, order).await?;
let response = client.post_order(signed_order).await?;
```

## WebSocket Streaming

```rust
use polymarket_client_sdk::clob::ws::Client;

let client = Client::default();
let asset_ids = vec!["<asset-id>".to_owned()];
let stream = client.subscribe_orderbook(asset_ids)?;
```

Available streams: `subscribe_orderbook()`, `subscribe_prices()`, `subscribe_midpoints()`, `subscribe_orders()`, `subscribe_trades()`.

## Optional APIs

- **Data API**: positions, trades, leaderboards, analytics
- **Gamma API**: market/event discovery, search, metadata
- **Bridge API**: cross-chain deposits from EVM, Solana, Bitcoin

## Additional CLOB Capabilities

- Rewards & Earnings — query maker rewards, daily earnings, reward percentages
- Streaming Pagination — `stream_data()` for large result sets
- Batch Operations — `post_orders()` and `cancel_orders()` for multiple orders
- Order Scoring — check if orders qualify for maker rewards
- Notifications management
- Balance Management
- Geoblock Detection

## Signature Types

- `signature_type=0`: Standard EOA (MetaMask, hardware wallets)
- `signature_type=1`: Email/Magic wallet signatures
- `signature_type=2`: Browser wallet proxy signatures

## Token Allowances

MetaMask and EOA users must set token allowances. Proxy or Safe-type wallet users do not.

## MSRV: Rust 1.88+
