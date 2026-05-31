---
title: "CTF Exchange - Overview Documentation"
url: https://github.com/Polymarket/ctf-exchange/blob/main/docs/Overview.md
source: github.com/Polymarket
date: "2026-05-30"
type: documentation
theme: defi
lang: en
---

# CTF Exchange: Architecture and Operations

## Core Purpose

The CTFExchange contract enables atomic swaps between binary outcome tokens (ERC1155) and collateral assets (ERC20). It operates as "an operator that provides matching/ordering/execution services while settlement happens on-chain, non-custodially."

## Key Assets

- **A**: ERC1155 outcome token
- **A'**: Complementary outcome token
- **C**: ERC20 collateral token (typically USDC)

The system assumes complementary tokens merge into collateral at a 1:1 ratio.

## Three Matching Scenarios

**Scenario 1 - Normal**: Direct token-for-collateral exchange between two parties without minting or merging operations.

**Scenario 2 - Mint**: Two buyers purchasing complementary outcomes simultaneously trigger minting of new token pairs from collateral.

**Scenario 3 - Merge**: Two sellers of complementary tokens trigger merging operations, converting token pairs back into collateral.

## Symmetric Fee Structure

Fees maintain market integrity across complementary pairs. The system applies fees to proceeds (output assets), calculated using:

- **When selling tokens**: `usdcFee = baseRate × min(price, 1-price) × outcomeShareCount`
- **When buying tokens**: `feeBase = baseRate × min(price, 1-price) × (size/price)`

The `baseFeeRate` represents 2x the fee rate when both complementary tokens equal $0.50.

## Contract Architecture

The exchange package includes libraries, mixins implementing related interfaces, and the primary `CTFExchange` contract. Mixins provide full interface implementations and inherit core logic supporting atomic operations.

## CTF Exchange V2 Gas Optimizations

Key gas efficiency improvements in V2:
- For MINT/MERGE matches with multiple makers, V1 called `splitPosition`/`mergePositions` once per maker order. V2 accumulates totals across all makers and executes a single CTF call for the entire batch.
- All events (`OrderFilled`, `OrdersMatched`, `FeeCharged`) are emitted via direct `log2/log3/log4` assembly instructions with manually packed memory layouts, avoiding Solidity's ABI encoding overhead.

## Order Structure

Orders are represented as signed typed structured data (EIP-712). The CTFExchange implements symmetric fees. When orders are matched, one side is considered the maker and the other the taker. The relationship is always either one-to-one or many-to-one (maker to taker), and any price improvement is captured by the taking agent.

## Contract Addresses

- CTF Exchange (v1): `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`
- CTF Core: `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`

Both contracts have been audited by Chainsecurity.
