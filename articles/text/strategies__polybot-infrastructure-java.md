---
title: "Polybot: Reverse-Engineer Every Polymarket Strategy and Trade Fast"
url: https://github.com/ent0n29/polybot
source: github.com
date: "2025"
type: repo
theme: strategies
lang: en
---

# Polybot: Polymarket Trading Infrastructure

## Overview

Polybot is an open-source system for automated trading on Polymarket with comprehensive microservices architecture. Goal: "reverse-engineer every Polymarket strategy and trade fast."

## Architecture (5 Java 21 Microservices)

1. **Executor Service** (Port 8080): Order execution, paper trading simulations, settlement
2. **Strategy Service** (Port 8081): Strategy runtime and operational status management
3. **Analytics Service** (Port 8082): APIs querying ClickHouse analytics data
4. **Ingestor Service** (Port 8083): Market and user trade data ingestion pipeline
5. **Infrastructure Orchestrator** (Port 8084): Analytics and monitoring stack lifecycle management

## Data Infrastructure

- **ClickHouse**: Analytics storage (columnar database, optimized for time-series)
- **Redpanda (Kafka-compatible)**: Event streaming
- **Grafana + Prometheus + Alertmanager**: Observability stack

## Included Strategy: Complete-Set Arbitrage

Default implementation: complete-set arbitrage for Polymarket Up/Down binary contracts.

When YES + NO prices < $1.00, simultaneously buy both → guaranteed $1.00 at settlement → risk-free profit (minus fees and gas).

## Research Capabilities

The `research/` directory provides:
- Snapshot extraction
- Replication scoring
- Backtesting
- Execution quality analysis

## Tech Stack Requirements

- Amazon Corretto 21+ / Java 21+
- Maven 3.8+
- Docker with Compose
- Python 3.11+ (research tooling)

## Why ClickHouse + Redpanda

This architecture is production-grade for HFT:
- ClickHouse: handles billions of order book rows, sub-second analytical queries
- Redpanda: Kafka-compatible but lower latency, processes millions of events/second
- Together: enables real-time strategy execution with full historical backtest capability

## License

MIT. For educational and research purposes.
