---
title: "PolyHermes: Polymarket Copy Trading System (Open Source)"
url: "https://github.com/WrBug/PolyHermes"
source: "GitHub (WrBug)"
date: "2025-01-01"
type: "technical"
theme: "onchain"
lang: "zh"
---

# PolyHermes: Polymarket Copy Trading System

**GitHub:** https://github.com/WrBug/PolyHermes

PolyHermes is a comprehensive copy trading platform for Polymarket prediction markets. Supports automated order replication, multi-account management, real-time notifications, and performance analytics.

---

## Core Features

### Account Management
- Multi-wallet support via private key import
- Encrypted storage for credentials
- Portfolio tracking and transaction history
- Account customization options

### Leader Tracking
- Add and monitor trader addresses ("Leaders")
- Category filtering: sports / crypto
- Trade history and performance metrics
- Custom notation system

### Copy Trading Templates
- Configurable templates for order replication
- **Two modes:**
  - Proportional copy (% of leader's position)
  - Fixed-amount copy ($X per trade)
- **Risk controls:**
  - Daily loss limits
  - Order count caps
  - Price tolerance thresholds
- Template reusability across relationships

### Position Management
- Real-time portfolio visibility
- WebSocket-based live updates
- Market and limit order execution
- Batch settlement capability

### Analytics & Statistics
- Global and per-Leader performance summaries
- Category-based reporting
- Time-range filtering
- Individual relationship metrics

---

## Technical Architecture

### Backend
- Spring Boot 3.2.0 (Kotlin 1.9.20)
- MySQL 8.2.0 database
- Retrofit 2.9.0 HTTP client
- Spring WebSocket support

### Frontend
- React 18 with TypeScript
- Ant Design 5.12.0 UI components
- ethers.js 6.9.0 for blockchain interaction
- Zustand for state management

### Deployment
Docker-based deployment with automated setup via interactive scripts. Includes production configurations and external Nginx reverse proxy support.

---

## Known Pitfalls (from developer notes and community experience)

1. **Proportional mode fails** when your capital is 100× smaller than leader — trades round to zero or below $1 minimum
2. **Fixed-amount mode risks** overexposure to low-probability markets
3. **FAK (Fill-And-Kill) orders recommended** for speed: immediately consume available liquidity, uncompleted portion auto-cancelled
4. **De-duplication required**: Use `transactionHash` as unique key, maintain `Set<string>` in memory to prevent double-copying

---

## Deployment Note

Requires `POLYMARKET_PRIVATE_KEY` and API credentials. Docker compose setup included.

---

## Related Open Source Projects

- **FKPolyTools** (GitHub: duzhi5368/FKPolyTools): Unified Polymarket toolkit for arbitrage, copy trading, smart money analysis
- **polymarket-crypto-toolkit** (GitHub: 0xrsydn): Python toolkit with copytrade bot, streak reversal, backtesting, indicators (EMA, SMA, RSI, MACD, Bollinger Bands)
- **dr-manhattan** (GitHub: guzus): "CCXT for prediction markets" — unified Python interface for Polymarket, Kalshi, Limitless, etc.
- **polybot** (GitHub: ent0n29): Complete-set arbitrage strategy for Polymarket Up/Down binaries
- **polyterm** (GitHub: NYTEMODEONLY): Terminal with 20+ analytics tools including whale tracking, insider detection, arbitrage scanning, wash trade detection
