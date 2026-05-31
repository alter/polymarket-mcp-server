---
title: "Network-Based Detection of Wash Trading (Polymarket / Columbia Study)"
url: "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5714122"
source: SSRN / Columbia Business School
date: "2025-11-06"
type: academic_paper
theme: manip
lang: en
---

# Network-Based Detection of Wash Trading

**Authors:** Allen Sirolly, Hongyao Ma, Yash Kanoria, Rajiv Sethi  
**Institution:** Columbia Business School  
**Published:** November 6, 2025 (SSRN working paper, not yet peer-reviewed)

## Abstract / Overview

The paper proposes an iterative network-based procedure for detecting wash trading—the practice of buying and selling the same asset without changing net position, solely to inflate recorded volume. The key insight is that wash traders form approximately closed clusters of colluding wallets, transacting almost exclusively with each other rather than with the broader market. Applied to Polymarket's complete on-chain trading history (Polygon blockchain), the algorithm estimates that roughly **25% of total historical volume** is attributable to wash trading.

## Methodology

- Analyzed the full on-chain Polygon blockchain record for Polymarket from inception through October 2025
- Flagged 14% of 1.26 million wallets as likely wash traders based on network clustering
- Detected wallets that "frequently transacted with each other but seldom with other market participants"
- One sub-network of 43,000 wallets traded nearly $1 million "mostly at prices under a penny"

## Key Findings

| Metric | Value |
|--------|-------|
| Overall estimated wash-trading share | ~25% of total volume |
| Peak weekly share | ~60% (December 2024) |
| Sports markets | 45% wash |
| Election markets | 17% wash |
| Politics markets | 12% wash |
| Crypto markets | 3% wash |
| Peak election-market week | ~95% (week of March 24, 2025) |
| Peak sports-market week | ~90% (week of Oct 21, 2024) |
| Total flagged dollar value | ~$4.5 billion |

## Enabling Factors

1. No transaction fees on Polymarket (wash trading economically costless)
2. Absence of KYC / pseudonymous Polygon wallets
3. Speculation about token airdrop rewards (airdrop farming)

## Key Quote

> "Wash trading doesn't add liquidity or information to the market, so it would seem valuable to distinguish authentic from inauthentic volume." — Yash Kanoria

## Implications

Inflated volume misleads users about market depth and sentiment. The authors note Polymarket itself is not implicated as orchestrating wash trading. The paper proposes real-time network analytics as a mitigation path.

## Related Coverage

- Fortune: https://fortune.com/2025/11/07/polymarket-wash-trading-inflated-prediction-markets-columbia-research/
- Decrypt: https://decrypt.co/347842/columbia-study-25-polymarket-volume-wash-trading
- CoinDesk: https://www.coindesk.com/markets/2025/11/07/polymarket-s-trading-volume-may-be-25-fake-columbia-study-finds
- Columbia CBS: https://business.columbia.edu/faculty/research/network-based-detection-wash-trading
