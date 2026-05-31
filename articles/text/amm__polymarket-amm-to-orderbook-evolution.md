---
title: "From AMM to Order Book: Exploring the Transformation of Polymarket's Pricing Mechanism"
url: "https://www.panewslab.com/en/articles/fz20kk02b04n"
source: "panewslab.com"
date: "2024-01-01"
type: "blog"
theme: "amm"
lang: "en"
---

# From AMM to Order Book: Exploring the Transformation of Polymarket's Pricing Mechanism

**Source:** PANews

## Polymarket's AMM Era: LMSR

Polymarket originally adopted the Logarithmic Market Scoring Rule (LMSR) as its AMM mechanism:
- Provides continuous liquidity without requiring a counterparty
- Price determined by: P = e^(q_YES / b) / (e^(q_YES / b) + e^(q_NO / b))
- High b = deep market, prices barely move; low b = shallow market, prices spike

### LMSR Limitations at Scale

1. Setting b requires predicting trading volume in advance
2. Arbitrage behavior harder to control
3. Not optimized for professional market makers
4. Oracle reliability issues for settlement

## Transition to Order Book Model

As Polymarket's volume grew (from $73M in 2023 to ~$9B in 2024), they transitioned to a Central Limit Order Book (CLOB):

| Feature | AMM/LMSR Era | Order Book Era |
|---|---|---|
| Liquidity Source | Algorithmic (protocol-provided) | Market Makers & Users |
| Pricing | Automated via LMSR curve | Supply/Demand bid-ask |
| Counterparty | Not needed | Required (matched orders) |
| For niche markets | Excellent | Challenging |
| For liquid markets | Inefficient capital | Efficient |

## Hybrid Architecture

Polymarket's current model uses a hybrid on-chain/off-chain architecture:
- Off-chain: order matching (fast, no gas)
- On-chain: settlement (secure, trustless)
- The protocol does not act as arbitrator — market participants maintain price stability

## Key Lesson

AMMs are best for bootstrapping liquidity in thin, new markets. Order books become preferable once enough professional market makers enter to provide competitive spreads.
