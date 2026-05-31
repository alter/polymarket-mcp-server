---
title: "Truthcoin: Peer-to-Peer Oracle System and Prediction Marketplace"
url: "https://www.truthcoin.info/papers/truthcoin-whitepaper.pdf"
source: truthcoin.info
date: "2014-01-01"
type: whitepaper
theme: depm
lang: en
---

# Truthcoin: Peer-to-Peer Oracle System and Prediction Marketplace

**Author:** Paul Sztorc  
**URL:** https://www.truthcoin.info/papers/truthcoin-whitepaper.pdf  
**Note:** PDF failed to parse as text; content reconstructed from search results and secondary sources.

## Overview

Where Bitcoin allows for the decentralized exchange of value, Truthcoin adds the decentralized creation and administration of Prediction Markets (PMs), using a proof-of-work blockchain to collect information on the creation and state of those markets. An incentive mechanism attempts to guarantee that selfish users resolve outcomes accurately, and bear the economic costs and benefits of the trades they execute and prediction markets they create.

Also known as **Bitcoin Hivemind**.

## What Is a Prediction Market?

A prediction market is a market for buying and selling predictions just like you can buy and sell anything you want—orange juice or milk. Users put their money where their mouth is; they will only win money if they correctly guess a future event.

It is a marketplace for the creation and trading of 'event derivatives', which have a final value based only on the state-of-the-world (such as election results or stock prices) and nothing else.

## Why Decentralize?

While past prediction markets, such as Intrade, shuttered due to government interference, Truthcoin is an uncensorable peer-to-peer prediction market that uses a decentralized oracle system. Past prediction markets relied on a central entity to report the outcomes of events, but Truthcoin relies on a group of volunteers to determine outcomes, with the system incentivizing these oracles to be honest.

Blockchain solutions also generate efficiency by cutting out middlemen and avoiding overhead costs (no brick-and-mortar, compliance, administration, etc.), and they are egalitarian and immortal.

## System Design: VoteCoins and CashCoins

Truthcoin's structure features two types of coins: **VoteCoins** representing reputation and **CashCoins** representing money. Decisions can either be Binary (bordered) or Scaled (blurred). When used in Markets, Scaled Decisions span an entire dimension, whereas Binaries only partition-from-null.

Binary PMs can be used to estimate the likelihood of any defined event, and Scaled PMs can estimate the expected value of any future quantity. However, it is when combined that PMs truly yield their powerful insights — PMs combined within-dimension can assess the probabilities of events with any number of mutually exclusive states, while PMs combined across-dimension can assess joint and marginal probabilities of multiple variables.

## Oracle Mechanism

Reporters (VoteCoin holders) submit reports on outcomes. The system uses a Bayesian Truth Serum-inspired mechanism to incentivize honest reporting: honest behavior is the Nash equilibrium because dishonest reporters risk losing their VoteCoin stake. This is the foundational cryptoeconomic oracle design that later influenced Augur's REP system.

## Beyond Forecasting

The mere presence of a PM-based forecast can conclusively end debates, prevent lies, encourage and protect whistleblowers, and provide decision makers with honest advice. Additionally, PMs have applications altogether beyond forecasting: through creative use of tradable shares, one can provide financial services such as risk management, insurance, retirement portfolios, and recreational gambling.

## Five 'Big Ideas' for Cryptocurrency PMs

1. A decentralized governance model for hard forks
2. Blockchain crypto-assets with a stable fiat-value ("BitUSD")
3. SPV-compatible colored coins
4. The provision of 'public goods' without coercive taxation or third parties
5. Smart contracts and decentralized applications

## Relationship to Other Platforms

Truthcoin was the first blockchain-native prediction market design (Bitcoin-based). Augur and Gnosis are related decentralized prediction markets that use Ethereum rather than Bitcoin. Truthcoin's oracle design directly influenced Augur's REP-based reporting system.

## Additional Papers by Sztorc

- "Unlocking the Power of Prediction Markets" — https://www.truthcoin.info/papers/2_PM_Types.pdf
- "Extra-Predictive Applications of Prediction Markets" — https://www.truthcoin.info/papers/3_PM_Applications.pdf
