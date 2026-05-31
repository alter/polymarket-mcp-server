---
title: "Augur: a Decentralized Oracle and Prediction Market Platform (v2.0)"
url: "https://arxiv.org/abs/1501.01042"
source: arxiv
date: "2015-01-05"
type: whitepaper
theme: depm
lang: en
---

# Augur: a Decentralized Oracle and Prediction Market Platform (v2.0)

**Authors:** Jack Peterson, Joseph Krug, Micah Zoltu, Austin K. Williams, Stephanie Alexander  
**arXiv:** 1501.01042 [cs.CR]  
**Submitted:** January 5, 2015; Last revised: November 30, 2020  
**Length:** 16 pages, 2 figures

## Abstract

Augur is a trustless, decentralized oracle and platform for prediction markets built on Ethereum. The outcomes of Augur's prediction markets are chosen by users that hold Augur's native Reputation token (REP), who stake tokens on the actual observed outcome and, in return, receive settlement fees from the markets. Augur's incentive structure is designed to ensure that honest, accurate reporting of outcomes is always the most profitable option for Reputation token holders.

## Introduction

Augur addresses historical limitations of centralized prediction markets by eliminating trust requirements. Developers publish smart contracts to Ethereum with no ability to control fund distribution, market resolution, or trade execution. This represents the first truly decentralized oracle mechanism. The only role of the Augur developers is to publish smart contracts to the Ethereum network—they do not have the ability to spend funds in escrow, do not control how markets resolve, cannot undo trades, and cannot modify or cancel orders.

## Market Mechanics: Four-Stage Progression

Markets follow creation, trading, reporting, and settlement phases:

**Creation Phase:** Anyone can establish a market on any real-world event. Creators select event end times, designated reporters, resolution sources, and post validity bonds (in DAI) plus creation bonds (in REP). The validity bond incentivizes market creators to create markets based on well-defined events with objective, unambiguous outcomes.

**Trading Phase:** Participants forecast outcomes by trading shares representing possible results. The platform maintains automated order books and matching engines, creating complete sets of shares as needed. Fees are paid by traders only when complete sets of shares are sold.

**Reporting Phase:** After events occur, REP holders determine outcomes through a structured process involving designated reporters and community disputes. Honest reporters receive settlement fee rewards proportional to staked tokens.

**Settlement Phase:** Traders close positions either by selling shares or settling with the contract using complete share sets or winning outcome shares.

## REP Token Design and Economic Incentives

Reputation tokens serve multiple functions. The token is necessary for market creation, outcome reporting, and fee collection. By owning REP and participating in accurate reporting on the outcomes of events, token holders are entitled to a portion of the fees on the platform.

Critically, REP is not used for trading—only reporting. This separation ensures traders need not understand the REP mechanism to participate.

## Reporting System: Dispute Windows and Cycles

The system operates through 7-day dispute windows. All fees collected by Augur during a given dispute window are added to the reporting fee pool for that dispute window. At window conclusion, pools distribute to participating reporters proportionally.

**Participation Tokens** allow REP holders to purchase weekly check-in access, earning fee shares regardless of active reporting. This mechanism encourages regular platform engagement and fork awareness.

## Dispute Resolution Architecture

Markets progress through seven states: pre-reporting, designated reporting, open reporting, dispute round, waiting for window, fork, and finalized.

**Designated Reporting:** Chosen reporters have 24 hours to submit outcomes. Failure forfeits the creation bond to the first public reporter.

**Open Reporting:** If designated reporters miss deadlines, any REP holder can report, receiving the forfeited bond if correct.

**Dispute Rounds:** Communities can challenge tentative outcomes by staking REP on alternatives. The dispute bond formula is: B(ω,n) = 2An − 3S(ω,n) where An represents total stakes and S represents outcome-specific stakes. Successfully disputing incorrect outcomes yields 40% ROI on dispute stakes.

**Forking:** Markets initiating bonds exceeding 2.5% of theoretical REP supply trigger forks—the system's "nuclear option" lasting up to 60 days.

## Fork Mechanism: Last Resort Resolution

When forking occurs, new universes split from the parent universe, one for each possible outcome. REP holders migrate tokens to child universes corresponding to believed-true outcomes. Tokens remaining in parent universes after 60 days lose value permanently.

Sibling universes are entirely disjoint. REP tokens that exist in one universe cannot be used to report on events or earn rewards from markets in another universe.

This design ensures false universes become worthless—no traders engage with untrustworthy oracles, eliminating REP demand in false universes.

## Security Framework: Market Cap Integrity

Platform security depends on REP market capitalization relative to escrowed native open interest. The forking protocol has integrity whenever S>1/2 or whenever Ia+Ip < (P−Pf)SM, where S represents proportion of REP migrating to true outcomes, P is REP price, M is total REP supply, and I variables represent interest amounts.

Under reasonable assumptions—50% minimum true-universe migration, zero false-universe REP value—market cap should exceed native open interest by 5x for security.

## Automated Fee Adjustment

Augur dynamically adjusts reporting fees within 0.01% to 33.3% ranges. If the current market cap is below the target, then reporting fees are automatically increased, putting upward pressure on the price of REP and/or downward pressure on new native open interest. This feedback mechanism maintains security ratios automatically.

## Potential Risks and Limitations

**Parasitic Markets:** External markets resolving per Augur outcomes but paying no fees drain trading interest and reporting fee pools, threatening REP market cap and security integrity.

**Volatile Open Interest:** Sudden spikes (sporting events) temporarily increase security requirements faster than fee mechanisms respond, creating brief vulnerability windows.

**Malicious Resolution Sources:** Market creators selecting unreliable sources force reporters into conflicting interpretations, potentially enabling attack vectors.

**Self-Referential Markets:** Predictions about oracle behavior itself may create perverse incentives undermining designated reporter participation.

**Fork Participation Uncertainty:** Developers cannot guarantee >50% REP migration to true outcomes during forks, as assumed by security proofs.

**Subjective Outcome Ambiguity:** Genuinely ambiguous events may split reporter opinions, potentially resulting in multiple valuable child universes post-fork.

## Conclusion

Augur achieves decentralized, trustless market resolution through economic incentives, cryptographic security, and game-theoretic design. By leveraging fork threats rather than executing forks regularly, the platform balances efficiency with integrity. Success depends critically on maintaining adequate REP market capitalization and community participation in accurate reporting during disputes.
