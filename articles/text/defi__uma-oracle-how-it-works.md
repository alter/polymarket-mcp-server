---
title: "How does UMA's Oracle work? | UMA Documentation"
url: https://docs.uma.xyz/protocol-overview/how-does-umas-oracle-work
source: docs.uma.xyz
date: "2026-05-30"
type: documentation
theme: defi
lang: en
---

# UMA Oracle System: How It Works

## Core Architecture

UMA employs a two-layer oracle system combining the Optimistic Oracle (OO) with the Data Verification Mechanism (DVM). The Optimistic Oracle "acts as a generalized escalation game between contracts that initiate a price request and UMA's dispute resolution system."

## Optimistic Oracle Process

The first layer operates through this sequence:

1. **Assertion Phase**: An asserter posts a bonded claim containing the price identifier, timestamp, ancillary claim details, approved currency, and bond amount representing their stake on correctness.

2. **Challenge Window**: Disputers can contest assertions "within the assertion liveness period by referencing their own off-chain price feeds and determination methodologies." This pre-defined timeframe allows for refutation.

3. **Optimistic Resolution**: If unchallenged, "the assertion is optimistically treated as being correct" without DVM involvement.

4. **Escalation**: Disputed assertions escalate to the DVM for final arbitration.

## Data Verification Mechanism (DVM)

The DVM provides backstop security through tokenholder governance. Upon dispute, it "proposes a vote to UMA tokenholders to report the price of the asset at a specific timestamp." The voting period spans 48-96 hours, with tokenholders referencing established UMIPs to determine outcomes using off-chain methodologies.

## Commit-Reveal Voting Scheme

UMA stakers vote using a commit-reveal scheme:
- **Commit phase (24 hours)**: UMA token holders submit hashed votes
- **Reveal phase (24 hours)**: Voters reveal answers and salts
- This prevents coordination and front-running

## Economic Security

The system incorporates built-in protections: corrupting the oracle would require controlling 65%+ of UMA tokens, making corruption economically infeasible compared to potential profits from manipulation.

Bonds align incentives: proposers and disputers post USDC, and the losing side forfeits the bond — making frivolous proposals or disputes economically irrational.

## Query Types

**YES_OR_NO_QUERY**: Handles binary outcomes using ancillary data for context.

**MULTIPLE_VALUES**: Consolidates up to seven integers in a single request, reducing gas costs for multi-outcome events.

## Smart Contracts Involved

- **OptimisticOracleV2**: Manages the full request lifecycle
- **Store**: Tracks request data
- **Finder**: Connects system components
- **Managed Optimistic Oracle V2 (MOOv2)**: Adds whitelisting for requesters and proposers

## Why UMA for Polymarket

Compared to alternatives:
- **Augur (REP-based)**: Slower settlement, low participation in minor markets
- **Chainlink feeds**: Works for objective data; can't handle subjective judgments
- **Kleros arbitrators**: Introduces curator risk
- **Platform-decided**: Fastest but introduces single-point-of-trust

UMA balances speed (fast for uncontested cases, ~2 hours) with rigor (decentralized voting for disputes, 48-96 hours).

## Resolution Timeline

- Roughly 93% of all markets resolve within 2 hours (undisputed)
- Disputed: 4-8 hours (re-proposal accepted)
- DVM escalation: 48-96 hours
