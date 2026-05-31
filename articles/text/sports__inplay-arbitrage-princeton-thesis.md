---
title: "Exploiting Arbitrage Opportunities in Live Sports Betting: An Automated Approach"
url: https://theses-dissertations.princeton.edu/entities/publication/437f7731-67ad-4af7-a1e6-648c1d003d46
source: princeton
date: "2024-01-01"
type: paper
theme: sports
lang: en
---

# Exploiting Arbitrage Opportunities in Live Sports Betting: An Automated Approach

**Source:** Princeton University Theses & Dissertations

## Research Question

Do arbitrage opportunities exist in live (in-play) sports betting, and how feasible is automated exploitation?

## Methodology

- **Data:** Live odds from FanDuel and BetMGM across multiple NBA games
- **Scraping frequency:** Real-time odds collection
- **Analysis:** Measured frequency, duration, and magnitude of arbitrage windows

## Key Findings

| Metric | Finding |
|---|---|
| Arbitrage presence | **4.52%** of total scraped game time |
| Average duration | **~13 seconds** under favorable conditions |
| Cross-state arbitrage | No opportunities found |

## Key Observations

1. Arbitrage opportunities in live betting are **rare and short-lived**
2. Opportunities typically last ~13 seconds — requiring automated detection and execution
3. Sportsbooks occasionally "freeze" odds (locking behavior), but freezes were **not correlated** with arbitrage instances
4. **Cross-state arbitrage** (comparing odds between sportsbooks across states) impractical — geolocation restrictions + inability to place simultaneously in two states

## Why In-Play Creates More Opportunities vs. Pre-Match

- Odds fluctuate more dramatically during live events
- If team falls behind by 2 touchdowns early, one book may update faster than another
- New markets open in-play that weren't available pre-match

## Practical Implications

- Manual exploitation is impossible at 13-second windows
- Automated systems are required
- **4.52% of game time** = meaningful opportunity density for well-implemented algos
- Fee structure must be modeled carefully — small arb edges can be consumed by commissions

## Transferable Edge for Polymarket

Polymarket sports contracts during live events show analogous microstructure: information arrives (goal scored, injury), some market participants update faster than others, creating brief windows where YES + NO ≠ $1.00 or where prices diverge from rational expectations. The 13-second window finding sets a benchmark for required execution speed.
