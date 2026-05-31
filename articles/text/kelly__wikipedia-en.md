---
title: "Kelly criterion - Wikipedia"
url: "https://en.wikipedia.org/wiki/Kelly_criterion"
source: "Wikipedia"
date: "2024"
type: "encyclopedia"
theme: "kelly"
lang: "en"
---

# Kelly Criterion — Wikipedia

## Definition and Historical Context

The Kelly criterion is a mathematical formula for determining optimal bet sizing in situations with known probabilities. Developed by John Larry Kelly Jr. at Bell Labs in 1956, it "maximizes the long-term expected value of the logarithm of wealth, which is equivalent to maximizing the long-term expected geometric growth rate."

## Core Formula for Binary Outcomes

For simple win-or-lose scenarios, the fundamental gambling formula is:

**f* = p - q/b**

Where:
- f* represents the fraction of bankroll to wager
- p is the winning probability
- q equals 1 - p (losing probability)
- b is the payoff ratio on a win

### Practical Example

In a scenario with 60% win probability and 1-to-1 odds, the optimal bet would be 20% of one's bankroll per opportunity.

## Investment Applications

A generalized version for investments with partial losses:

**f* = (p/l) - (q/g)**

This accounts for different magnitudes of gains and losses, making it applicable to stock market decisions where outcomes aren't strictly binary.

## Fractional Kelly Approaches

Rather than betting the full Kelly amount, practitioners commonly use:
- **Half Kelly**: 50% of the calculated fraction
- **Quarter Kelly**: 25% of the calculated fraction

These reduced positions help minimize volatility and account for estimation errors in probability calculations.

## Key Properties and Advantages

The strategy guarantees superior long-term performance compared to alternative approaches. Over many repeated bets with consistent probabilities and payoffs, full Kelly maximizes compound wealth growth. The geometric growth advantage becomes more pronounced as the number of trials increases.

## Critical Limitations and Criticisms

- **Probability estimation errors**: The formula requires accurate win probabilities, which are rarely obtainable in real markets. When a gambler overestimates their true probability of winning, the criterion value calculated will diverge from the optimal, increasing the risk of ruin.
- **Volatility concerns**: Full Kelly strategies experience substantial drawdowns, leading most practitioners to adopt fractional alternatives despite accepting slower growth rates.
- **Behavioral factors**: Research on betting behavior reveals significant deviations from Kelly predictions. In one experiment, only 21% of participants achieved optimal results when given favorable coin-flip opportunities.
- **Philosophical debates**: Economists like Paul Samuelson questioned whether time diversification truly differs from asset diversification.

## Modern Applications

The criterion has influenced mainstream investment theory since the 2000s, with claims that prominent investors including Warren Buffett employ Kelly-style analysis. However, most acknowledge the need for substantial safety margins through fractional Kelly betting given real-world uncertainties.
