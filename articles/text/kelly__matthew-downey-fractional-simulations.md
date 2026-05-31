---
title: "Why Fractional Kelly? Simulations of Bet Size with Uncertainty and Downside Risk Mitigation"
url: "https://matthewdowney.github.io/uncertainty-kelly-criterion-optimal-bet-size.html"
source: "Matthew Downey (personal blog)"
date: "2020"
type: "blog"
theme: "kelly"
lang: "en"
---

# Why Fractional Kelly? Simulations of Bet Size with Uncertainty and Downside Risk Mitigation

## Summary

Matthew Downey's article examines why practitioners use fractional Kelly betting rather than the theoretical optimal amount. The piece presents three interactive simulations exploring different explanations.

## Key Findings

**Uncertainty Impact**: Simulates 100 portfolios making 100 wagers each, with win probabilities drawn from a normal distribution. Uncertainty has surprisingly modest effects — increasing standard deviation from 5% to 20% only decreases optimal bet size from 0.38 to 0.36. "Uncertainty matters, but apparently not that much."

**Thorp's Perspective**: E.O. Thorp argued the real issue isn't uncertainty itself but systematic overestimation of winning chances. The asymmetry favors risk reduction: "overbetting is indeed worse than underbetting, and betting half-Kelly offers protection against negative growth rate at the cost of reducing growth by, in this case, <= 25%."

**Risk of Ruin**: Modeling catastrophic loss scenarios proves more persuasive. A stock with 60/40 odds would normally warrant 0.8 Kelly exposure, but factoring in a 1% ruin probability drops optimal betting to 0.46.

**Downside Protection**: The most compelling explanation involves optimizing for lower percentile outcomes rather than median returns. Maximizing the 10th percentile result instead of the 50th substantially reduces bet sizes across different odds structures.

## Conclusion

"Fractional Kelly is overdetermined" — multiple factors justify smaller wagers than pure theory suggests, with downside risk mitigation appearing most significant.

## Relevance

This blog provides practical simulation evidence for why fractional Kelly (rather than full Kelly) is rational even when probability estimates are believed to be accurate.
