---
title: "Gambling and Information Theory — Wikipedia"
url: "https://en.wikipedia.org/wiki/Gambling_and_information_theory"
source: "Wikipedia"
date: "2024"
type: "encyclopedia"
theme: "kelly"
lang: "en"
---

# Gambling and Information Theory — Wikipedia

## Core Connection

Kelly betting (or proportional betting) is an application of information theory to investing and gambling, discovered by John Larry Kelly Jr.

A key part of Kelly's insight was to have the gambler maximize the expectation of the **logarithm** of his capital, rather than the expected profit from each bet. This is important because the logarithm of the gambler's capital is additive in sequential bets, "to which the law of large numbers applies."

## The Shannon Entropy–Kelly Bridge

Kelly's result established a bridge between Shannon's information theory and rational decision-making under uncertainty, revealing that **information gain directly translates into observable payoffs**.

Formally, the **maximum exponential rate of growth** of the gambler's capital is equal to the **rate of transmission of information** over the channel.

## Relative Entropy (KL Divergence)

In information-theoretic contexts, winning the Kelly betting game reduces to the ability of a gambler and adversary to estimate the **true probability distribution** of the random variable. This aligns with how **relative entropy (Kullback-Leibler divergence)** quantifies the inefficiency of approximating one distribution with another.

## Capital Growth as Channel Capacity

If we recognize both terms in the expected wealth equation as entropy terms of the communication channel, we can derive capital as a function of the **transmission rate**: the expected return equals H(α) − H(X|Y), which is the **channel transmission rate**.

## Entropy in the Kelly Profit Formula

The maximal profit in the Kelly framework has two components:
1. The profit on unpopularity of the winning bet (the "seer's profit")
2. The negative entropy −S of the branching — a direct embedding of Shannon entropy into the profit formula

## Summary Table

| Concept | Information Theory Analog |
|---|---|
| Optimal bet size | Channel capacity |
| Log wealth growth | Information transmission rate |
| KL Divergence | Inefficiency of wrong probability estimates |
| Shannon Entropy | Embedded in the Kelly profit formula |
| Side information | Reduces uncertainty → increases optimal bet |

In essence, Kelly betting is a **direct realization of Shannon's information theory** in the domain of financial decision-making.
