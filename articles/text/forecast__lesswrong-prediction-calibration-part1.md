---
title: "Prediction and Calibration - Part 1"
url: https://www.lesswrong.com/posts/8xcn55gzqzKzjHi7j/prediction-and-calibration-part-1
source: lesswrong
date: "2021-05-08"
type: blog
theme: forecast
lang: en
---

# Prediction and Calibration - Part 1

**Author:** Jan Christian Refsgaard
**Date:** May 8, 2021
**Platform:** LessWrong

## Core Concepts

Uses Bayes' Theorem as its foundation, breaking down how to evaluate predictions through likelihood functions.

## Key Example

Using Scott Alexander's 2019 predictions as a case study, the author demonstrates that Scott's forecasts were approximately "7 billion times more likely" than random guessing. However, this raw statistic conflates two different abilities: making accurate predictions vs. being well-calibrated.

## Critical Distinction: Accuracy vs. Calibration

- **Well-calibrated predictor:** Confidence levels match actual outcome frequencies
- Person A predicting 100 events at 60% confidence with 61 correct outcomes: better calibration
- Person B predicting 100 events at 80% confidence with only 67 correct outcomes: better accuracy but worse calibration
- Improving calibration is often more achievable than improving raw accuracy

## Bernoulli Likelihood Framework

The simplest relevant likelihood function is Bernoulli, where predictions are scored based on:
- Correct prediction (y=1): score = log(p)
- Incorrect prediction (y=0): score = log(1-p)

This is equivalent to the log scoring rule (negative cross-entropy).

## Relevance to Prediction-Market Pricing

Demonstrates via Bayesian reasoning why calibrated probability estimates are more valuable than raw accuracy — directly applicable to building trading models for Polymarket where miscalibration leads to systematic edge erosion.
