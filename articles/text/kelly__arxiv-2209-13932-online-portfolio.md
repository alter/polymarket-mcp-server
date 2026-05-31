---
title: "Efficient and Near-Optimal Online Portfolio Selection"
url: "https://arxiv.org/abs/2209.13932"
source: "arXiv"
date: "2022-09-28"
type: "academic_paper"
theme: "kelly"
lang: "en"
---

# Efficient and Near-Optimal Online Portfolio Selection

**Authors:** Rémi Jézéquel, Dmitrii M. Ostrovskii, Pierre Gaillard  
**arXiv ID:** 2209.13932  
**Submitted:** September 28, 2022 (revised March 9, 2025)  
**Published:** Mathematics of Operations Research

## Abstract Summary

This paper addresses the online portfolio selection problem, where traders repeatedly allocate capital across multiple assets over time to maximize returns. The researchers propose a new algorithm that improves upon Cover's Universal Portfolios method from 1991.

## Key Improvement

The new approach achieves "essentially the same regret guarantee as Universal Portfolios — up to a constant factor and replacement of log(T) with log(T+d)" while dramatically reducing computational complexity. The per-round runtime drops from Õ(d⁴(T+d)¹⁴) to approximately Õ(d²(T+d)).

## Technical Approach

The algorithm works by minimizing current logarithmic loss regularized by the log-determinant of the portfolio's Hessian. This creates connections between online portfolio selection and two classical optimization areas: cutting-plane algorithms and interior-point methods.

## Classification

- Subjects: Optimization and Control, Computational Finance, Portfolio Management
- Paper Length: 48 pages

## Connection to Kelly

This is the log-optimal / Kelly portfolio framework in an online learning setting. The Universal Portfolio (Cover 1991) achieves no-regret vs the best constant Kelly portfolio; this paper achieves the same guarantee computationally efficiently.
