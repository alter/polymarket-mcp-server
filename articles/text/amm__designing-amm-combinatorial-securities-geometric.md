---
title: "Designing Automated Market Makers for Combinatorial Securities: A Geometric Viewpoint"
url: "https://arxiv.org/abs/2411.08972"
source: "arxiv"
date: "2024-11-13"
type: "paper"
theme: "amm"
lang: "en"
---

# Designing Automated Market Makers for Combinatorial Securities: A Geometric Viewpoint

**Authors:** Prommy Sultana Hossain, Xintong Wang, Fang-Yi Yu

**arXiv ID:** 2411.08972 | Accepted at SODA'25

## Abstract

Designing automated market makers (AMMs) for prediction markets on combinatorial securities over large outcome spaces poses significant computational challenges. Prior research has primarily focused on specific set systems (e.g., intervals, permutations), but this paper introduces a framework for designing AMMs on arbitrary set systems by building a novel connection to the range query problem in computational geometry. The work demonstrates equivalence between price queries and trade updates under the combinatorial logarithmic market scoring rule (LMSR) and the range query/update problem, enabling analysis of computational complexity and design of efficient AMMs. Sublinear-time algorithms are constructed when the VC dimension of the set system is bounded, and the non-existence of sublinear-time AMMs is proved when the VC dimension is unbounded.

## Key Contributions

- Novel geometric framework connecting AMMs to range query problems in computational geometry
- Equivalence proofs between LMSR market operations and computational geometry operations (range query / range update)
- Complexity analysis based on VC dimension — sublinear time when bounded, impossible when unbounded
- Extension to quadratic and power scoring rules via variations of the partition tree scheme
- Multi-resolution market design naturally integrated into partition-tree scheme
- Application to combinatorial swap operations in decentralized finance

## Technical Details

For LMSR, all operations can be done in linear time by exhausting all n outcomes, but as n becomes large, sublinear or polylogarithmic running time becomes critical. The paper builds a partition-tree-based scheme to support price queries and trade updates in time sublinear to the number of outcomes. The formal definition: an LMSR market supporting price, cost, and buy operations in times T_P(n), T_C(n), and T_B(n), with overall running time max{T_P(n), T_C(n), T_B(n)}.
