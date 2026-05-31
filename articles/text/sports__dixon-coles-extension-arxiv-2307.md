---
title: "Extending the Dixon and Coles Model: An Application to Women's Football Data"
url: https://arxiv.org/abs/2307.02139
source: arxiv
date: "2023-07-05"
type: paper
theme: sports
lang: en
---

# Extending the Dixon and Coles Model: An Application to Women's Football Data

**Authors:** Rouven Michels, Marius Ötting, Dimitris Karlis  
**arXiv:** 2307.02139 | July 5, 2023

## Abstract

The prevalent Dixon and Coles (1997) model extends the double Poisson model, moving probabilities between scores 0-0, 0-1, 1-0, and 1-1. This paper demonstrates this framework is a specific instance of multiplicative models within the **Sarmanov family** and develops enhanced alternatives by reallocating probabilities across score combinations and employing alternative discrete probability distributions for women's football datasets.

## Background: Dixon-Coles (1997) Core Innovation

The original Dixon-Coles model introduced:
1. **ρ (rho) parameter**: Dependence structure correcting joint probability from independence assumption — corrects underestimation of 0-0, 1-1 draws
2. **φ (phi) weighting function**: Time-weighting that down-weights old matches, giving more weight to recent form
3. Published in *Journal of the Royal Statistical Society: Series C*, 46(2): 265–280

## Maher (1982) Foundation

Maher (1982) — "Modelling association football scores," *Statistica Neerlandica* 36(3): 109–118 — first used independent Poisson distributions to model football scores, with bivariate Poisson as an extension.

## Key Methodological Contributions

- Dixon-Coles is a special case of the **Sarmanov family of multiplicative models**
- Alternative model specifications reallocate probability mass between different score outcomes
- Alternative discrete distributions (beyond standard Poisson) improve fit on women's football scoring data
- Women's football has distinct statistical characteristics (lower scoring rates, different distributional shapes)

## Transferable Insights

- Low-scoring events (draws in football = unresolved prediction markets) are systematically underestimated by basic Poisson models
- Temporal weighting (φ parameter) is crucial for any model operating on non-stationary time series of team/player strengths
- The Sarmanov family provides a unified theoretical basis for extending probability adjustment mechanisms across any outcome model
