---
title: "Predicting Football Results With Statistical Modelling: Dixon-Coles and Time-Weighting"
url: https://dashee87.github.io/football/python/predicting-football-results-with-statistical-modelling-dixon-coles-and-time-weighting/
source: blog
date: "2018-01-01"
type: blog
theme: sports
lang: en
---

# Predicting Football Results: Dixon-Coles and Time-Weighting (Python)

**Author:** dashee87.github.io

## Basic Poisson Model

Each team receives attack and defense strength parameters, with home advantage.

Expected goals:
- Home: `λ = exp(attack_home + defense_away + home_advantage)`
- Away: `μ = exp(attack_away + defense_home)`

Example (EPL 2017/18): Arsenal (home) vs Southampton — Arsenal expected 2.43 goals, Southampton 0.86.

## Dixon-Coles Improvement 1: Low-Score Correction

Basic model systematically underestimates 0-0, 1-0, 0-1, 1-1 outcomes.

Dixon-Coles ρ (rho) correction function:
```python
def rho_correction(x, y, lambda_x, mu_y, rho):
    if x==0 and y==0: return 1 - (lambda_x * mu_y * rho)
    elif x==0 and y==1: return 1 + (lambda_x * rho)
    elif x==1 and y==0: return 1 + (mu_y * rho)
    elif x==1 and y==1: return 1 - rho
    else: return 1.0
```

Optimized ρ = **-0.1285** for EPL 2017/18.  
Effect: draw probability increases from 0.167 → **0.186** for Arsenal-Southampton.

## Dixon-Coles Improvement 2: Maximum Likelihood Estimation

Because ρ correction prevents standard GLM, custom log-likelihood is needed:

```python
def dc_log_like(x, y, alpha_x, beta_x, alpha_y, beta_y, rho, gamma):
    lambda_x = np.exp(alpha_x + beta_y + gamma)
    mu_y = np.exp(alpha_y + beta_x) 
    return (np.log(rho_correction(x, y, lambda_x, mu_y, rho)) + 
            np.log(poisson.pmf(x, lambda_x)) + np.log(poisson.pmf(y, mu_y)))
```

Scipy's minimize function converges on optimal parameters. Constraint: average attack strength = 1.

## Dixon-Coles Improvement 3: Time Weighting

Motivation: Crystal Palace's disastrous 8-game start under De Boer (2017/18) should matter less than recovery under Hodgson.

Exponential decay with parameter ξ:
```python
def dc_log_like_decay(x, y, ..., t, xi=0):
    return np.exp(-xi*t) * (log_likelihood_terms)
```

ξ=0 → no weighting. Higher ξ → older matches discounted more heavily.

**Validation results:**
- Single season: optimal ξ=0 (no weighting benefits within one season)
- Five seasons (2013/14–2017/18): optimal ξ≈0.00325 → improves prediction

## Key Conclusion from Author

"While I've described the different models in some detail, I haven't yet discussed whether these models will make you any money. **They won't.**"

Constraints:
- End-of-season predictions inherently unreliable
- Single-season validation limited decay analysis
- Need to compare against market odds, not raw results

## Source Code

Full Python notebooks available on GitHub (referenced in article). Uses scipy, numpy, pandas.

## Transferable Insights

1. Low-scoring corrections are essential — models without ρ consistently mis价格 draw/nil markets
2. Time weighting matters most over multi-year horizons
3. Custom log-likelihood + scipy optimization is the practical implementation path
4. Always validate against market odds, not just historical outcomes
