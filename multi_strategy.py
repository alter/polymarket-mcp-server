#!/usr/bin/env python3
"""Multi-Strategy Paper Trading Arena v2 — diverse indicators + tick persistence."""

import asyncio, itertools, json, logging, math, os, time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, List

import httpx

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("Arena")

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"

STARTING_BALANCE = 1000.0
MAX_POSITIONS = 20
PRICE_POLL_INTERVAL = 30
MARKET_SCAN_INTERVAL = 900
SETTLE_CHECK_INTERVAL = 300
LEADERBOARD_INTERVAL = 600
TICK_FLUSH_INTERVAL = 60
PRUNE_INTERVAL = 3600
MIN_VOLUME_24H = 10_000
MIN_MID_PRICE = 0.10
MAX_MID_PRICE = 0.90
MIN_SPREAD = 0.001
MAX_SPREAD = 0.05
PRUNE_MIN_TRADES = 100
PRUNE_DEAD_EQUITY = 500.0
PRUNE_MAX_STRATEGIES = 300

DATA_DIR = "data"
TICKS_FILE = os.path.join(DATA_DIR, "arena_ticks.jsonl")
RESULTS_FILE = os.path.join(DATA_DIR, "arena_results.json")
TRADES_FILE = os.path.join(DATA_DIR, "arena_trades.jsonl")
BLACKLIST_FILE = os.path.join(DATA_DIR, "strategy_blacklist.json")


def strategy_signature(params):
    """Param-only signature (no id). Same params across regrid → same sig.
    Used for persistent retire: if a sig is blacklisted, any future strategy
    matching it starts pre-retired regardless of arena_results.json state."""
    return (f"{params.indicator}|p{params.period}|e{params.entry_param:.4f}"
            f"|sl{params.stop_loss:.3f}|tp{params.take_profit:.3f}"
            f"|fee{int(params.fee_free_only)}|{params.side_bias}")

FEE_RATES = {
    "sports_fees_v2": 0.03, "culture_fees": 0.05, "finance_fees": 0.04,
    "politics_fees": 0.04, "economics_fees": 0.05, "crypto_fees": 0.072,
    "mentions_fees": 0.04, "tech_fees": 0.04,
}


def calc_taker_fee(size_usd, price, fee_type):
    if not fee_type:
        return 0.0
    return size_usd * FEE_RATES.get(fee_type, 0.05) * (1.0 - price)


# ─── Indicator calculations ─────────────────────────────────────────────────

def ema(prices, period):
    """Exponential moving average. prices: list of floats, newest last."""
    if not prices:
        return 0.0
    alpha = 2.0 / (period + 1)
    e = prices[0]
    for p in prices[1:]:
        e = alpha * p + (1 - alpha) * e
    return e


def sma(prices, period):
    """Simple moving average of last N prices."""
    window = prices[-period:] if len(prices) >= period else prices
    return sum(window) / len(window) if window else 0.0


def rsi(prices, period=14):
    """Relative Strength Index. Returns 0-100."""
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        d = prices[i] - prices[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))


def bollinger(prices, period=20, std_mult=2.0):
    """Bollinger bands. Returns (lower, middle, upper)."""
    window = prices[-period:] if len(prices) >= period else prices
    if len(window) < 2:
        return (0, 0, 0)
    m = sum(window) / len(window)
    var = sum((p - m) ** 2 for p in window) / len(window)
    sd = math.sqrt(var)
    return (m - std_mult * sd, m, m + std_mult * sd)


def zscore(prices, period=20):
    """Z-score of current price vs last N."""
    window = prices[-period:] if len(prices) >= period else prices
    if len(window) < 5:
        return 0.0
    m = sum(window) / len(window)
    var = sum((p - m) ** 2 for p in window) / len(window)
    sd = math.sqrt(var)
    if sd == 0:
        return 0.0
    return (prices[-1] - m) / sd


def macd(prices, fast=12, slow=26, signal=9):
    """MACD line and signal. Returns (macd_line, signal_line, histogram)."""
    if len(prices) < slow:
        return (0, 0, 0)
    ema_f = ema(prices, fast)
    ema_s = ema(prices, slow)
    line = ema_f - ema_s
    # Signal line = EMA of MACD line — approximated with last N MACD values
    # For simplicity: use a rolling estimate
    recent_lines = []
    for i in range(max(0, len(prices) - signal), len(prices)):
        sub = prices[:i+1]
        if len(sub) >= slow:
            recent_lines.append(ema(sub, fast) - ema(sub, slow))
    sig = ema(recent_lines, signal) if recent_lines else 0
    return (line, sig, line - sig)


def breakout(prices, period=20):
    """Breakout: is current price at high/low of last N? Returns -1, 0, or 1."""
    window = prices[-period:] if len(prices) >= period else prices
    if len(window) < 5:
        return 0
    cur = prices[-1]
    hi, lo = max(window), min(window)
    if cur >= hi * 0.999:
        return 1
    if cur <= lo * 1.001:
        return -1
    return 0


def momentum(prices, period=10):
    """Momentum: (current - N ago) / N ago."""
    if len(prices) <= period:
        return 0.0
    old = prices[-period-1]
    if old == 0:
        return 0.0
    return (prices[-1] - old) / old


# ─── Extended indicator library ─────────────────────────────────────────────

def wma(prices, period):
    """Weighted Moving Average (linear weights)."""
    window = prices[-period:] if len(prices) >= period else prices
    if not window:
        return 0.0
    n = len(window)
    weights = list(range(1, n + 1))
    return sum(w * p for w, p in zip(weights, window)) / sum(weights)


def hma(prices, period):
    """Hull Moving Average approximation."""
    if len(prices) < period:
        return sum(prices) / len(prices) if prices else 0.0
    half = max(period // 2, 1)
    sqrt_p = max(int(period ** 0.5), 1)
    w1 = wma(prices, half)
    w2 = wma(prices, period)
    raw = 2 * w1 - w2
    # WMA of raw — need series, simplified: return raw
    return raw


def dema(prices, period):
    """Double EMA."""
    if len(prices) < period:
        return prices[-1] if prices else 0.0
    e1 = ema(prices, period)
    # EMA of EMA approximation: run EMA again on a virtual series
    # Simplified: 2*ema1 - ema2
    e2 = ema(prices, period * 2)
    return 2 * e1 - e2


def tema(prices, period):
    """Triple EMA."""
    e1 = ema(prices, period)
    e2 = ema(prices, period * 2)
    e3 = ema(prices, period * 3)
    return 3 * e1 - 3 * e2 + e3


def kama(prices, period=10):
    """Kaufman Adaptive MA approximation."""
    if len(prices) < period + 1:
        return prices[-1] if prices else 0.0
    change = abs(prices[-1] - prices[-period-1])
    vol = sum(abs(prices[i] - prices[i-1]) for i in range(-period, 0))
    er = change / vol if vol > 0 else 0  # efficiency ratio
    fast = 2.0 / (2 + 1)
    slow = 2.0 / (30 + 1)
    sc = (er * (fast - slow) + slow) ** 2
    return prices[-period-1] + sc * (prices[-1] - prices[-period-1])


def stochastic_k(prices, period=14):
    """%K of Stochastic: (close - low) / (high - low) × 100."""
    window = prices[-period:] if len(prices) >= period else prices
    if len(window) < 2:
        return 50.0
    hi, lo = max(window), min(window)
    if hi == lo:
        return 50.0
    return (prices[-1] - lo) / (hi - lo) * 100


def williams_r(prices, period=14):
    """Williams %R: -100 to 0."""
    window = prices[-period:] if len(prices) >= period else prices
    if len(window) < 2:
        return -50.0
    hi, lo = max(window), min(window)
    if hi == lo:
        return -50.0
    return (hi - prices[-1]) / (hi - lo) * -100


def cci(prices, period=20):
    """Commodity Channel Index."""
    window = prices[-period:] if len(prices) >= period else prices
    if len(window) < 2:
        return 0.0
    ma = sum(window) / len(window)
    mean_dev = sum(abs(p - ma) for p in window) / len(window)
    if mean_dev == 0:
        return 0.0
    return (prices[-1] - ma) / (0.015 * mean_dev)


def roc(prices, period=10):
    """Rate of Change (percentage)."""
    if len(prices) <= period:
        return 0.0
    old = prices[-period-1]
    if old == 0:
        return 0.0
    return (prices[-1] - old) / old * 100


def ppo(prices, fast=12, slow=26):
    """Percentage Price Oscillator."""
    if len(prices) < slow:
        return 0.0
    ef = ema(prices, fast)
    es = ema(prices, slow)
    if es == 0:
        return 0.0
    return (ef - es) / es * 100


def awesome_osc(prices):
    """Awesome Oscillator: SMA(5) - SMA(34)."""
    if len(prices) < 34:
        return 0.0
    return sma(prices, 5) - sma(prices, 34)


def fisher_transform(prices, period=10):
    """Fisher Transform approximation."""
    import math
    window = prices[-period:] if len(prices) >= period else prices
    if len(window) < 2:
        return 0.0
    hi, lo = max(window), min(window)
    if hi == lo:
        return 0.0
    x = 2 * ((prices[-1] - lo) / (hi - lo) - 0.5)
    x = max(min(x, 0.999), -0.999)
    return 0.5 * math.log((1 + x) / (1 - x))


def cmo(prices, period=14):
    """Chande Momentum Oscillator."""
    if len(prices) < period + 1:
        return 0.0
    ups, downs = 0.0, 0.0
    for i in range(-period, 0):
        d = prices[i] - prices[i-1]
        if d > 0:
            ups += d
        else:
            downs += abs(d)
    if ups + downs == 0:
        return 0.0
    return (ups - downs) / (ups + downs) * 100


def trix(prices, period=14):
    """TRIX: % ROC of triple-smoothed EMA."""
    if len(prices) < period * 3:
        return 0.0
    e1 = ema(prices, period)
    e2 = ema(prices, period * 2)
    e3 = ema(prices, period * 3)
    tema_val = 3 * e1 - 3 * e2 + e3
    tema_old = 3 * ema(prices[:-1], period) - 3 * ema(prices[:-1], period*2) + ema(prices[:-1], period*3)
    if tema_old == 0:
        return 0.0
    return (tema_val - tema_old) / tema_old * 100


def ulcer_index(prices, period=14):
    """Ulcer Index — drawdown measure."""
    if len(prices) < period:
        return 0.0
    window = prices[-period:]
    max_so_far = window[0]
    sq_dd = []
    for p in window:
        if p > max_so_far:
            max_so_far = p
        if max_so_far > 0:
            dd = (p - max_so_far) / max_so_far * 100
            sq_dd.append(dd ** 2)
    if not sq_dd:
        return 0.0
    return (sum(sq_dd) / len(sq_dd)) ** 0.5


def historical_volatility(prices, period=20):
    """Std of returns."""
    if len(prices) < period + 1:
        return 0.0
    window = prices[-period-1:]
    returns = [(window[i] - window[i-1]) / window[i-1] if window[i-1] > 0 else 0
               for i in range(1, len(window))]
    if len(returns) < 2:
        return 0.0
    m = sum(returns) / len(returns)
    var = sum((r - m) ** 2 for r in returns) / len(returns)
    return var ** 0.5


def donchian_channel(prices, period=20):
    """Returns (lo, mid, hi) of N-period range."""
    window = prices[-period:] if len(prices) >= period else prices
    if not window:
        return 0, 0, 0
    lo, hi = min(window), max(window)
    return lo, (lo + hi) / 2, hi


def keltner_channel(prices, period=20, atr_mult=2.0):
    """Keltner: EMA ± ATR_mult × rolling std."""
    if len(prices) < period:
        return 0, 0, 0
    mid = ema(prices, period)
    std = historical_volatility(prices, period) * (sum(prices[-period:]) / period)
    return mid - atr_mult * std, mid, mid + atr_mult * std


def linear_regression_slope(prices, period=20):
    """Slope of best-fit line over last N."""
    window = prices[-period:] if len(prices) >= period else prices
    n = len(window)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(window) / n
    num = sum((i - x_mean) * (p - y_mean) for i, p in enumerate(window))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den != 0 else 0.0


def percentile_rank(prices, period=20):
    """Current price percentile in last N (0-100)."""
    window = prices[-period:] if len(prices) >= period else prices
    if not window:
        return 50.0
    cur = prices[-1]
    below = sum(1 for p in window if p < cur)
    return below / len(window) * 100


def skewness(prices, period=20):
    """Skewness of returns."""
    if len(prices) < period + 1:
        return 0.0
    window = prices[-period-1:]
    returns = [(window[i] - window[i-1]) / max(window[i-1], 1e-9) for i in range(1, len(window))]
    n = len(returns)
    if n < 3:
        return 0.0
    m = sum(returns) / n
    var = sum((r - m) ** 2 for r in returns) / n
    sd = var ** 0.5
    if sd == 0:
        return 0.0
    return sum((r - m) ** 3 for r in returns) / (n * sd ** 3)


def moving_median(prices, period=20):
    window = prices[-period:] if len(prices) >= period else prices
    if not window:
        return 0.0
    return sorted(window)[len(window) // 2]


def adx_approx(prices, period=14):
    """Simplified ADX: average of |return| over period / avg |return| baseline."""
    if len(prices) < period * 2:
        return 0.0
    returns = [abs((prices[i] - prices[i-1]) / max(prices[i-1], 1e-9)) for i in range(1, len(prices))]
    recent = returns[-period:]
    baseline = returns[-period*2:-period]
    r_mean = sum(recent) / len(recent)
    b_mean = sum(baseline) / max(len(baseline), 1)
    if b_mean == 0:
        return 50.0
    return min(r_mean / b_mean * 25, 100.0)


def aroon_up_down(prices, period=14):
    """Aroon Up = ((period - bars_since_high) / period) × 100."""
    window = prices[-period-1:] if len(prices) >= period+1 else prices
    if len(window) < 2:
        return 50.0, 50.0
    max_idx = max(range(len(window)), key=lambda i: window[i])
    min_idx = min(range(len(window)), key=lambda i: window[i])
    period_actual = len(window) - 1
    up = ((period_actual - (period_actual - max_idx)) / period_actual) * 100
    down = ((period_actual - (period_actual - min_idx)) / period_actual) * 100
    return up, down


def vortex(prices, period=14):
    """Vortex Indicator approximation — trend strength & direction."""
    if len(prices) < period + 1:
        return 0.0
    vm_pos = sum(abs(prices[-i] - prices[-i-1]) for i in range(1, period+1) if prices[-i] > prices[-i-1])
    vm_neg = sum(abs(prices[-i] - prices[-i-1]) for i in range(1, period+1) if prices[-i] < prices[-i-1])
    total = vm_pos + vm_neg
    if total == 0:
        return 0.0
    return (vm_pos - vm_neg) / total


def kalman_filter_1d(prices, process_var=0.01, measurement_var=1.0):
    """Simple 1D Kalman filter; returns smoothed latest value."""
    if not prices:
        return 0.0
    x = prices[0]
    P = 1.0
    for p in prices[1:]:
        # Predict
        P = P + process_var
        # Update
        K = P / (P + measurement_var)
        x = x + K * (p - x)
        P = (1 - K) * P
    return x


def hurst_exponent(prices, period=50):
    """Simplified Hurst exponent via R/S analysis. 0.5=random walk, >0.5=trending, <0.5=mean-reverting."""
    import math
    if len(prices) < period:
        return 0.5
    window = prices[-period:]
    mean = sum(window) / len(window)
    # Cumulative deviations
    cum_dev = []
    total = 0
    for p in window:
        total += p - mean
        cum_dev.append(total)
    R = max(cum_dev) - min(cum_dev)
    var = sum((p - mean) ** 2 for p in window) / len(window)
    S = var ** 0.5
    if S == 0 or R == 0:
        return 0.5
    return math.log(R / S) / math.log(period)


def haar_decompose(prices, levels=3):
    """Haar wavelet decomposition. Returns list of approximations by level.
    approximations[0] = original, approximations[L] = 2^L-point averages."""
    approx = list(prices)
    result = [approx]
    for _ in range(levels):
        if len(approx) < 2:
            break
        new_approx = []
        for i in range(0, len(approx) - 1, 2):
            new_approx.append((approx[i] + approx[i+1]) / 2.0)
        result.append(new_approx)
        approx = new_approx
    return result


def wavelet_reconstruct(prices, level=2, keep_details=False):
    """Haar wavelet denoise: zero out details at specified level, reconstruct.
    Returns denoised signal aligned to original length (last value = denoised latest).
    Simple: upsample approximation back to original length."""
    approxs = haar_decompose(prices, levels=level)
    if len(approxs) <= level:
        return prices[-1] if prices else 0.0
    # Get approximation at target level and use last value (represents 2^level smoothed)
    smoothed = approxs[level]
    if not smoothed:
        return prices[-1]
    return smoothed[-1]


def wavelet_divergence(prices, level=2):
    """Returns (price - smoothed) / smoothed — % divergence from wavelet smoothing.
    Positive = price above smoothed trend (sell signal for mean reversion)."""
    if len(prices) < (2 ** level) + 2:
        return 0.0
    smoothed = wavelet_reconstruct(prices, level=level)
    if smoothed <= 0:
        return 0.0
    return (prices[-1] - smoothed) / smoothed


def wavelet_multiscale(prices, levels=(1, 2, 3)):
    """Multi-scale divergence. Returns list of divergences at each level.
    If all agree in sign → strong signal."""
    divs = []
    for lv in levels:
        if len(prices) >= (2 ** lv) + 2:
            d = wavelet_divergence(prices, level=lv)
            divs.append(d)
    return divs


# ─── Strategy parameters ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class StrategyParams:
    id: int
    indicator: str           # "mean_rev_ema", "momentum", "rsi", "bollinger",
                             # "macd", "breakout", "zscore", "mean_rev_sma"
    period: int              # lookback window
    entry_param: float       # entry threshold (meaning varies by indicator)
    exit_param: float        # exit threshold
    stop_loss: float         # -99 = disabled
    take_profit: float
    position_usd: float
    fee_free_only: bool
    side_bias: str = "both"  # "long", "short", "both"

    @property
    def name(self):
        sl = "off" if self.stop_loss <= -0.90 else f"{self.stop_loss:.0%}"
        return (f"S{self.id:03d}|{self.indicator[:7]}|"
                f"p{self.period}|e{self.entry_param:.2f}|"
                f"sl{sl}|tp{self.take_profit:.0%}|"
                f"{'free' if self.fee_free_only else 'all'}|"
                f"{self.side_bias[0]}")


def generate_strategies():
    """Generate 250+ diverse strategies across 8 indicator types."""
    strategies = []
    sid = 0

    def add(**kw):
        nonlocal sid
        sid += 1
        kw.setdefault("position_usd", 50.0)
        kw.setdefault("side_bias", "both")
        strategies.append(StrategyParams(id=sid, **kw))

    # ── Group 1: mean_rev_ema (winning strategy family, keep same IDs 1-48) ──
    # Replicating original 129 grid for backward compatibility
    for ema_p, entry, sl, tp in itertools.product(
            [5, 10, 20, 40], [0.01, 0.02, 0.04],
            [-0.10, -0.25, -99.0], [0.05, 0.10, 0.25]):
        add(indicator="mean_rev_ema", period=ema_p, entry_param=entry,
            exit_param=entry*0.75, stop_loss=sl, take_profit=tp,
            fee_free_only=False)

    # Fee-free variants (this is where money is)
    for ema_p, entry in [(5, 0.01), (5, 0.015), (5, 0.005),
                         (10, 0.02), (10, 0.01), (10, 0.04),
                         (20, 0.02), (20, 0.03)]:
        for sl, tp in [(-0.25, 0.10), (-99.0, 0.10), (-0.10, 0.05),
                       (-0.25, 0.05), (-99.0, 0.05)]:
            add(indicator="mean_rev_ema", period=ema_p, entry_param=entry,
                exit_param=entry*0.75, stop_loss=sl, take_profit=tp,
                fee_free_only=True)

    # ── Group 2: momentum ──
    for p, entry in [(10, 0.02), (20, 0.03), (40, 0.04), (5, 0.015)]:
        for sl, tp in [(-0.10, 0.08), (-0.20, 0.15), (-99.0, 0.10)]:
            add(indicator="momentum", period=p, entry_param=entry,
                exit_param=entry*0.5, stop_loss=sl, take_profit=tp,
                fee_free_only=False)
            add(indicator="momentum", period=p, entry_param=entry,
                exit_param=entry*0.5, stop_loss=sl, take_profit=tp,
                fee_free_only=True)

    # ── Group 3: RSI (mean reversion on overbought/oversold) ──
    # entry_param = RSI threshold for oversold (buy when RSI < X)
    # exit_param = RSI threshold for exit (sell when RSI > Y)
    for period in [14, 21, 7]:
        for entry, exit_p in [(30, 70), (25, 75), (35, 65), (20, 80)]:
            for sl, tp in [(-0.10, 0.05), (-0.25, 0.10), (-99.0, 0.10)]:
                add(indicator="rsi", period=period, entry_param=entry,
                    exit_param=exit_p, stop_loss=sl, take_profit=tp,
                    fee_free_only=True)

    # ── Group 4: Bollinger bands ──
    # entry_param = std dev multiplier for entry (buy below -X, sell above +X)
    for period in [20, 10, 30]:
        for std_mult in [2.0, 2.5, 1.5]:
            for sl, tp in [(-0.10, 0.05), (-0.25, 0.10), (-99.0, 0.10)]:
                add(indicator="bollinger", period=period, entry_param=std_mult,
                    exit_param=0.5, stop_loss=sl, take_profit=tp,
                    fee_free_only=True)

    # ── Group 5: Z-score ──
    for period in [20, 10, 50]:
        for z_thresh in [2.0, 1.5, 2.5, 3.0]:
            for sl, tp in [(-0.10, 0.05), (-99.0, 0.10)]:
                add(indicator="zscore", period=period, entry_param=z_thresh,
                    exit_param=0.5, stop_loss=sl, take_profit=tp,
                    fee_free_only=True)

    # ── Group 6: MACD ──
    for period in [26, 12, 40]:
        for entry in [0.005, 0.01, 0.02]:
            for sl, tp in [(-0.10, 0.08), (-99.0, 0.10)]:
                add(indicator="macd", period=period, entry_param=entry,
                    exit_param=0, stop_loss=sl, take_profit=tp,
                    fee_free_only=True)

    # ── Group 7: Breakout ──
    for period in [10, 20, 50]:
        for sl, tp in [(-0.05, 0.10), (-0.10, 0.15), (-0.15, 0.20)]:
            add(indicator="breakout", period=period, entry_param=1,
                exit_param=0, stop_loss=sl, take_profit=tp,
                fee_free_only=True)
            add(indicator="breakout", period=period, entry_param=1,
                exit_param=0, stop_loss=sl, take_profit=tp,
                fee_free_only=False)

    # ── Group 8: mean_rev_sma (SMA-based instead of EMA) ──
    for sma_p, entry in [(10, 0.01), (20, 0.02), (30, 0.015)]:
        for sl, tp in [(-0.10, 0.05), (-0.25, 0.10), (-99.0, 0.10)]:
            add(indicator="mean_rev_sma", period=sma_p, entry_param=entry,
                exit_param=entry*0.75, stop_loss=sl, take_profit=tp,
                fee_free_only=True)

    # ── Group 9: side-biased variants (NO-only, mimicking our structural NO) ──
    for indicator, period, entry in [
            ("mean_rev_ema", 5, 0.01), ("mean_rev_ema", 10, 0.02),
            ("rsi", 14, 30), ("bollinger", 20, 2.0)]:
        add(indicator=indicator, period=period, entry_param=entry,
            exit_param=entry*0.75 if "mean_rev" in indicator else 0.5,
            stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True, side_bias="short")  # NO-only

    # ── Group 10: large position sizing variants for S118-like winners ──
    for pos_usd in [25.0, 100.0, 150.0]:
        add(indicator="mean_rev_ema", period=5, entry_param=0.01,
            exit_param=0.0075, stop_loss=-0.25, take_profit=0.10,
            position_usd=pos_usd, fee_free_only=True)

    # ── Group 11: Wavelet mean reversion (single scale) ──
    # period = wavelet level: 1=smooth over 2 ticks, 2=4 ticks, 3=8, 4=16
    for level in [1, 2, 3, 4]:
        for entry in [0.005, 0.01, 0.02]:
            for sl, tp in [(-0.10, 0.05), (-0.25, 0.10), (-99.0, 0.10)]:
                add(indicator="wavelet_mr", period=level, entry_param=entry,
                    exit_param=entry*0.5, stop_loss=sl, take_profit=tp,
                    fee_free_only=True)

    # ── Group 12: Multi-scale wavelet (all scales must agree) ──
    for max_level in [2, 3, 4]:
        for entry in [0.005, 0.01, 0.015]:
            for sl, tp in [(-0.25, 0.10), (-99.0, 0.10)]:
                add(indicator="wavelet_ms", period=max_level, entry_param=entry,
                    exit_param=entry*0.5, stop_loss=sl, take_profit=tp,
                    fee_free_only=True)

    # ── Group 13: Wavelet on all markets (not just fee-free) ──
    for level in [2, 3]:
        for entry in [0.01, 0.02]:
            add(indicator="wavelet_mr", period=level, entry_param=entry,
                exit_param=entry*0.5, stop_loss=-0.25, take_profit=0.10,
                fee_free_only=False)

    # ── Group 14-17: Ensemble strategies (majority voting across indicators) ──
    # Each ensemble samples K indicators from indicator buckets.
    # Signal: majority of sub-indicators must agree.
    import random
    random.seed(42)  # reproducibility of strategy IDs

    # Ensemble types:
    # - uniform: sample K indicators randomly
    # - stratified: 1 from each bucket (6 buckets = 6 indicators)
    # - weighted: bias towards "winners" (wavelet, ema5, zscore)
    # - voting_all: all indicators vote, majority wins

    # Stratified (5 from different buckets): varied sample
    for _ in range(20):
        add(indicator="ensemble_strat", period=5, entry_param=0.6,  # 60% majority
            exit_param=0.5, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # Uniform random (10 indicators): denser voting
    for _ in range(15):
        add(indicator="ensemble_uniform", period=10, entry_param=0.55,
            exit_param=0.5, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # Large ensemble (30 indicators, weighted): robust aggregate
    for _ in range(15):
        add(indicator="ensemble_weighted", period=30, entry_param=0.55,
            exit_param=0.5, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # Supermajority ensemble (need 70% agreement)
    for _ in range(10):
        add(indicator="ensemble_super", period=15, entry_param=0.70,
            exit_param=0.5, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # Variants for "all markets" (not fee-free)
    for _ in range(10):
        add(indicator="ensemble_strat", period=5, entry_param=0.6,
            exit_param=0.5, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=False)

    # ── Group 18-21: RANDOM FORESTS (15-45 indicators per strategy) ──
    # Each forest samples N indicators randomly across buckets.
    # Different forest variants explore: small/medium/large size,
    # different majority thresholds, different seed distributions.

    # Small forests (15 indicators, 60% majority) — 50 strategies
    for i in range(50):
        add(indicator="forest_15", period=15, entry_param=0.60,
            exit_param=0.45, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # Medium forests (25 indicators, 55% majority) — 60 strategies
    for i in range(60):
        add(indicator="forest_25", period=25, entry_param=0.55,
            exit_param=0.45, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # Large forests (35 indicators, 55% majority) — 50 strategies
    for i in range(50):
        add(indicator="forest_35", period=35, entry_param=0.55,
            exit_param=0.45, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # Huge forests (45 indicators, 52% majority — weak signal on large N) — 40
    for i in range(40):
        add(indicator="forest_45", period=45, entry_param=0.52,
            exit_param=0.45, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # High-confidence forests (25 indicators, 70% supermajority) — 30
    for i in range(30):
        add(indicator="forest_25_super", period=25, entry_param=0.70,
            exit_param=0.5, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # Tight SL / quick TP variant — 30
    for i in range(30):
        add(indicator="forest_25", period=25, entry_param=0.55,
            exit_param=0.45, stop_loss=-0.10, take_profit=0.05,
            fee_free_only=True)

    # On all markets (not fee-free) — 20
    for i in range(20):
        add(indicator="forest_25", period=25, entry_param=0.55,
            exit_param=0.45, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=False)

    # ── Group 22: META-GATED FORESTS (A/B test vs regular forests) ──
    # Same forest compositions (deterministic by seed) but gated through
    # meta_predict lookup. Expectation: lower trade volume, higher WR.

    # forest_15_meta — 50 strategies (same size as forest_15)
    for i in range(50):
        add(indicator="forest_15_meta", period=15, entry_param=0.60,
            exit_param=0.45, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # forest_25_meta — 60 strategies (same size as forest_25 main group)
    for i in range(60):
        add(indicator="forest_25_meta", period=25, entry_param=0.55,
            exit_param=0.45, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # forest_35_meta — 50 strategies
    for i in range(50):
        add(indicator="forest_35_meta", period=35, entry_param=0.55,
            exit_param=0.45, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=True)

    # forest_25_meta on all markets — 20
    for i in range(20):
        add(indicator="forest_25_meta", period=25, entry_param=0.55,
            exit_param=0.45, stop_loss=-0.25, take_profit=0.10,
            fee_free_only=False)

    # ── Group 23: HYBRID TRIPLE-CONFIRM (wavelet + bollinger + zscore) ──
    # Grid search over wavelet params; BB(20,2.0) and zscore(20,2.0) fixed
    # to validated defaults from top individual performers.

    # hybrid_wbz_all — ALL 3 must agree (strict, low trade freq, high conviction)
    for level in [2, 3, 4]:
        for wv_thresh in [0.005, 0.01, 0.015, 0.02]:
            for sl, tp in [(-0.10, 0.05), (-0.25, 0.10), (-99.0, 0.10)]:
                add(indicator="hybrid_wbz_all", period=level,
                    entry_param=wv_thresh, exit_param=wv_thresh*0.5,
                    stop_loss=sl, take_profit=tp,
                    fee_free_only=True)

    # hybrid_wbz_2of3 — any 2 of 3 (relaxed, more trades)
    for level in [2, 3, 4]:
        for wv_thresh in [0.005, 0.01, 0.015, 0.02]:
            for sl, tp in [(-0.10, 0.05), (-0.25, 0.10), (-99.0, 0.10)]:
                add(indicator="hybrid_wbz_2of3", period=level,
                    entry_param=wv_thresh, exit_param=wv_thresh*0.5,
                    stop_loss=sl, take_profit=tp,
                    fee_free_only=True)

    # all-markets variants (with fees) — small subset for comparison
    for level in [3]:
        for wv_thresh in [0.01, 0.015]:
            for sl, tp in [(-0.25, 0.10)]:
                add(indicator="hybrid_wbz_all", period=level,
                    entry_param=wv_thresh, exit_param=wv_thresh*0.5,
                    stop_loss=sl, take_profit=tp,
                    fee_free_only=False)
                add(indicator="hybrid_wbz_2of3", period=level,
                    entry_param=wv_thresh, exit_param=wv_thresh*0.5,
                    stop_loss=sl, take_profit=tp,
                    fee_free_only=False)

    # ── Group 24: AUTOTUNED_SAFE — top autotune params + SL=-20% tail protection ──
    # Based on 130K trade analysis: sloff wins on avg PnL but leaves tail open.
    # SL=-0.20 (20%) is compromise: rare trigger (most trades never hit it)
    # but caps catastrophic loss on outlier events.

    # zscore winners: p50 thresh 2.0/2.5/3.0
    for p, t in [(50, 2.0), (50, 2.5), (50, 3.0), (20, 2.5), (20, 3.0)]:
        for tp in [0.05, 0.10]:
            add(indicator="zscore", period=p, entry_param=t,
                exit_param=0.5, stop_loss=-0.20, take_profit=tp,
                fee_free_only=True)

    # mean_rev_ema winners: p20/p10, dev 0.02-0.04
    for p, dev in [(20, 0.02), (20, 0.03), (10, 0.03), (10, 0.04)]:
        for tp in [0.05, 0.10]:
            add(indicator="mean_rev_ema", period=p, entry_param=dev,
                exit_param=dev*0.75, stop_loss=-0.20, take_profit=tp,
                fee_free_only=True)

    # bollinger winners: p30 std 2.0/2.5, p20 std 1.5/2.5
    for p, std in [(30, 2.0), (30, 2.5), (20, 1.5), (20, 2.5)]:
        for tp in [0.05, 0.10]:
            add(indicator="bollinger", period=p, entry_param=std,
                exit_param=0.5, stop_loss=-0.20, take_profit=tp,
                fee_free_only=True)

    # wavelet winners: level=3 / 4
    for lv, t in [(3, 0.01), (4, 0.01), (4, 0.02)]:
        for tp in [0.05, 0.10]:
            add(indicator="wavelet_mr", period=lv, entry_param=t,
                exit_param=t*0.5, stop_loss=-0.20, take_profit=tp,
                fee_free_only=True)

    # hybrid winners with SL=-0.20 (safety net variant of best hybrid configs)
    for lv in [3, 4]:
        for t in [0.01, 0.015]:
            for tp in [0.05, 0.10]:
                add(indicator="hybrid_wbz_all", period=lv,
                    entry_param=t, exit_param=t*0.5,
                    stop_loss=-0.20, take_profit=tp,
                    fee_free_only=True)
                add(indicator="hybrid_wbz_2of3", period=lv,
                    entry_param=t, exit_param=t*0.5,
                    stop_loss=-0.20, take_profit=tp,
                    fee_free_only=True)

    return strategies


ALL_STRATEGIES = generate_strategies()
logger.info(f"Generated {len(ALL_STRATEGIES)} strategies")


# ─── Market & state ──────────────────────────────────────────────────────────

@dataclass
class PriceTick:
    ts: float
    mid: float
    bid: float
    ask: float


@dataclass
class TrackedMarket:
    market_id: str
    question: str
    token_yes: str
    token_no: str
    end_date: str
    volume_24h: float
    fees_enabled: bool = False
    fee_type: Optional[str] = None
    ticks: deque = field(default_factory=lambda: deque(maxlen=500))


class StrategyState:
    def __init__(self, params: StrategyParams):
        self.params = params
        self.balance = STARTING_BALANCE
        self.positions: Dict[str, dict] = {}
        self.history: List[dict] = []
        self.total_fees = 0.0
        self.total_trades = 0
        self.retired = False  # pruned strategies stay retired
        self.retired_at = None
        self.peak_equity = STARTING_BALANCE  # tracks max equity for DD-based retire

    @property
    def equity(self):
        return self.balance + sum(p["cost_usd"] for p in self.positions.values())

    @property
    def pnl(self):
        return self.equity - STARTING_BALANCE

    @property
    def realized(self):
        return sum(h["pnl"] for h in self.history)

    @property
    def wins(self):
        return sum(1 for h in self.history if h["pnl"] > 0)

    @property
    def losses(self):
        return sum(1 for h in self.history if h["pnl"] <= 0 and h["pnl"] != 0)

    def open_position(self, market_id, question, side, token_id, price,
                      fee_type, reason):
        fee = calc_taker_fee(self.params.position_usd, price, fee_type)
        cost = self.params.position_usd + fee
        if cost > self.balance or len(self.positions) >= MAX_POSITIONS:
            return
        if market_id in self.positions:
            return
        shares = self.params.position_usd / price
        self.balance -= cost
        self.total_fees += fee
        self.total_trades += 1
        self.positions[market_id] = {
            "question": question, "side": side, "token_id": token_id,
            "entry_price": price, "shares": shares, "cost_usd": cost,
            "fee_type": fee_type, "reason": reason,
            "opened_at": time.time(),
        }

    def close_position(self, market_id, exit_price, reason):
        if market_id not in self.positions:
            return
        pos = self.positions[market_id]
        gross = pos["shares"] * exit_price
        fee = calc_taker_fee(gross, exit_price, pos.get("fee_type"))
        net = gross - fee
        pnl = net - pos["cost_usd"]
        self.balance += net
        self.total_fees += fee
        self.total_trades += 1
        # Track peak equity for drawdown-based retire
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity
        trade_record = {
            "strategy_id": self.params.id,
            "strategy_name": self.params.name,
            "indicator": self.params.indicator,
            "market_id": market_id,
            "question": pos["question"][:80],
            "side": pos["side"],
            "entry": round(pos["entry_price"], 4),
            "exit": round(exit_price, 4),
            "shares": round(pos["shares"], 4),
            "cost": round(pos["cost_usd"], 4),
            "fee": round(fee, 4),
            "pnl": round(pnl, 4),
            "reason": reason,
            "opened_at": pos.get("opened_at", 0),
            "closed_at": time.time(),
            "equity_after": round(self.equity, 4),
        }
        self.history.append(trade_record)
        # Persist to JSONL (append mode, durable across restarts)
        try:
            with open(TRADES_FILE, "a") as f:
                f.write(json.dumps(trade_record) + "\n")
        except Exception:
            pass
        del self.positions[market_id]

    def to_dict(self):
        return {
            "id": self.params.id, "name": self.params.name,
            "equity": round(self.equity, 2),
            "balance": round(self.balance, 2),
            "positions": len(self.positions),
            "trades": self.total_trades,
            "wins": self.wins, "losses": self.losses,
            "realized": round(self.realized, 2),
            "fees": round(self.total_fees, 4),
            "retired": self.retired,
            "retired_at": self.retired_at,
            "peak_equity": round(self.peak_equity, 2),
            "params": {
                "indicator": self.params.indicator,
                "period": self.params.period,
                "entry_param": self.params.entry_param,
                "stop_loss": self.params.stop_loss,
                "take_profit": self.params.take_profit,
                "fee_free_only": self.params.fee_free_only,
                "side_bias": self.params.side_bias,
            },
        }


# ─── Indicator buckets for ensembles ────────────────────────────────────────

# Each sub-indicator returns "buy", "sell", or None.
# These are the atomic voters for ensembles.

def sub_ema_mr(prices, period=10, thresh=0.01):
    if len(prices) < period + 2:
        return None
    e = ema(prices, period)
    if e <= 0:
        return None
    dev = (prices[-1] - e) / e
    if dev < -thresh:
        return "buy"
    if dev > thresh:
        return "sell"
    return None


def sub_sma_mr(prices, period=10, thresh=0.01):
    if len(prices) < period + 2:
        return None
    s = sma(prices, period)
    if s <= 0:
        return None
    dev = (prices[-1] - s) / s
    if dev < -thresh:
        return "buy"
    if dev > thresh:
        return "sell"
    return None


def sub_rsi(prices, period=14, lo=30, hi=70):
    if len(prices) < period + 2:
        return None
    r = rsi(prices, period)
    if r < lo:
        return "buy"
    if r > hi:
        return "sell"
    return None


def sub_bollinger(prices, period=20, std_mult=2.0):
    if len(prices) < period + 2:
        return None
    low_b, mid, high_b = bollinger(prices, period, std_mult)
    cur = prices[-1]
    if cur < low_b:
        return "buy"
    if cur > high_b:
        return "sell"
    return None


def sub_zscore(prices, period=20, thresh=2.0):
    if len(prices) < period + 2:
        return None
    z = zscore(prices, period)
    if z < -thresh:
        return "buy"
    if z > thresh:
        return "sell"
    return None


def sub_momentum(prices, period=10, thresh=0.02):
    if len(prices) <= period:
        return None
    m = momentum(prices, period)
    if m > thresh:
        return "buy"
    if m < -thresh:
        return "sell"
    return None


def sub_breakout(prices, period=20):
    if len(prices) < period + 2:
        return None
    b = breakout(prices, period)
    if b == 1:
        return "buy"
    if b == -1:
        return "sell"
    return None


def sub_wavelet(prices, level=3, thresh=0.01):
    if len(prices) < 2**level + 2:
        return None
    div = wavelet_divergence(prices, level)
    if div < -thresh:
        return "buy"
    if div > thresh:
        return "sell"
    return None


def sub_hybrid_wbz_all(prices, wv_level=3, wv_thresh=0.01,
                       bb_period=20, bb_std=2.0,
                       z_period=20, z_thresh=2.0):
    """Triple-confirm: wavelet + bollinger + zscore ALL agree."""
    votes = [
        sub_wavelet(prices, wv_level, wv_thresh),
        sub_bollinger(prices, bb_period, bb_std),
        sub_zscore(prices, z_period, z_thresh),
    ]
    if all(v == "buy" for v in votes):
        return "buy"
    if all(v == "sell" for v in votes):
        return "sell"
    return None


def sub_hybrid_wbz_2of3(prices, wv_level=3, wv_thresh=0.01,
                        bb_period=20, bb_std=2.0,
                        z_period=20, z_thresh=2.0):
    """2-of-3 confirm: wavelet, bollinger, zscore — any 2 agree same direction."""
    votes = [
        sub_wavelet(prices, wv_level, wv_thresh),
        sub_bollinger(prices, bb_period, bb_std),
        sub_zscore(prices, z_period, z_thresh),
    ]
    buys = sum(1 for v in votes if v == "buy")
    sells = sum(1 for v in votes if v == "sell")
    if buys >= 2 and buys > sells:
        return "buy"
    if sells >= 2 and sells > buys:
        return "sell"
    return None


def sub_macd(prices, fast=12, slow=26, signal=9, thresh=0.005):
    if len(prices) < slow + 2:
        return None
    line, sig, hist = macd(prices, fast, slow, signal)
    if hist > thresh:
        return "buy"
    if hist < -thresh:
        return "sell"
    return None


def sub_wma_mr(prices, period=10, thresh=0.01):
    if len(prices) < period + 2:
        return None
    w = wma(prices, period)
    if w <= 0:
        return None
    dev = (prices[-1] - w) / w
    if dev < -thresh:
        return "buy"
    if dev > thresh:
        return "sell"
    return None


def sub_hma_mr(prices, period=10, thresh=0.01):
    if len(prices) < period + 2:
        return None
    h = hma(prices, period)
    if h <= 0:
        return None
    dev = (prices[-1] - h) / h
    if dev < -thresh:
        return "buy"
    if dev > thresh:
        return "sell"
    return None


def sub_dema_mr(prices, period=10, thresh=0.01):
    if len(prices) < period * 2:
        return None
    d = dema(prices, period)
    if d <= 0:
        return None
    dev = (prices[-1] - d) / d
    if dev < -thresh:
        return "buy"
    if dev > thresh:
        return "sell"
    return None


def sub_tema_mr(prices, period=10, thresh=0.01):
    if len(prices) < period * 3:
        return None
    t = tema(prices, period)
    if t <= 0:
        return None
    dev = (prices[-1] - t) / t
    if dev < -thresh:
        return "buy"
    if dev > thresh:
        return "sell"
    return None


def sub_kama_mr(prices, period=10, thresh=0.01):
    if len(prices) < period + 2:
        return None
    k = kama(prices, period)
    if k <= 0:
        return None
    dev = (prices[-1] - k) / k
    if dev < -thresh:
        return "buy"
    if dev > thresh:
        return "sell"
    return None


def sub_stoch(prices, period=14, lo=20, hi=80):
    if len(prices) < period + 2:
        return None
    k = stochastic_k(prices, period)
    if k < lo:
        return "buy"
    if k > hi:
        return "sell"
    return None


def sub_williams(prices, period=14, lo=-80, hi=-20):
    if len(prices) < period + 2:
        return None
    w = williams_r(prices, period)
    if w < lo:
        return "buy"
    if w > hi:
        return "sell"
    return None


def sub_cci(prices, period=20, thresh=100):
    if len(prices) < period + 2:
        return None
    c = cci(prices, period)
    if c < -thresh:
        return "buy"
    if c > thresh:
        return "sell"
    return None


def sub_roc(prices, period=10, thresh=2.0):
    if len(prices) <= period:
        return None
    r = roc(prices, period)
    if r > thresh:
        return "buy"
    if r < -thresh:
        return "sell"
    return None


def sub_ppo(prices, fast=12, slow=26, thresh=0.5):
    if len(prices) < slow:
        return None
    p = ppo(prices, fast, slow)
    if p > thresh:
        return "buy"
    if p < -thresh:
        return "sell"
    return None


def sub_fisher(prices, period=10, thresh=1.5):
    if len(prices) < period + 2:
        return None
    f = fisher_transform(prices, period)
    if f < -thresh:
        return "buy"
    if f > thresh:
        return "sell"
    return None


def sub_cmo(prices, period=14, thresh=50):
    if len(prices) < period + 2:
        return None
    c = cmo(prices, period)
    if c < -thresh:
        return "buy"
    if c > thresh:
        return "sell"
    return None


def sub_donchian(prices, period=20):
    if len(prices) < period + 2:
        return None
    lo, mid, hi = donchian_channel(prices, period)
    cur = prices[-1]
    if cur <= lo:
        return "buy"
    if cur >= hi:
        return "sell"
    return None


def sub_keltner(prices, period=20, atr_mult=2.0):
    if len(prices) < period + 2:
        return None
    lo, mid, hi = keltner_channel(prices, period, atr_mult)
    cur = prices[-1]
    if cur < lo:
        return "buy"
    if cur > hi:
        return "sell"
    return None


def sub_lin_slope(prices, period=20, thresh=0.001):
    if len(prices) < period:
        return None
    s = linear_regression_slope(prices, period)
    if s > thresh:
        return "buy"
    if s < -thresh:
        return "sell"
    return None


def sub_pct_rank(prices, period=20, lo=10, hi=90):
    if len(prices) < period:
        return None
    pr = percentile_rank(prices, period)
    if pr < lo:
        return "buy"
    if pr > hi:
        return "sell"
    return None


def sub_skew(prices, period=20, thresh=0.5):
    if len(prices) < period + 2:
        return None
    s = skewness(prices, period)
    if s > thresh:
        return "sell"  # positive skew = mean reversion from high
    if s < -thresh:
        return "buy"
    return None


def sub_median_mr(prices, period=20, thresh=0.01):
    if len(prices) < period:
        return None
    med = moving_median(prices, period)
    if med <= 0:
        return None
    dev = (prices[-1] - med) / med
    if dev < -thresh:
        return "buy"
    if dev > thresh:
        return "sell"
    return None


def sub_aroon(prices, period=14):
    if len(prices) < period + 2:
        return None
    up, down = aroon_up_down(prices, period)
    if up > 70 and down < 30:
        return "buy"
    if down > 70 and up < 30:
        return "sell"
    return None


def sub_vortex(prices, period=14, thresh=0.2):
    if len(prices) < period + 2:
        return None
    v = vortex(prices, period)
    if v > thresh:
        return "buy"
    if v < -thresh:
        return "sell"
    return None


def sub_kalman_mr(prices, period=None, thresh=0.01):
    if len(prices) < 10:
        return None
    k = kalman_filter_1d(prices)
    if k <= 0:
        return None
    dev = (prices[-1] - k) / k
    if dev < -thresh:
        return "buy"
    if dev > thresh:
        return "sell"
    return None


def sub_hurst(prices, period=50, trend_thresh=0.55, revert_thresh=0.45):
    if len(prices) < period:
        return None
    h = hurst_exponent(prices, period)
    # Trending: momentum, mean-reverting: fade
    if h > trend_thresh:
        # Follow the trend
        mom = momentum(prices, 10)
        if mom > 0.01:
            return "buy"
        if mom < -0.01:
            return "sell"
    elif h < revert_thresh:
        # Mean reversion
        e = ema(prices, 10)
        if e > 0:
            dev = (prices[-1] - e) / e
            if dev < -0.01:
                return "buy"
            if dev > 0.01:
                return "sell"
    return None


def sub_awesome(prices, thresh=0.01):
    if len(prices) < 34:
        return None
    ao = awesome_osc(prices)
    if ao > thresh:
        return "buy"
    if ao < -thresh:
        return "sell"
    return None


def sub_ulcer(prices, period=14, thresh=5.0):
    if len(prices) < period:
        return None
    u = ulcer_index(prices, period)
    # High ulcer = recent drawdown = mean reversion buy opportunity
    if u > thresh:
        return "buy"
    return None


def sub_ma_cross(prices, fast=10, slow=30):
    """Fast MA crossing slow MA."""
    if len(prices) < slow + 1:
        return None
    f_now = ema(prices, fast)
    s_now = ema(prices, slow)
    f_prev = ema(prices[:-1], fast)
    s_prev = ema(prices[:-1], slow)
    # Bullish cross
    if f_prev <= s_prev and f_now > s_now:
        return "buy"
    if f_prev >= s_prev and f_now < s_now:
        return "sell"
    return None


def sub_adx(prices, period=14, thresh=40):
    if len(prices) < period * 2:
        return None
    a = adx_approx(prices, period)
    # High ADX = trend, use momentum direction
    if a > thresh:
        mom = momentum(prices, period)
        if mom > 0:
            return "buy"
        if mom < 0:
            return "sell"
    return None


# Bucket structure: ~300 sub-indicators grouped by signal type.
# Each bucket spans a wide range of parameters for diversity in random forests.

def _build_buckets():
    buckets = {
        "mean_reversion": [],
        "oscillators": [],
        "volatility": [],
        "statistical": [],
        "momentum": [],
        "pattern": [],
        "wavelet": [],
        "trend": [],
        "filter": [],
        "advanced": [],
    }

    # ── mean_reversion: EMA/SMA/WMA/HMA/DEMA/TEMA/KAMA/Kalman/Median × many periods ──
    for p in [3, 5, 7, 10, 15, 20, 30, 50]:
        for t in [0.005, 0.01, 0.015, 0.02]:
            buckets["mean_reversion"].append(
                (f"ema_mr_{p}_{int(t*1000)}", sub_ema_mr, {"period": p, "thresh": t}))
            buckets["mean_reversion"].append(
                (f"sma_mr_{p}_{int(t*1000)}", sub_sma_mr, {"period": p, "thresh": t}))
    for p in [5, 10, 20, 30]:
        for t in [0.01, 0.02]:
            buckets["mean_reversion"].append(
                (f"wma_mr_{p}_{int(t*1000)}", sub_wma_mr, {"period": p, "thresh": t}))
            buckets["mean_reversion"].append(
                (f"hma_mr_{p}_{int(t*1000)}", sub_hma_mr, {"period": p, "thresh": t}))
            buckets["mean_reversion"].append(
                (f"dema_mr_{p}_{int(t*1000)}", sub_dema_mr, {"period": p, "thresh": t}))
            buckets["mean_reversion"].append(
                (f"tema_mr_{p}_{int(t*1000)}", sub_tema_mr, {"period": p, "thresh": t}))
            buckets["mean_reversion"].append(
                (f"kama_mr_{p}_{int(t*1000)}", sub_kama_mr, {"period": p, "thresh": t}))
    for p in [10, 20, 30, 50]:
        for t in [0.01, 0.02]:
            buckets["mean_reversion"].append(
                (f"median_mr_{p}_{int(t*1000)}", sub_median_mr, {"period": p, "thresh": t}))
    for t in [0.005, 0.01, 0.02]:
        buckets["mean_reversion"].append(
            (f"kalman_mr_{int(t*1000)}", sub_kalman_mr, {"thresh": t}))

    # ── oscillators: RSI, Stochastic, Williams, CCI, CMO, Fisher ──
    for p in [3, 5, 7, 10, 14, 21, 28]:
        for lo, hi in [(20, 80), (25, 75), (30, 70), (35, 65)]:
            buckets["oscillators"].append(
                (f"rsi_{p}_{lo}_{hi}", sub_rsi, {"period": p, "lo": lo, "hi": hi}))
    for p in [5, 14, 21]:
        for lo, hi in [(15, 85), (20, 80), (25, 75)]:
            buckets["oscillators"].append(
                (f"stoch_{p}_{lo}_{hi}", sub_stoch, {"period": p, "lo": lo, "hi": hi}))
    for p in [5, 14, 21]:
        for lo, hi in [(-85, -15), (-80, -20), (-75, -25)]:
            buckets["oscillators"].append(
                (f"williams_{p}_{abs(lo)}_{abs(hi)}", sub_williams,
                 {"period": p, "lo": lo, "hi": hi}))
    for p in [10, 14, 20, 30]:
        for t in [80, 100, 150, 200]:
            buckets["oscillators"].append(
                (f"cci_{p}_{t}", sub_cci, {"period": p, "thresh": t}))
    for p in [9, 14, 20]:
        for t in [30, 50, 70]:
            buckets["oscillators"].append(
                (f"cmo_{p}_{t}", sub_cmo, {"period": p, "thresh": t}))
    for p in [5, 10, 20]:
        for t in [1.0, 1.5, 2.0]:
            buckets["oscillators"].append(
                (f"fisher_{p}_{int(t*10)}", sub_fisher, {"period": p, "thresh": t}))

    # ── volatility: Bollinger, Keltner, Donchian ──
    for p in [10, 15, 20, 30, 50]:
        for sd in [1.0, 1.5, 2.0, 2.5, 3.0]:
            buckets["volatility"].append(
                (f"bb_{p}_{int(sd*10)}", sub_bollinger, {"period": p, "std_mult": sd}))
    for p in [10, 20, 30]:
        for m in [1.5, 2.0, 2.5]:
            buckets["volatility"].append(
                (f"keltner_{p}_{int(m*10)}", sub_keltner,
                 {"period": p, "atr_mult": m}))
    for p in [10, 20, 30, 50]:
        buckets["volatility"].append(
            (f"donchian_{p}", sub_donchian, {"period": p}))

    # ── statistical: z-score, percentile rank, skewness, hurst ──
    for p in [5, 10, 20, 30, 50, 100]:
        for t in [1.0, 1.5, 2.0, 2.5, 3.0]:
            buckets["statistical"].append(
                (f"z_{p}_{int(t*10)}", sub_zscore, {"period": p, "thresh": t}))
    for p in [10, 20, 50]:
        for lo, hi in [(5, 95), (10, 90), (20, 80)]:
            buckets["statistical"].append(
                (f"pctrank_{p}_{lo}_{hi}", sub_pct_rank,
                 {"period": p, "lo": lo, "hi": hi}))
    for p in [10, 20, 30]:
        for t in [0.3, 0.5, 1.0]:
            buckets["statistical"].append(
                (f"skew_{p}_{int(t*10)}", sub_skew, {"period": p, "thresh": t}))
    for p in [30, 50, 100]:
        buckets["statistical"].append(
            (f"hurst_{p}", sub_hurst, {"period": p}))

    # ── momentum: ROC, Momentum, PPO, Awesome ──
    for p in [3, 5, 10, 15, 20, 30]:
        for t in [0.01, 0.02, 0.03, 0.05]:
            buckets["momentum"].append(
                (f"mom_{p}_{int(t*1000)}", sub_momentum, {"period": p, "thresh": t}))
    for p in [5, 10, 20, 30]:
        for t in [1.0, 2.0, 3.0, 5.0]:
            buckets["momentum"].append(
                (f"roc_{p}_{int(t*10)}", sub_roc, {"period": p, "thresh": t}))
    for fast, slow in [(5, 20), (10, 30), (12, 26), (20, 50)]:
        for t in [0.3, 0.5, 1.0]:
            buckets["momentum"].append(
                (f"ppo_{fast}_{slow}_{int(t*10)}", sub_ppo,
                 {"fast": fast, "slow": slow, "thresh": t}))
    for t in [0.005, 0.01, 0.02]:
        buckets["momentum"].append(
            (f"ao_{int(t*1000)}", sub_awesome, {"thresh": t}))

    # ── pattern: Breakout, Aroon, Vortex ──
    for p in [5, 10, 15, 20, 30, 50, 100]:
        buckets["pattern"].append(
            (f"break_{p}", sub_breakout, {"period": p}))
    for p in [10, 14, 25]:
        buckets["pattern"].append(
            (f"aroon_{p}", sub_aroon, {"period": p}))
    for p in [10, 14, 20]:
        for t in [0.15, 0.25, 0.35]:
            buckets["pattern"].append(
                (f"vortex_{p}_{int(t*100)}", sub_vortex, {"period": p, "thresh": t}))

    # ── wavelet: levels 1-5 × thresholds ──
    for lv in [1, 2, 3, 4, 5]:
        for t in [0.005, 0.01, 0.02, 0.03]:
            buckets["wavelet"].append(
                (f"wv_l{lv}_{int(t*1000)}", sub_wavelet, {"level": lv, "thresh": t}))

    # ── trend: MACD variants, MA crossovers, ADX ──
    for fast, slow in [(5, 13), (8, 21), (12, 26), (10, 30), (20, 50)]:
        for t in [0.002, 0.005, 0.01]:
            buckets["trend"].append(
                (f"macd_{fast}_{slow}_{int(t*1000)}", sub_macd,
                 {"fast": fast, "slow": slow, "signal": 9, "thresh": t}))
    for fast, slow in [(5, 15), (10, 30), (20, 50), (5, 20)]:
        buckets["trend"].append(
            (f"cross_{fast}_{slow}", sub_ma_cross, {"fast": fast, "slow": slow}))
    for p in [10, 14, 20]:
        for t in [30, 40, 50]:
            buckets["trend"].append(
                (f"adx_{p}_{t}", sub_adx, {"period": p, "thresh": t}))

    # ── filter: Linear slope, Kalman ──
    for p in [10, 20, 30, 50]:
        for t in [0.0005, 0.001, 0.002]:
            buckets["filter"].append(
                (f"lin_{p}_{int(t*10000)}", sub_lin_slope, {"period": p, "thresh": t}))

    # ── advanced: Ulcer (drawdown), Hurst-already in stat ──
    for p in [10, 14, 20, 30]:
        for t in [3.0, 5.0, 8.0]:
            buckets["advanced"].append(
                (f"ulcer_{p}_{int(t*10)}", sub_ulcer, {"period": p, "thresh": t}))

    return buckets


INDICATOR_BUCKETS = _build_buckets()
_TOTAL_INDICATORS = sum(len(v) for v in INDICATOR_BUCKETS.values())
logger.info(f"Indicator library: {_TOTAL_INDICATORS} indicators across {len(INDICATOR_BUCKETS)} buckets")


def build_indicator_list(mode="stratified", size=15, seed=None):
    """Build a list of sub-indicators for an ensemble.

    Modes:
      stratified: sample 1-2 from each bucket
      uniform: sample K uniformly from all indicators
      weighted: bias towards proven winners (wavelet, oscillators, statistical)
      all: use every indicator
    """
    import random as _rnd
    rng = _rnd.Random(seed)
    all_inds = []
    for bucket, items in INDICATOR_BUCKETS.items():
        for name, fn, kw in items:
            all_inds.append((name, fn, kw, bucket))

    if mode == "all":
        return all_inds

    if mode == "stratified":
        sampled = []
        buckets = list(INDICATOR_BUCKETS.keys())
        # Take 1 from each bucket, optionally more if size > n_buckets
        for bucket in buckets:
            items = [x for x in all_inds if x[3] == bucket]
            if items:
                sampled.append(rng.choice(items))
        # Fill up to size
        while len(sampled) < size and len(all_inds) > len(sampled):
            pick = rng.choice(all_inds)
            if pick not in sampled:
                sampled.append(pick)
        return sampled[:size]

    if mode == "uniform":
        n = min(size, len(all_inds))
        return rng.sample(all_inds, n)

    if mode == "weighted":
        # Higher weight to winners from backtest (wavelet, osc, stat)
        weights_by_bucket = {
            "wavelet": 3, "oscillators": 2, "statistical": 2, "volatility": 2,
            "mean_reversion": 2, "momentum": 1, "trend": 1, "pattern": 1,
        }
        weighted_list = []
        for (name, fn, kw, bucket) in all_inds:
            w = weights_by_bucket.get(bucket, 1)
            weighted_list.extend([(name, fn, kw, bucket)] * w)
        n = min(size, len(set(x[0] for x in weighted_list)))
        sampled = []
        seen = set()
        while len(sampled) < n and len(seen) < len(set(x[0] for x in weighted_list)):
            pick = rng.choice(weighted_list)
            if pick[0] not in seen:
                sampled.append(pick)
                seen.add(pick[0])
        return sampled

    return all_inds


# Per-strategy ensemble composition: cached to be deterministic per strategy ID
_ENSEMBLE_CACHE: Dict[int, list] = {}


def get_ensemble_for_strategy(params):
    """Get (and cache) the indicator list for a given strategy ID."""
    if params.id in _ENSEMBLE_CACHE:
        return _ENSEMBLE_CACHE[params.id]

    mode = "stratified"
    size = 6
    if params.indicator == "ensemble_uniform":
        mode, size = "uniform", 10
    elif params.indicator == "ensemble_weighted":
        mode, size = "weighted", 15
    elif params.indicator == "ensemble_super":
        mode, size = "stratified", 8
    elif params.indicator == "ensemble_strat":
        mode, size = "stratified", 6
    # ── Random forests: varying size, all stratified-random ──
    elif params.indicator == "forest_15":
        mode, size = "stratified", 15
    elif params.indicator == "forest_25":
        mode, size = "stratified", 25
    elif params.indicator == "forest_25_super":
        mode, size = "stratified", 25
    elif params.indicator == "forest_35":
        mode, size = "stratified", 35
    elif params.indicator == "forest_45":
        mode, size = "stratified", 45
    # ── Meta-gated variants: same forest compositions, meta filter ──
    elif params.indicator == "forest_25_meta":
        mode, size = "stratified", 25
    elif params.indicator == "forest_15_meta":
        mode, size = "stratified", 15
    elif params.indicator == "forest_35_meta":
        mode, size = "stratified", 35

    inds = build_indicator_list(mode=mode, size=size, seed=params.id)
    _ENSEMBLE_CACHE[params.id] = inds
    return inds


def compute_ensemble_signal(params, prices):
    """Majority voting across sub-indicators. entry_param = required fraction (0.5-1.0)."""
    inds = get_ensemble_for_strategy(params)
    if not inds:
        return None

    votes_buy = 0
    votes_sell = 0
    votes_total = 0
    for name, fn, kw, bucket in inds:
        try:
            sig = fn(prices, **kw)
        except Exception:
            sig = None
        votes_total += 1
        if sig == "buy":
            votes_buy += 1
        elif sig == "sell":
            votes_sell += 1

    if votes_total == 0:
        return None

    # Required majority fraction = entry_param (e.g. 0.6 = 60% must agree)
    threshold_votes = params.entry_param * votes_total

    if votes_buy >= threshold_votes and votes_buy > votes_sell:
        return "buy"
    if votes_sell >= threshold_votes and votes_sell > votes_buy:
        return "sell"
    return None


# ─── Signal dispatch per indicator ───────────────────────────────────────────

def compute_signal(params: StrategyParams, prices: List[float]):
    """Returns 'buy', 'sell', or None.
    'buy' = open YES long, 'sell' = open NO long."""
    if len(prices) < max(params.period + 2, 5):
        return None
    ind = params.indicator

    if ind == "mean_rev_ema":
        e = ema(prices, params.period)
        if e <= 0:
            return None
        dev = (prices[-1] - e) / e
        if dev < -params.entry_param:
            return "buy"
        if dev > params.entry_param:
            return "sell"

    elif ind == "mean_rev_sma":
        s = sma(prices, params.period)
        if s <= 0:
            return None
        dev = (prices[-1] - s) / s
        if dev < -params.entry_param:
            return "buy"
        if dev > params.entry_param:
            return "sell"

    elif ind == "momentum":
        mom = momentum(prices, params.period)
        if mom > params.entry_param:
            return "buy"
        if mom < -params.entry_param:
            return "sell"

    elif ind == "rsi":
        r = rsi(prices, params.period)
        if r < params.entry_param:
            return "buy"  # oversold
        if r > params.exit_param:
            return "sell"  # overbought (will flip for NO)

    elif ind == "bollinger":
        lo, mid, hi = bollinger(prices, params.period, params.entry_param)
        cur = prices[-1]
        if cur < lo:
            return "buy"
        if cur > hi:
            return "sell"

    elif ind == "zscore":
        z = zscore(prices, params.period)
        if z < -params.entry_param:
            return "buy"
        if z > params.entry_param:
            return "sell"

    elif ind == "macd":
        line, sig, hist = macd(prices, fast=12, slow=params.period, signal=9)
        # Histogram > entry_param = strong positive momentum
        if hist > params.entry_param:
            return "buy"
        if hist < -params.entry_param:
            return "sell"

    elif ind == "breakout":
        b = breakout(prices, params.period)
        if b == 1:
            return "buy"  # breakout up
        if b == -1:
            return "sell"  # breakdown

    elif ind == "wavelet_mr":
        # Mean reversion on wavelet-smoothed trend
        # period = wavelet level (1,2,3,4)
        # entry_param = divergence threshold
        div = wavelet_divergence(prices, level=params.period)
        if div < -params.entry_param:
            return "buy"
        if div > params.entry_param:
            return "sell"

    elif ind == "wavelet_ms":
        # Multi-scale wavelet: signal only when all scales agree
        # period = max level to check
        # entry_param = threshold for each scale
        divs = wavelet_multiscale(prices, levels=range(1, params.period + 1))
        if len(divs) < 2:
            return None
        if all(d < -params.entry_param for d in divs):
            return "buy"
        if all(d > params.entry_param for d in divs):
            return "sell"

    elif ind == "hybrid_wbz_all":
        # period = wavelet level (1-4)
        # entry_param = wavelet threshold (0.005-0.03)
        # BB(20, std=2.0) and zscore(20, thresh=2.0) are fixed — these are the
        # validated defaults from the top performers. Only wavelet tuned.
        return sub_hybrid_wbz_all(prices,
            wv_level=params.period, wv_thresh=params.entry_param,
            bb_period=20, bb_std=2.0, z_period=20, z_thresh=2.0)

    elif ind == "hybrid_wbz_2of3":
        return sub_hybrid_wbz_2of3(prices,
            wv_level=params.period, wv_thresh=params.entry_param,
            bb_period=20, bb_std=2.0, z_period=20, z_thresh=2.0)

    elif ind.startswith("ensemble") or ind.startswith("forest"):
        return compute_ensemble_signal(params, prices)

    return None


def should_exit(params: StrategyParams, pos: dict, prices: List[float]):
    """Returns reason string or None."""
    if len(prices) < 2:
        return None
    ind = params.indicator
    cur = prices[-1]
    side = pos["side"]

    # Signal reversal exits
    if ind == "mean_rev_ema":
        e = ema(prices, params.period)
        if e <= 0:
            return None
        dev = (cur - e) / e
        if side == "YES" and dev > params.exit_param:
            return "revert_exit"
        if side == "NO" and dev < -params.exit_param:
            return "revert_exit"

    elif ind == "mean_rev_sma":
        s = sma(prices, params.period)
        if s <= 0:
            return None
        dev = (cur - s) / s
        if side == "YES" and dev > params.exit_param:
            return "revert_exit"
        if side == "NO" and dev < -params.exit_param:
            return "revert_exit"

    elif ind == "rsi":
        r = rsi(prices, params.period)
        if side == "YES" and r > params.exit_param:
            return "rsi_overbought"
        if side == "NO" and r < params.entry_param:
            return "rsi_oversold"

    elif ind == "bollinger":
        lo, mid, hi = bollinger(prices, params.period, params.entry_param)
        if side == "YES" and cur > mid:
            return "bb_mid_cross"
        if side == "NO" and cur < mid:
            return "bb_mid_cross"

    elif ind == "zscore":
        z = zscore(prices, params.period)
        if side == "YES" and z > 0:
            return "z_mean_cross"
        if side == "NO" and z < 0:
            return "z_mean_cross"

    elif ind == "macd":
        line, sig, hist = macd(prices, fast=12, slow=params.period, signal=9)
        if side == "YES" and hist < 0:
            return "macd_flip"
        if side == "NO" and hist > 0:
            return "macd_flip"

    elif ind == "momentum":
        mom = momentum(prices, params.period)
        if side == "YES" and mom < -params.exit_param:
            return "mom_flip"
        if side == "NO" and mom > params.exit_param:
            return "mom_flip"

    # Breakout: time-based exit after 20 ticks
    elif ind == "breakout":
        return None  # only SL/TP

    elif ind in ("wavelet_mr", "wavelet_ms"):
        # Exit when divergence mean-reverts (crosses 0)
        div = wavelet_divergence(prices, level=params.period)
        if side == "YES" and div > 0:
            return "wv_mean_cross"
        if side == "NO" and div < 0:
            return "wv_mean_cross"

    elif ind.startswith("ensemble") or ind.startswith("forest"):
        # Exit when signal flips (majority now says opposite)
        sig = compute_ensemble_signal(params, prices)
        if side == "YES" and sig == "sell":
            return "ensemble_flip"
        if side == "NO" and sig == "buy":
            return "ensemble_flip"

    elif ind == "hybrid_wbz_all":
        sig = sub_hybrid_wbz_all(prices, wv_level=params.period,
            wv_thresh=params.entry_param, bb_period=20, bb_std=2.0,
            z_period=20, z_thresh=2.0)
        if side == "YES" and sig == "sell":
            return "hybrid_flip"
        if side == "NO" and sig == "buy":
            return "hybrid_flip"

    elif ind == "hybrid_wbz_2of3":
        sig = sub_hybrid_wbz_2of3(prices, wv_level=params.period,
            wv_thresh=params.entry_param, bb_period=20, bb_std=2.0,
            z_period=20, z_thresh=2.0)
        if side == "YES" and sig == "sell":
            return "hybrid_flip"
        if side == "NO" and sig == "buy":
            return "hybrid_flip"

    return None


# ─── Arena ───────────────────────────────────────────────────────────────────

class Arena:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.markets: Dict[str, TrackedMarket] = {}
        self.last_scan = 0.0
        self.last_settle = 0.0
        self.last_leaderboard = 0.0
        self.last_save = 0.0
        self.last_tick_flush = 0.0
        self.last_prune = 0.0

        self.strategies = [StrategyState(p) for p in ALL_STRATEGIES]
        self.tick_buffer: List[dict] = []
        self.blacklist: Dict[str, dict] = {}

        self._load_blacklist()
        self._load_state()
        self._apply_blacklist()
        logger.info(f"Arena initialized with {len(self.strategies)} strategies")

    def _load_blacklist(self):
        if not os.path.exists(BLACKLIST_FILE):
            return
        try:
            with open(BLACKLIST_FILE) as f:
                d = json.load(f)
            self.blacklist = {e["sig"]: e for e in d.get("banned", [])}
            logger.info(f"Loaded {len(self.blacklist)} banned strategy signatures")
        except Exception as e:
            logger.error(f"blacklist load failed: {e}")

    def _save_blacklist(self):
        try:
            with open(BLACKLIST_FILE, "w") as f:
                json.dump({"updated_at": datetime.now(timezone.utc).isoformat(),
                           "banned": list(self.blacklist.values())}, f, indent=1)
        except Exception as e:
            logger.error(f"blacklist save failed: {e}")

    def _apply_blacklist(self):
        n = 0
        for strat in self.strategies:
            if strat.retired:
                continue
            sig = strategy_signature(strat.params)
            if sig in self.blacklist:
                strat.retired = True
                strat.retired_at = self.blacklist[sig].get("banned_at")
                n += 1
        if n:
            logger.info(f"Pre-retired {n} strategies via blacklist match")

    def _add_to_blacklist(self, strat, reason):
        sig = strategy_signature(strat.params)
        if sig in self.blacklist:
            return
        self.blacklist[sig] = {
            "sig": sig,
            "reason": reason,
            "name": strat.params.name,
            "trades_at_ban": strat.total_trades,
            "equity_at_ban": round(strat.equity, 2),
            "banned_at": datetime.now(timezone.utc).isoformat(),
        }

    def _load_state(self):
        """Restore equity/trades/history from previous arena_results.json."""
        if not os.path.exists(RESULTS_FILE):
            return
        try:
            with open(RESULTS_FILE) as f:
                data = json.load(f)
            prev = {r["id"]: r for r in data.get("results", [])}
            restored = 0
            for strat in self.strategies:
                old = prev.get(strat.params.id)
                if not old:
                    continue
                # FIX: on restart, positions are lost. Use equity (not balance)
                # so cash locked in positions returns to balance.
                strat.balance = old.get("equity", old.get("balance", STARTING_BALANCE))
                strat.total_fees = old.get("fees", 0.0)
                strat.total_trades = old.get("trades", 0)
                strat.retired = old.get("retired", False)
                strat.retired_at = old.get("retired_at")
                # Restore peak_equity for DD-based retire (default to current eq if missing)
                strat.peak_equity = old.get("peak_equity", max(strat.equity, STARTING_BALANCE))
                # Reconstruct history stubs from W/L counts
                w, l = old.get("wins", 0), old.get("losses", 0)
                realized = old.get("realized", 0)
                if w + l > 0:
                    avg_win = realized / w if w else 0
                    avg_loss = realized / l if l else 0
                    strat.history = (
                        [{"pnl": max(avg_win, 0.01), "reason": "restored"} for _ in range(w)]
                        + [{"pnl": min(avg_loss, -0.01), "reason": "restored"} for _ in range(l)]
                    )
                restored += 1
            logger.info(f"Restored state for {restored} strategies")
        except Exception as e:
            logger.warning(f"Could not restore state: {e}")

    async def scan_markets(self):
        now = time.time()
        if now - self.last_scan < MARKET_SCAN_INTERVAL and self.markets:
            return
        self.last_scan = now
        try:
            r = await self.client.get(f"{GAMMA_URL}/markets", params={
                "active": "true", "closed": "false", "limit": 100,
                "order": "volume24hr", "ascending": "false"})
            r.raise_for_status()
            raw = r.json()
        except Exception as e:
            logger.error(f"Market scan failed: {e}")
            return

        found = 0
        for m in raw:
            market_id = m.get("id", "")
            if market_id in self.markets:
                continue
            tokens = m.get("clobTokenIds", "[]")
            if isinstance(tokens, str):
                try:
                    tokens = json.loads(tokens)
                except Exception:
                    continue
            if len(tokens) < 2:
                continue
            vol = float(m.get("volume24hr", 0) or 0)
            if vol < MIN_VOLUME_24H:
                continue
            end = m.get("endDate", "")
            if end:
                try:
                    if datetime.fromisoformat(end.replace("Z", "+00:00")) < datetime.now(timezone.utc):
                        continue
                except Exception:
                    pass
            try:
                r_mid = await self.client.get(f"{CLOB_URL}/midpoint", params={"token_id": tokens[0]})
                mid = float(r_mid.json().get("mid", 0))
                if mid < MIN_MID_PRICE or mid > MAX_MID_PRICE:
                    continue
                r_buy = await self.client.get(f"{CLOB_URL}/price", params={"token_id": tokens[0], "side": "buy"})
                r_sell = await self.client.get(f"{CLOB_URL}/price", params={"token_id": tokens[0], "side": "sell"})
                bid = float(r_buy.json().get("price", 0))
                ask = float(r_sell.json().get("price", 0))
                spread = ask - bid
                if spread < MIN_SPREAD or spread > MAX_SPREAD:
                    continue
            except Exception:
                continue

            fees_on = m.get("feesEnabled", False)
            ft = m.get("feeType") if fees_on else None
            self.markets[market_id] = TrackedMarket(
                market_id=market_id, question=m.get("question", ""),
                token_yes=str(tokens[0]), token_no=str(tokens[1]),
                end_date=end, volume_24h=vol,
                fees_enabled=fees_on, fee_type=ft,
            )
            found += 1

        # Prune expired
        now_dt = datetime.now(timezone.utc)
        expired = []
        for mid, mkt in self.markets.items():
            if mkt.end_date:
                try:
                    if datetime.fromisoformat(mkt.end_date.replace("Z", "+00:00")) < now_dt:
                        expired.append(mid)
                except Exception:
                    pass
        for mid in expired:
            del self.markets[mid]

        fee_free = sum(1 for m in self.markets.values() if not m.fees_enabled)
        logger.info(
            f"Tracking {len(self.markets)} markets (+{found}, -{len(expired)}) "
            f"| {fee_free} free, {len(self.markets)-fee_free} fees"
        )

    async def poll_prices(self):
        tasks = [self._poll_one(mid) for mid in list(self.markets.keys())]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _poll_one(self, market_id):
        mkt = self.markets.get(market_id)
        if not mkt:
            return
        try:
            r_buy = await self.client.get(f"{CLOB_URL}/price",
                params={"token_id": mkt.token_yes, "side": "buy"})
            r_sell = await self.client.get(f"{CLOB_URL}/price",
                params={"token_id": mkt.token_yes, "side": "sell"})
            bid = float(r_buy.json().get("price", 0))
            ask = float(r_sell.json().get("price", 0))
            if bid <= 0 or ask <= 0:
                return
            mid_price = (bid + ask) / 2.0
        except Exception:
            return

        tick = PriceTick(ts=time.time(), mid=mid_price, bid=bid, ask=ask)
        mkt.ticks.append(tick)

        # Buffer for persistence
        self.tick_buffer.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "market_id": market_id,
            "mid": round(mid_price, 4),
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "fees": mkt.fees_enabled,
        })

    def flush_ticks(self):
        now = time.time()
        if now - self.last_tick_flush < TICK_FLUSH_INTERVAL:
            return
        self.last_tick_flush = now
        if not self.tick_buffer:
            return
        try:
            with open(TICKS_FILE, "a") as f:
                for t in self.tick_buffer:
                    f.write(json.dumps(t) + "\n")
            n = len(self.tick_buffer)
            self.tick_buffer.clear()
            logger.debug(f"Flushed {n} ticks")
        except Exception as e:
            logger.error(f"Tick flush failed: {e}")

    def tick_all_strategies(self):
        for strat in self.strategies:
            if strat.retired:
                continue
            for mid, mkt in self.markets.items():
                if len(mkt.ticks) < 5:
                    continue
                last = mkt.ticks[-1]
                prices = [t.mid for t in mkt.ticks]

                fee_type = mkt.fee_type if mkt.fees_enabled else None
                if strat.params.fee_free_only and mkt.fees_enabled:
                    if mid not in strat.positions:
                        continue

                # Exit logic
                if mid in strat.positions:
                    pos = strat.positions[mid]
                    entry = pos["entry_price"]
                    current = last.mid if pos["side"] == "YES" else (1.0 - last.mid)
                    if entry > 0:
                        pnl_pct = (current - entry) / entry
                        if strat.params.stop_loss > -0.90 and pnl_pct <= strat.params.stop_loss:
                            strat.close_position(mid, current, "stop_loss")
                            continue
                        if pnl_pct >= strat.params.take_profit:
                            strat.close_position(mid, current, "take_profit")
                            continue
                        exit_reason = should_exit(strat.params, pos, prices)
                        if exit_reason:
                            strat.close_position(mid, current, exit_reason)
                            continue
                    continue

                # Entry logic
                signal = compute_signal(strat.params, prices)
                if not signal:
                    continue

                # Side bias filter
                if strat.params.side_bias == "long" and signal != "buy":
                    continue
                if strat.params.side_bias == "short" and signal != "sell":
                    continue

                # Spread / fee viability
                spread_pct = (last.ask - last.bid) / last.mid if last.mid > 0 else 1
                if spread_pct > 0.10:
                    continue
                rt_fee = calc_taker_fee(1.0, last.mid, fee_type) * 2
                if rt_fee > 0.03:
                    continue

                # ── Meta-labeling gate (only for *_meta strategies) ──
                if strat.params.indicator.endswith("_meta"):
                    try:
                        from meta_predict import should_trade as _meta_should_trade
                        entry_price = last.mid if signal == "buy" else (1.0 - last.mid)
                        # Use base indicator (strip _meta) for lookup
                        base_ind = strat.params.indicator.replace("_meta", "")
                        side_str = "YES" if signal == "buy" else "NO"
                        allow, reason, info = _meta_should_trade(
                            base_ind, side_str, entry_price,
                            has_fee=mkt.fees_enabled, min_wr=0.55,
                            require_bucket=False,
                        )
                        if not allow:
                            continue  # meta blocked this trade
                    except Exception:
                        pass  # model not available → pass through

                if signal == "buy":
                    strat.open_position(mid, mkt.question, "YES", mkt.token_yes,
                        last.mid, fee_type, f"{strat.params.indicator}_buy")
                else:
                    strat.open_position(mid, mkt.question, "NO", mkt.token_no,
                        1.0 - last.mid, fee_type, f"{strat.params.indicator}_sell")

    async def check_settlements(self):
        now = time.time()
        if now - self.last_settle < SETTLE_CHECK_INTERVAL:
            return
        self.last_settle = now
        markets_with_pos = set()
        for strat in self.strategies:
            markets_with_pos.update(strat.positions.keys())
        for mid in markets_with_pos:
            try:
                r = await self.client.get(f"{GAMMA_URL}/markets/{mid}")
                if r.status_code != 200:
                    continue
                m = r.json()
                if not m.get("closed", False) and m.get("active", True):
                    continue
                prices = m.get("outcomePrices", [])
                if isinstance(prices, str):
                    try:
                        prices = json.loads(prices)
                    except Exception:
                        continue
                if len(prices) < 2:
                    continue
                p_yes = float(prices[0])
                p_no = float(prices[1])
                for strat in self.strategies:
                    if mid in strat.positions:
                        side = strat.positions[mid]["side"]
                        strat.close_position(mid, p_yes if side == "YES" else p_no, "settlement")
            except Exception:
                pass

    def prune_dead(self):
        now = time.time()
        if now - self.last_prune < PRUNE_INTERVAL:
            return
        self.last_prune = now
        newly_dead = 0
        for strat in self.strategies:
            if strat.retired:
                continue
            # Refresh peak in case current equity is higher than tracked
            if strat.equity > strat.peak_equity:
                strat.peak_equity = strat.equity
            if strat.total_trades < PRUNE_MIN_TRADES:
                continue
            reason = None
            # Rule A: absolute floor — lost 50% of starting capital
            if strat.equity < PRUNE_DEAD_EQUITY:
                reason = f"equity ${strat.equity:.0f} < ${PRUNE_DEAD_EQUITY:.0f} (absolute floor)"
            # Rule B: peak-relative drawdown — lost 50% of peak (catches emergent losers
            # who briefly went profitable then crashed, e.g. S431 ensemble peaked $911 → $611)
            elif (strat.peak_equity > STARTING_BALANCE * 1.05
                  and strat.equity < strat.peak_equity * 0.5):
                reason = (f"DD ${strat.peak_equity:.0f} → ${strat.equity:.0f} "
                          f"({(1-strat.equity/strat.peak_equity)*100:.0f}% from peak)")
            if reason:
                strat.retired = True
                strat.retired_at = datetime.now(timezone.utc).isoformat()
                for mkt_id in list(strat.positions.keys()):
                    pos = strat.positions[mkt_id]
                    strat.balance += pos["cost_usd"]
                    del strat.positions[mkt_id]
                self._add_to_blacklist(strat, reason)
                newly_dead += 1
        if newly_dead:
            self._save_blacklist()
            active = sum(1 for s in self.strategies if not s.retired)
            logger.info(f"Pruned {newly_dead} dead strategies | {active} active | blacklist={len(self.blacklist)}")

    def print_leaderboard(self):
        now = time.time()
        if now - self.last_leaderboard < LEADERBOARD_INTERVAL:
            return
        self.last_leaderboard = now
        active = [s for s in self.strategies if not s.retired]
        ranked = sorted(active, key=lambda s: s.equity, reverse=True)
        total_trades = sum(s.total_trades for s in self.strategies)
        logger.info(f"{'='*80}")
        logger.info(f"LEADERBOARD | {len(active)} active / {len(self.strategies)} total "
                    f"| {total_trades} trades | {len(self.markets)} markets")
        logger.info(f"{'='*80}")
        logger.info("TOP 10:")
        for i, s in enumerate(ranked[:10]):
            wr = (s.wins / (s.wins + s.losses) * 100) if (s.wins + s.losses) else 0
            logger.info(f"  #{i+1:>2} {s.params.name:<48} "
                        f"Eq=${s.equity:>8.2f} ({s.pnl:>+7.2f}) | "
                        f"W/L {s.wins}/{s.losses} ({wr:.0f}%) | Pos={len(s.positions)}")
        logger.info("BOTTOM 3:")
        for i, s in enumerate(ranked[-3:]):
            wr = (s.wins / (s.wins + s.losses) * 100) if (s.wins + s.losses) else 0
            logger.info(f"  #{len(ranked)-2+i:>3} {s.params.name:<48} "
                        f"Eq=${s.equity:>8.2f} ({s.pnl:>+7.2f}) W/L {s.wins}/{s.losses}")
        equities = [s.equity for s in active]
        if equities:
            avg = sum(equities) / len(equities)
            profit = sum(1 for e in equities if e > STARTING_BALANCE)
            logger.info(f"AVG=${avg:.2f} | Profitable={profit}/{len(active)} "
                        f"| Best=${max(equities):.2f} Worst=${min(equities):.2f}")
        logger.info("="*80)

    def save_results(self):
        now = time.time()
        if now - self.last_save < LEADERBOARD_INTERVAL:
            return
        self.last_save = now
        ranked = sorted(self.strategies, key=lambda s: s.equity, reverse=True)
        try:
            with open(RESULTS_FILE, "w") as f:
                json.dump({
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "strategies": len(ranked),
                    "active": sum(1 for s in ranked if not s.retired),
                    "markets_tracked": len(self.markets),
                    "results": [s.to_dict() for s in ranked],
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Save failed: {e}")

    async def run(self):
        logger.info("Arena v2 starting...")
        while True:
            try:
                await self.scan_markets()
                await self.poll_prices()
                self.flush_ticks()
                self.tick_all_strategies()
                await self.check_settlements()
                self.prune_dead()
                self.print_leaderboard()
                self.save_results()
            except Exception as e:
                logger.error(f"Loop error: {e}", exc_info=True)
            await asyncio.sleep(PRICE_POLL_INTERVAL)

    async def cleanup(self):
        await self.client.aclose()


async def main():
    arena = Arena()
    try:
        await arena.run()
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        arena.save_results()
        arena.flush_ticks()
        await arena.cleanup()
        logger.info("Arena shutdown.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
