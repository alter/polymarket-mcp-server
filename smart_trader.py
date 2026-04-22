#!/usr/bin/env python3
"""
Smart Paper Trading Bot for Polymarket.

Strategy: Mean-reversion on liquid markets with tight spreads.
- Scans top-volume markets every 15 minutes
- Tracks midpoint prices every 30 seconds
- Buys when price drops >2% below 5-min EMA, sells when >2% above
- Realistic fee simulation (taker ~0.2%, maker 0%)
- Risk: max $50/position, max 20 positions, stop-loss -8%, take-profit +5%
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("SmartTrader")

# ── API endpoints ──
GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"

# ── Portfolio config ──
STARTING_BALANCE = 1000.0
MAX_POSITION_USD = 50.0
MAX_POSITIONS = 20
STOP_LOSS_PCT = -0.15       # -15% (accounts for spread at entry)
TAKE_PROFIT_PCT = 0.08      # +8%
MIN_SPREAD = 0.001           # 0.1 cent — ignore markets with zero spread
MAX_SPREAD = 0.05            # 5 cents
MIN_VOLUME_24H = 10_000
MIN_MID_PRICE = 0.10
MAX_MID_PRICE = 0.90

# ── Signal config ──
EMA_FAST = 10                # 10 ticks = ~5 min at 30s interval
EMA_SLOW = 60                # 60 ticks = ~30 min
ENTRY_DEVIATION = 0.020      # 2% below EMA → buy signal
EXIT_DEVIATION = 0.015       # 1.5% above EMA → sell signal
MIN_TICKS_FOR_SIGNAL = 12    # need at least 12 price points

# ── Timing ──
PRICE_POLL_INTERVAL = 30     # seconds
MARKET_SCAN_INTERVAL = 900   # 15 minutes
SETTLE_CHECK_INTERVAL = 300  # 5 minutes

DATA_DIR = "data"
PORTFOLIO_FILE = os.path.join(DATA_DIR, "smart_portfolio.json")
PRICE_LOG_FILE = os.path.join(DATA_DIR, "smart_prices.jsonl")

# ── Fee rates by feeType (from Polymarket docs) ──
# Formula: fee = shares × feeRate × p × (1 - p)
# Effective fee as % of USD spent = feeRate × (1 - p)
FEE_RATES = {
    "sports_fees_v2": 0.03,    # 3% → max ~0.75% effective at p=0.50
    "culture_fees": 0.05,      # 5% → max ~1.25% effective at p=0.50
    "finance_fees": 0.04,      # 4%
    "politics_fees": 0.04,     # 4%
    "economics_fees": 0.05,    # 5%
    "crypto_fees": 0.072,      # 7.2% → max ~1.8% effective at p=0.50
    "mentions_fees": 0.04,     # 4%
    "tech_fees": 0.04,         # 4%
}


def calc_taker_fee(size_usd: float, price: float, fee_type: Optional[str]) -> float:
    """Calculate taker fee: fee = (size_usd / price) × feeRate × price × (1 - price)
    Simplifies to: fee = size_usd × feeRate × (1 - price)"""
    if not fee_type:
        return 0.0
    rate = FEE_RATES.get(fee_type, 0.05)  # default 5% for unknown types
    return size_usd * rate * (1.0 - price)


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
    ticks: deque = field(default_factory=lambda: deque(maxlen=200))
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    ema_initialized: bool = False

    def update_ema(self, price: float):
        if not self.ema_initialized:
            self.ema_fast = price
            self.ema_slow = price
            self.ema_initialized = True
            return
        alpha_fast = 2.0 / (EMA_FAST + 1)
        alpha_slow = 2.0 / (EMA_SLOW + 1)
        self.ema_fast = alpha_fast * price + (1 - alpha_fast) * self.ema_fast
        self.ema_slow = alpha_slow * price + (1 - alpha_slow) * self.ema_slow


class Portfolio:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.balance: float = STARTING_BALANCE
        self.positions: Dict[str, dict] = {}   # market_id → position
        self.history: List[dict] = []
        self.total_fees_paid: float = 0.0
        self.total_trades: int = 0
        self.load()

    def load(self):
        if os.path.exists(PORTFOLIO_FILE):
            with open(PORTFOLIO_FILE) as f:
                d = json.load(f)
                self.balance = d.get("balance", STARTING_BALANCE)
                self.positions = d.get("positions", {})
                self.history = d.get("history", [])
                self.total_fees_paid = d.get("total_fees_paid", 0.0)
                self.total_trades = d.get("total_trades", 0)

    def save(self):
        with open(PORTFOLIO_FILE, "w") as f:
            json.dump({
                "balance": round(self.balance, 4),
                "positions": self.positions,
                "history": self.history,
                "total_fees_paid": round(self.total_fees_paid, 6),
                "total_trades": self.total_trades,
            }, f, indent=2)

    def open_position(
        self, market_id: str, question: str, side: str,
        token_id: str, price: float, size_usd: float, reason: str,
        fee_type: Optional[str] = None,
    ):
        fee = calc_taker_fee(size_usd, price, fee_type)
        cost = size_usd + fee
        if cost > self.balance:
            logger.warning(f"Insufficient balance: need ${cost:.2f}, have ${self.balance:.2f}")
            return False
        if len(self.positions) >= MAX_POSITIONS:
            logger.warning("Max positions reached")
            return False
        if market_id in self.positions:
            return False

        shares = size_usd / price
        self.balance -= cost
        self.total_fees_paid += fee
        self.total_trades += 1
        self.positions[market_id] = {
            "market_id": market_id,
            "question": question,
            "side": side,
            "token_id": token_id,
            "entry_price": price,
            "shares": shares,
            "cost_usd": cost,
            "fee_type": fee_type,
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
        }
        self.save()
        logger.info(
            f"OPEN {side} | {question[:45]}... @ {price:.3f} | "
            f"${size_usd:.2f} ({shares:.1f} shares) | fee=${fee:.4f} | reason={reason}"
        )
        return True

    def close_position(self, market_id: str, exit_price: float, reason: str):
        if market_id not in self.positions:
            return
        pos = self.positions[market_id]
        gross = pos["shares"] * exit_price
        fee = calc_taker_fee(gross, exit_price, pos.get("fee_type"))
        net = gross - fee
        pnl = net - pos["cost_usd"]
        pnl_pct = pnl / pos["cost_usd"] * 100

        self.balance += net
        self.total_fees_paid += fee
        self.total_trades += 1

        self.history.append({
            "market_id": market_id,
            "question": pos["question"],
            "side": pos["side"],
            "entry": pos["entry_price"],
            "exit": exit_price,
            "pnl": round(pnl, 4),
            "pnl_pct": round(pnl_pct, 2),
            "reason": reason,
            "closed_at": datetime.now(timezone.utc).isoformat(),
        })
        del self.positions[market_id]
        self.save()

        tag = "PROFIT" if pnl >= 0 else "LOSS"
        logger.info(
            f"CLOSE [{tag}] | {pos['question'][:45]}... @ {exit_price:.3f} | "
            f"PnL=${pnl:+.4f} ({pnl_pct:+.1f}%) | reason={reason}"
        )

    def summary(self) -> str:
        realized = sum(h["pnl"] for h in self.history)
        wins = sum(1 for h in self.history if h["pnl"] > 0)
        losses = sum(1 for h in self.history if h["pnl"] <= 0)
        unrealized = 0.0  # calculated externally
        return (
            f"Balance=${self.balance:.2f} | Positions={len(self.positions)} | "
            f"Realized=${realized:+.2f} | Trades={self.total_trades} | "
            f"W/L={wins}/{losses} | Fees=${self.total_fees_paid:.4f}"
        )


class SmartTrader:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=15.0)
        self.portfolio = Portfolio()
        self.markets: Dict[str, TrackedMarket] = {}
        self.last_scan = 0.0
        self.last_settle = 0.0

    # ── Market Discovery ──

    async def scan_markets(self):
        """Find liquid markets with tight spreads."""
        now = time.time()
        if now - self.last_scan < MARKET_SCAN_INTERVAL and self.markets:
            return
        self.last_scan = now

        logger.info("Scanning for tradeable markets...")
        try:
            r = await self.client.get(f"{GAMMA_URL}/markets", params={
                "active": "true", "closed": "false", "limit": 100,
                "order": "volume24hr", "ascending": "false",
            })
            r.raise_for_status()
            raw_markets = r.json()
        except Exception as e:
            logger.error(f"Market scan failed: {e}")
            return

        found = 0
        for m in raw_markets:
            mid = await self._get_market_mid(m)
            if mid is None:
                continue
            market_id = m.get("id", "")
            if market_id not in self.markets:
                tokens = self._parse_tokens(m)
                if not tokens:
                    continue
                fees_on = m.get("feesEnabled", False)
                ft = m.get("feeType") if fees_on else None
                self.markets[market_id] = TrackedMarket(
                    market_id=market_id,
                    question=m.get("question", ""),
                    token_yes=tokens[0],
                    token_no=tokens[1],
                    end_date=m.get("endDate", ""),
                    volume_24h=float(m.get("volume24hr", 0) or 0),
                    fees_enabled=fees_on,
                    fee_type=ft,
                )
                found += 1

        # Prune markets that ended
        now_dt = datetime.now(timezone.utc)
        expired = [
            mid for mid, mkt in self.markets.items()
            if mkt.end_date and self._parse_dt(mkt.end_date) and self._parse_dt(mkt.end_date) < now_dt
            and mid not in self.portfolio.positions
        ]
        for mid in expired:
            del self.markets[mid]

        fee_free = sum(1 for m in self.markets.values() if not m.fees_enabled)
        logger.info(
            f"Tracking {len(self.markets)} markets (+{found} new, -{len(expired)} expired) "
            f"| {fee_free} fee-free, {len(self.markets)-fee_free} with fees"
        )

    async def _get_market_mid(self, m: dict) -> Optional[float]:
        """Check if market is tradeable; return midpoint or None."""
        vol = float(m.get("volume24hr", 0) or 0)
        if vol < MIN_VOLUME_24H:
            return None

        tokens = self._parse_tokens(m)
        if not tokens:
            return None

        # Check end date is in future
        end = m.get("endDate")
        if end:
            end_dt = self._parse_dt(end)
            if end_dt and end_dt < datetime.now(timezone.utc):
                return None

        try:
            r_mid = await self.client.get(f"{CLOB_URL}/midpoint", params={"token_id": tokens[0]})
            mid = float(r_mid.json().get("mid", 0))
        except Exception:
            return None

        if mid < MIN_MID_PRICE or mid > MAX_MID_PRICE:
            return None

        # Check spread
        try:
            r_buy = await self.client.get(f"{CLOB_URL}/price", params={"token_id": tokens[0], "side": "buy"})
            r_sell = await self.client.get(f"{CLOB_URL}/price", params={"token_id": tokens[0], "side": "sell"})
            bid = float(r_buy.json().get("price", 0))
            ask = float(r_sell.json().get("price", 0))
            spread = ask - bid
        except Exception:
            return None

        if spread < MIN_SPREAD or spread > MAX_SPREAD:
            return None

        return mid

    def _parse_tokens(self, m: dict) -> Optional[List[str]]:
        tokens = m.get("clobTokenIds", "[]")
        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except Exception:
                return None
        if len(tokens) < 2:
            return None
        return [str(tokens[0]), str(tokens[1])]

    def _parse_dt(self, s: str) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

    # ── Price Polling ──

    async def poll_prices(self):
        """Fetch current prices for all tracked markets."""
        tasks = []
        market_ids = list(self.markets.keys())
        for mid in market_ids:
            tasks.append(self._poll_one(mid))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _poll_one(self, market_id: str):
        mkt = self.markets.get(market_id)
        if not mkt:
            return
        try:
            r_buy = await self.client.get(f"{CLOB_URL}/price", params={"token_id": mkt.token_yes, "side": "buy"})
            r_sell = await self.client.get(f"{CLOB_URL}/price", params={"token_id": mkt.token_yes, "side": "sell"})
            bid = float(r_buy.json().get("price", 0))
            ask = float(r_sell.json().get("price", 0))
            if bid <= 0 or ask <= 0:
                return
            mid = (bid + ask) / 2.0
        except Exception:
            return

        tick = PriceTick(ts=time.time(), mid=mid, bid=bid, ask=ask)
        mkt.ticks.append(tick)
        mkt.update_ema(mid)

        # Log to file
        try:
            with open(PRICE_LOG_FILE, "a") as f:
                f.write(json.dumps({
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "market": market_id,
                    "bid": bid, "ask": ask, "mid": mid,
                    "ema_f": round(mkt.ema_fast, 4),
                    "ema_s": round(mkt.ema_slow, 4),
                }) + "\n")
        except Exception:
            pass

    # ── Signal Generation ──

    def generate_signals(self) -> List[dict]:
        """Check all tracked markets for entry/exit signals."""
        signals = []
        for mid, mkt in self.markets.items():
            if len(mkt.ticks) < MIN_TICKS_FOR_SIGNAL:
                continue
            if not mkt.ema_initialized:
                continue

            last = mkt.ticks[-1]
            price = last.mid
            ema = mkt.ema_fast

            # Deviation from fast EMA
            if ema <= 0:
                continue
            dev = (price - ema) / ema

            # ── Entry signals ──
            if mid not in self.portfolio.positions:
                # BUY YES: price dropped below EMA (mean reversion → expect bounce)
                if dev < -ENTRY_DEVIATION:
                    # Only enter if spread is reasonable relative to price
                    spread_pct = (last.ask - last.bid) / last.mid if last.mid > 0 else 1
                    if spread_pct > 0.10:
                        continue  # spread >10% of price, skip
                    signals.append({
                        "type": "buy",
                        "market_id": mid,
                        "side": "YES",
                        "token": mkt.token_yes,
                        "price": last.mid,  # use mid for paper trading entry
                        "reason": f"mean_rev_buy dev={dev:+.3f} spr={spread_pct:.2f}",
                    })
                # BUY NO: price rose above EMA (mean reversion → expect drop)
                elif dev > ENTRY_DEVIATION:
                    spread_pct = (last.ask - last.bid) / last.mid if last.mid > 0 else 1
                    if spread_pct > 0.10:
                        continue
                    signals.append({
                        "type": "buy",
                        "market_id": mid,
                        "side": "NO",
                        "token": mkt.token_no,
                        "price": 1.0 - last.mid,  # use mid
                        "reason": f"mean_rev_sell dev={dev:+.3f} spr={spread_pct:.2f}",
                    })

            # ── Exit signals ──
            else:
                pos = self.portfolio.positions[mid]
                entry = pos["entry_price"]
                # Use mid for valuation (not bid) to avoid instant stop-loss from spread
                current = last.mid if pos["side"] == "YES" else (1.0 - last.mid)
                if entry <= 0:
                    continue
                pnl_pct = (current - entry) / entry

                # Stop-loss
                if pnl_pct <= STOP_LOSS_PCT:
                    signals.append({
                        "type": "close",
                        "market_id": mid,
                        "price": current,
                        "reason": f"stop_loss pnl={pnl_pct:+.3f}",
                    })
                # Take-profit
                elif pnl_pct >= TAKE_PROFIT_PCT:
                    signals.append({
                        "type": "close",
                        "market_id": mid,
                        "price": current,
                        "reason": f"take_profit pnl={pnl_pct:+.3f}",
                    })
                # Mean reversion exit: price reverted back to EMA
                elif pos["side"] == "YES" and dev > EXIT_DEVIATION:
                    signals.append({
                        "type": "close",
                        "market_id": mid,
                        "price": current,
                        "reason": f"revert_exit dev={dev:+.3f}",
                    })
                elif pos["side"] == "NO" and dev < -EXIT_DEVIATION:
                    signals.append({
                        "type": "close",
                        "market_id": mid,
                        "price": current,
                        "reason": f"revert_exit dev={dev:+.3f}",
                    })

        return signals

    # ── Execute Signals ──

    def execute_signals(self, signals: List[dict]):
        for sig in signals:
            mkt = self.markets.get(sig["market_id"])
            if not mkt:
                continue

            if sig["type"] == "buy":
                size = min(MAX_POSITION_USD, self.portfolio.balance * 0.10)
                if size < 1.0:
                    continue
                # Check if round-trip fees eat the expected edge
                fee_type = mkt.fee_type if mkt.fees_enabled else None
                entry_fee_pct = calc_taker_fee(1.0, sig["price"], fee_type)
                # Round trip cost ≈ 2× single leg
                rt_cost = entry_fee_pct * 2
                if rt_cost > 0.03:  # >3% round-trip fees → skip
                    logger.debug(f"Skipping {mkt.question[:30]}: RT fee {rt_cost:.1%} too high")
                    continue
                self.portfolio.open_position(
                    market_id=sig["market_id"],
                    question=mkt.question,
                    side=sig["side"],
                    token_id=sig["token"],
                    price=sig["price"],
                    size_usd=size,
                    reason=sig["reason"],
                    fee_type=fee_type,
                )
            elif sig["type"] == "close":
                self.portfolio.close_position(
                    market_id=sig["market_id"],
                    exit_price=sig["price"],
                    reason=sig["reason"],
                )

    # ── Settlement ──

    async def check_settlements(self):
        now = time.time()
        if now - self.last_settle < SETTLE_CHECK_INTERVAL:
            return
        self.last_settle = now

        settled = []
        for mid, pos in list(self.portfolio.positions.items()):
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

                if pos["side"] == "YES":
                    exit_price = p_yes
                else:
                    exit_price = p_no

                self.portfolio.close_position(mid, exit_price, reason="settlement")
                settled.append(mid)
            except Exception as e:
                logger.error(f"Settlement check error for {mid}: {e}")

        if settled:
            logger.info(f"Settled {len(settled)} positions")

    # ── Main Loop ──

    async def run(self):
        logger.info(f"SmartTrader starting | {self.portfolio.summary()}")
        tick_count = 0

        while True:
            try:
                # 1. Scan for new markets (every 15 min)
                await self.scan_markets()

                # 2. Poll prices
                await self.poll_prices()
                tick_count += 1

                # 3. Generate and execute signals
                signals = self.generate_signals()
                if signals:
                    self.execute_signals(signals)

                # 4. Check settlements
                await self.check_settlements()

                # 5. Periodic status
                if tick_count % 20 == 0:  # every ~10 min
                    logger.info(f"STATUS | {self.portfolio.summary()} | Tracking {len(self.markets)} markets")

            except Exception as e:
                logger.error(f"Main loop error: {e}", exc_info=True)

            await asyncio.sleep(PRICE_POLL_INTERVAL)

    async def cleanup(self):
        await self.client.aclose()


async def main():
    bot = SmartTrader()
    try:
        await bot.run()
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        await bot.cleanup()
        logger.info(f"Shutdown | {bot.portfolio.summary()}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
