#!/usr/bin/env python3
"""
Oil × Iran Causal Strategy Trader.

Based on empirical finding (47h of tick data, 104 active markets):
- Iran escalation UP → Oil HIGH targets UP (60% same dir, 101 matches, WTI LOW $80 × Trump announces)
- Iran peace UP → Oil HIGH targets DOWN (58% opposite, 102 matches)
- Iran LEADS oil by minutes (6 of 8 pairs)

Strategy:
1. Monitor Iran escalation markets (military, uranium, invade)
2. Monitor Iran peace markets (ceasefire, peace deal, hormuz normal)
3. When iran_escalation_index moves UP >1% in 5 min → open oil HIGH long
4. When iran_peace_index moves UP >1% in 5 min → open oil LOW long (oil drops)
5. Exit: 10% profit, -25% stop, or when signal flips

Paper portfolio: $1000 starting. $50 per position.
"""
import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("OilIran")

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA_DIR = "data"
PF_FILE = os.path.join(DATA_DIR, "oil_iran_portfolio.json")
POLL_INTERVAL = 30
SCAN_INTERVAL = 3600  # re-find markets hourly
MOMENTUM_WINDOW = 10  # ticks (~5 min)
ENTRY_THRESHOLD = 0.01   # 1% move
STOP_LOSS = -0.25
TAKE_PROFIT = 0.10
POSITION_USD = 50.0
STARTING_BALANCE = 1000.0

# Market classification keywords
ESCALATION_KEYWORDS = [
    "iran", "iranian", "israel", "hezbollah",
    "military operations against iran", "invade iran",
    "uranium stockpile", "enriched uranium",
    "iran conflict", "ends on",
]
PEACE_KEYWORDS = [
    "peace deal", "ceasefire", "iran permanent peace",
    "hormuz traffic returns to normal",
    "surrender enriched uranium",
]
OIL_HIGH_KEYWORDS = [
    "wti crude oil", "wti", "crude oil",
    "hit (high)", "$120",
]
OIL_LOW_KEYWORDS = [
    "hit (low)", "$70", "$80",
]


def classify(question: str) -> Optional[str]:
    """Returns 'escalation', 'peace', 'oil_high', 'oil_low' or None."""
    q = question.lower()

    # Oil markets (priority)
    if "wti" in q or "crude oil" in q:
        if "high" in q or "$120" in q or "$130" in q or "$140" in q or "$150" in q:
            return "oil_high"
        if "low" in q or "$70" in q or "$80" in q or "$60" in q or "$50" in q:
            return "oil_low"
        return None

    # Peace markets (de-escalation)
    if any(k in q for k in ["peace deal", "ceasefire", "surrender",
                             "returns to normal", "end of military"]):
        if "iran" in q or "israel" in q or "hormuz" in q:
            return "peace"

    # Escalation markets
    if any(k in q for k in ["invade iran", "military action", "uranium",
                             "iran conflict", "iranian regime", "blockade"]):
        return "escalation"

    return None


class Position:
    def __init__(self, market_id, question, side, token_id, entry_price, shares, cost, trigger):
        self.market_id = market_id
        self.question = question
        self.side = side
        self.token_id = token_id
        self.entry_price = entry_price
        self.shares = shares
        self.cost_usd = cost
        self.trigger = trigger
        self.opened_at = time.time()


class OilIranTrader:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.balance = STARTING_BALANCE
        self.positions: Dict[str, Position] = {}
        self.history: List[dict] = []
        self.total_trades = 0
        self.last_scan = 0

        # Market categories
        self.markets: Dict[str, dict] = {}
        # market_id -> {q, tokens, category, ticks: deque}
        self.tick_history: Dict[str, deque] = {}

        self._load_state()

    def _load_state(self):
        if os.path.exists(PF_FILE):
            try:
                with open(PF_FILE) as f:
                    d = json.load(f)
                # FIX: use equity (not balance) so cash locked in open positions
                # returns to balance on restart. Positions are not restored
                # (in-memory only), so we treat them as "closed at entry" for state.
                self.balance = d.get("equity", d.get("balance", STARTING_BALANCE))
                self.history = d.get("history", [])
                self.total_trades = d.get("total_trades", 0)
                logger.info(f"Loaded state: balance=${self.balance:.2f}, trades={self.total_trades}")
            except Exception as e:
                logger.warning(f"Load failed: {e}")

    def _save_state(self):
        with open(PF_FILE, "w") as f:
            json.dump({
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "balance": self.balance,
                "equity": self.balance + sum(p.cost_usd for p in self.positions.values()),
                "positions": [{
                    "market_id": p.market_id, "q": p.question[:60],
                    "side": p.side, "entry": p.entry_price,
                    "shares": p.shares, "cost": p.cost_usd,
                    "trigger": p.trigger,
                } for p in self.positions.values()],
                "history": self.history,
                "total_trades": self.total_trades,
                "wins": sum(1 for h in self.history if h["pnl"] > 0),
                "losses": sum(1 for h in self.history if h["pnl"] <= 0),
                "realized_pnl": sum(h["pnl"] for h in self.history),
            }, f, indent=2)

    async def scan_markets(self):
        now = time.time()
        if now - self.last_scan < SCAN_INTERVAL and self.markets:
            return
        self.last_scan = now

        try:
            # Fetch top-volume active markets
            all_mkts = []
            for offset in range(0, 500, 100):
                r = await self.client.get(f"{GAMMA}/markets", params={
                    "active": "true", "closed": "false", "limit": 100,
                    "offset": offset, "order": "volume24hr", "ascending": "false"})
                if r.status_code != 200:
                    break
                batch = r.json()
                if not batch:
                    break
                all_mkts.extend(batch)

            found = {}
            for m in all_mkts:
                q = m.get("question", "")
                cat = classify(q)
                if not cat:
                    continue
                mid = m.get("id", "")
                if not mid:
                    continue
                tokens = m.get("clobTokenIds", "[]")
                if isinstance(tokens, str):
                    try:
                        tokens = json.loads(tokens)
                    except Exception:
                        continue
                if len(tokens) < 2:
                    continue
                found[mid] = {
                    "q": q,
                    "token_yes": str(tokens[0]),
                    "token_no": str(tokens[1]),
                    "category": cat,
                    "fees_enabled": m.get("feesEnabled", False),
                }
                if mid not in self.tick_history:
                    self.tick_history[mid] = deque(maxlen=50)

            self.markets = found
            by_cat = {}
            for d in found.values():
                by_cat.setdefault(d["category"], 0)
                by_cat[d["category"]] += 1
            logger.info(f"Markets: {sum(by_cat.values())} total | {by_cat}")
        except Exception as e:
            logger.error(f"Scan failed: {e}")

    async def poll_prices(self):
        """Fetch current prices for all tracked markets."""
        tasks = [self._poll_one(mid, d) for mid, d in self.markets.items()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _poll_one(self, mid, data):
        try:
            r_buy = await self.client.get(f"{CLOB}/price",
                params={"token_id": data["token_yes"], "side": "buy"})
            r_sell = await self.client.get(f"{CLOB}/price",
                params={"token_id": data["token_yes"], "side": "sell"})
            bid = float(r_buy.json().get("price", 0))
            ask = float(r_sell.json().get("price", 0))
            if bid <= 0 or ask <= 0:
                return
            mid_price = (bid + ask) / 2.0
            self.tick_history[mid].append({
                "ts": time.time(),
                "mid": mid_price, "bid": bid, "ask": ask
            })
        except Exception:
            pass

    def compute_indices(self):
        """Compute aggregate momentum for each category."""
        indices = {
            "escalation": 0.0, "peace": 0.0,
            "oil_high": 0.0, "oil_low": 0.0,
        }
        counts = {k: 0 for k in indices}

        for mid, data in self.markets.items():
            ticks = self.tick_history[mid]
            if len(ticks) < MOMENTUM_WINDOW:
                continue
            # Momentum: (latest - N ago) / N ago
            latest = ticks[-1]["mid"]
            old = ticks[-MOMENTUM_WINDOW]["mid"]
            if old == 0:
                continue
            mom = (latest - old) / old
            cat = data["category"]
            indices[cat] += mom
            counts[cat] += 1

        # Average
        return {k: (indices[k] / counts[k] if counts[k] else 0)
                for k in indices}

    def decide_trades(self):
        """Based on indices, decide what to open."""
        idx = self.compute_indices()

        # Log indices
        logger.info(f"Indices: esc={idx['escalation']:+.3f} peace={idx['peace']:+.3f} "
                    f"oil_hi={idx['oil_high']:+.3f} oil_lo={idx['oil_low']:+.3f}")

        signals = []

        # Escalation UP → oil HIGH long (YES that oil reaches high target)
        if idx["escalation"] > ENTRY_THRESHOLD:
            for mid, data in self.markets.items():
                if data["category"] == "oil_high" and mid not in self.positions:
                    signals.append(("YES", mid, data, f"esc+{idx['escalation']:.3f}"))

        # Peace UP → oil HIGH short (NO that oil reaches high) = oil_low long
        if idx["peace"] > ENTRY_THRESHOLD:
            for mid, data in self.markets.items():
                if data["category"] == "oil_high" and mid not in self.positions:
                    signals.append(("NO", mid, data, f"peace+{idx['peace']:.3f}"))
                elif data["category"] == "oil_low" and mid not in self.positions:
                    # peace → oil falls → oil LOW target more likely
                    signals.append(("YES", mid, data, f"peace+{idx['peace']:.3f}"))

        # Escalation UP → peace NO (fade peace chances)
        if idx["escalation"] > ENTRY_THRESHOLD * 2:
            for mid, data in self.markets.items():
                if data["category"] == "peace" and mid not in self.positions:
                    signals.append(("NO", mid, data, f"esc_fade+{idx['escalation']:.3f}"))

        return signals

    def execute_signals(self, signals):
        """Open positions based on signals (max 5 new per cycle)."""
        for side, mid, data, trigger in signals[:5]:
            ticks = self.tick_history[mid]
            if not ticks:
                continue
            last = ticks[-1]

            if side == "YES":
                entry_price = last["ask"]
                token = data["token_yes"]
            else:
                entry_price = 1.0 - last["bid"]  # NO ask = 1 - YES bid
                token = data["token_no"]

            if entry_price <= 0.05 or entry_price >= 0.95:
                continue

            cost = POSITION_USD
            if cost > self.balance:
                continue

            shares = POSITION_USD / entry_price
            self.balance -= cost
            self.total_trades += 1
            self.positions[mid] = Position(
                market_id=mid, question=data["q"], side=side,
                token_id=token, entry_price=entry_price,
                shares=shares, cost=cost, trigger=trigger
            )
            logger.info(f"OPEN {side} {data['category']} @ {entry_price:.3f} | "
                        f"trigger={trigger} | {data['q'][:50]}")

    def check_exits(self):
        """Check if any open positions should close."""
        for mid in list(self.positions.keys()):
            pos = self.positions[mid]
            ticks = self.tick_history.get(mid)
            if not ticks:
                continue
            last = ticks[-1]

            if pos.side == "YES":
                current = last["bid"]
            else:
                current = 1.0 - last["ask"]

            if pos.entry_price <= 0:
                continue

            pnl_pct = (current - pos.entry_price) / pos.entry_price

            exit_reason = None
            if pnl_pct <= STOP_LOSS:
                exit_reason = "stop_loss"
            elif pnl_pct >= TAKE_PROFIT:
                exit_reason = "take_profit"
            elif time.time() - pos.opened_at > 4 * 3600:
                exit_reason = "time_out_4h"

            if exit_reason:
                gross = pos.shares * current
                pnl = gross - pos.cost_usd
                self.balance += gross
                self.total_trades += 1
                self.history.append({
                    "question": pos.question[:60],
                    "side": pos.side,
                    "entry": round(pos.entry_price, 4),
                    "exit": round(current, 4),
                    "pnl": round(pnl, 4),
                    "reason": exit_reason,
                    "trigger": pos.trigger,
                    "opened_at": pos.opened_at,
                    "closed_at": time.time(),
                })
                del self.positions[mid]
                tag = "WIN" if pnl > 0 else "LOSS"
                logger.info(f"CLOSE[{tag}] {pos.side} @ {current:.3f} "
                            f"pnl=${pnl:+.2f} ({exit_reason}) | {pos.question[:45]}")

    def print_status(self):
        wins = sum(1 for h in self.history if h["pnl"] > 0)
        losses = sum(1 for h in self.history if h["pnl"] <= 0 and h["pnl"] != 0)
        realized = sum(h["pnl"] for h in self.history)
        equity = self.balance + sum(p.cost_usd for p in self.positions.values())
        wr = wins / max(wins + losses, 1) * 100
        logger.info(f"━━━ Oil×Iran Status ━━━ "
                    f"Equity=${equity:.2f} ({(equity/STARTING_BALANCE-1)*100:+.1f}%) "
                    f"| Open={len(self.positions)} | W/L={wins}/{losses} ({wr:.0f}%) "
                    f"| Realized=${realized:+.2f}")

    async def run(self):
        logger.info("Oil×Iran trader starting...")
        last_status = 0
        while True:
            try:
                await self.scan_markets()
                if self.markets:
                    await self.poll_prices()
                    signals = self.decide_trades()
                    if signals:
                        self.execute_signals(signals)
                    self.check_exits()

                now = time.time()
                if now - last_status > 300:  # every 5 min
                    self.print_status()
                    last_status = now

                self._save_state()
            except Exception as e:
                logger.error(f"Loop error: {e}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)

    async def cleanup(self):
        await self.client.aclose()


async def main():
    trader = OilIranTrader()
    try:
        await trader.run()
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        trader._save_state()
        await trader.cleanup()
        logger.info("Oil×Iran trader shutdown")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
