#!/usr/bin/env python3
"""
"Always NO" multi-variant bot.

Tests 5 strategy variants in parallel on same market scans:
  V1 fee_free       — base validated strategy (price 0.40-0.85, fee-free, 5 categories)
  V2 with_fees      — same but allow fee markets (test if edge holds with fees)
  V3 aggressive     — wider price range 0.30-0.95
  V4 conservative   — narrow price 0.50-0.80, only sports/crypto
  V5 all_categories — include 'other' (no category filter)

Each variant maintains its own paper portfolio.
"""
import asyncio, json, os, time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA = "data"
SCAN_INTERVAL = 1800
SETTLE_INTERVAL = 600
LEADERBOARD_INTERVAL = 600
BET_USD = 0.01
MIN_HOURS_TO_CLOSE = 0.5
MAX_HOURS_TO_CLOSE = 168
MIN_VOLUME_24H = 5_000


def categorize(q):
    ql = q.lower()
    if any(s in ql for s in ["bitcoin", "btc", "ethereum", "eth", "solana"]):
        return "crypto"
    if any(s in ql for s in ["lol:", "cs2", "valorant", "dota", "counter-strike"]):
        return "esports"
    if any(s in ql for s in ["fc", "vs.", "vs ", "win on 2026"]) and not any(
            x in ql for x in ["lol", "cs2", "valorant", "dota"]):
        return "sports"
    if any(s in ql for s in ["temperature", "weather", "rain", "snow", "wind"]):
        return "weather"
    if any(s in ql for s in ["mrbeast", "elon", "tweet", "musk"]):
        return "culture"
    if any(s in ql for s in ["iran", "russia", "ukraine", "israel", "military",
                              "ceasefire", "hormuz", "war"]):
        return "geopolitics"
    return "other"


def get_yes_price(mkt):
    op = mkt.get("outcomePrices", "")
    if isinstance(op, str):
        try:
            op = json.loads(op)
        except Exception:
            return None
    if op and len(op) >= 1:
        try:
            return float(op[0])
        except Exception:
            pass
    return None


@dataclass
class StrategyVariant:
    name: str
    price_min: float
    price_max: float
    allow_fees: bool
    categories: set  # categories allowed
    file: str = ""

    def __post_init__(self):
        self.file = os.path.join(DATA, f"always_no_{self.name}.json")
        self.positions = {}
        self.history = []
        self._load()

    def _load(self):
        if os.path.exists(self.file):
            try:
                d = json.load(open(self.file))
                self.positions = {p["cid"]: p for p in d.get("positions", [])}
                self.history = d.get("history", [])
            except Exception:
                pass

    def save(self):
        wins = sum(1 for h in self.history if h["pnl"] > 0)
        losses = sum(1 for h in self.history if h["pnl"] <= 0)
        realized = sum(h["pnl"] for h in self.history)
        with open(self.file, "w") as f:
            json.dump({
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "name": self.name,
                "price_range": [self.price_min, self.price_max],
                "allow_fees": self.allow_fees,
                "categories": sorted(self.categories),
                "positions": list(self.positions.values()),
                "history": self.history,
                "wins": wins, "losses": losses,
                "total_bets": len(self.history),
                "realized_pnl": round(realized, 4),
                "total_invested": round(len(self.history) * BET_USD, 4),
                "roi_pct": round(realized / max(len(self.history) * BET_USD, 0.01) * 100, 2),
            }, f, indent=2)

    def should_bet(self, market, yes_price, category, fees_on, hours_to_close, vol24):
        if yes_price < self.price_min or yes_price > self.price_max:
            return False
        if not self.allow_fees and fees_on:
            return False
        if category not in self.categories:
            return False
        if hours_to_close < MIN_HOURS_TO_CLOSE or hours_to_close > MAX_HOURS_TO_CLOSE:
            return False
        if vol24 < MIN_VOLUME_24H:
            return False
        return True


# ── Define variants ──

ALL_CATS = {"sports", "esports", "crypto", "culture", "geopolitics", "weather"}
NARROW_CATS = {"sports", "crypto"}
ALL_PLUS_OTHER = ALL_CATS | {"other"}

VARIANTS = [
    StrategyVariant(name="v1_fee_free", price_min=0.40, price_max=0.85,
                    allow_fees=False, categories=ALL_CATS),
    StrategyVariant(name="v2_with_fees", price_min=0.40, price_max=0.85,
                    allow_fees=True, categories=ALL_CATS),
    StrategyVariant(name="v3_aggressive", price_min=0.30, price_max=0.95,
                    allow_fees=False, categories=ALL_CATS),
    StrategyVariant(name="v4_conservative", price_min=0.50, price_max=0.80,
                    allow_fees=False, categories=NARROW_CATS),
    StrategyVariant(name="v5_all_cats", price_min=0.40, price_max=0.85,
                    allow_fees=False, categories=ALL_PLUS_OTHER),
]


# ── Bot ──

class MultiNoBot:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.last_scan = 0
        self.last_settle = 0
        self.last_leaderboard = 0
        self.variants = VARIANTS

    async def fetch_active_markets(self, limit=500):
        markets = []
        for offset in range(0, limit, 100):
            try:
                r = await self.client.get(f"{GAMMA}/markets", params={
                    "active": "true", "closed": "false",
                    "limit": 100, "offset": offset,
                    "order": "volume24hr", "ascending": "false",
                })
                if r.status_code != 200:
                    break
                batch = r.json()
                if not batch:
                    break
                markets.extend(batch)
            except Exception:
                break
        return markets

    async def scan_and_bet(self):
        now = time.time()
        if now - self.last_scan < SCAN_INTERVAL:
            return
        self.last_scan = now

        markets = await self.fetch_active_markets()
        if not markets:
            return

        bets_per_variant = defaultdict(int)
        for m in markets:
            cid = m.get("conditionId", "")
            if not cid:
                continue
            yp = get_yes_price(m)
            if yp is None:
                continue
            q = m.get("question", "")
            category = categorize(q)
            fees_on = m.get("feesEnabled", False)
            vol24 = float(m.get("volume24hr", 0) or 0)

            end_str = m.get("endDate", "")
            try:
                close_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                hours_to_close = (close_dt - datetime.now(timezone.utc)).total_seconds() / 3600
            except Exception:
                continue

            entry = 1.0 - yp
            if entry < 0.05 or entry > 0.95:
                continue

            for v in self.variants:
                if cid in v.positions:
                    continue
                if any(h["cid"] == cid for h in v.history):
                    continue
                if not v.should_bet(m, yp, category, fees_on, hours_to_close, vol24):
                    continue

                shares = BET_USD / entry
                v.positions[cid] = {
                    "cid": cid, "question": q[:80], "category": category,
                    "side": "NO", "entry": round(entry, 4),
                    "market_yes_at_entry": round(yp, 4),
                    "cost": BET_USD, "shares": round(shares, 4),
                    "fees_on": fees_on,
                    "vol24": vol24, "hours_to_close": round(hours_to_close, 1),
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                    "end_date": end_str,
                }
                bets_per_variant[v.name] += 1

        for v in self.variants:
            if bets_per_variant[v.name]:
                v.save()

        if bets_per_variant:
            print(f"\n[{datetime.now():%H:%M}] Scan: {len(markets)} markets, "
                  f"new bets per variant: {dict(bets_per_variant)}")

    async def settle_resolutions(self):
        now = time.time()
        if now - self.last_settle < SETTLE_INTERVAL:
            return
        self.last_settle = now

        # Collect all unique cids with open positions
        all_cids = set()
        for v in self.variants:
            all_cids.update(v.positions.keys())

        if not all_cids:
            return

        resolved_count = 0
        for cid in all_cids:
            try:
                r = await self.client.get(f"{CLOB}/markets/{cid}")
                if r.status_code != 200:
                    continue
                d = r.json()
                if not d.get("closed"):
                    continue
                tokens = d.get("tokens", [])
                if not tokens:
                    continue
                yes_won = tokens[0].get("winner", False)

                for v in self.variants:
                    if cid not in v.positions:
                        continue
                    pos = v.positions[cid]
                    won = not yes_won
                    if won:
                        pnl = pos["shares"] * 1.0 - pos["cost"]
                    else:
                        pnl = -pos["cost"]
                    pos["yes_won"] = yes_won
                    pos["won"] = won
                    pos["pnl"] = round(pnl, 4)
                    pos["resolved_at"] = datetime.now(timezone.utc).isoformat()
                    v.history.append(pos)
                    del v.positions[cid]
                    resolved_count += 1
                    tag = "WIN" if won else "LOSS"
                    print(f"  [{v.name:<18}] {tag} {pos['category']:<11} "
                          f"entry={pos['entry']:.3f} pnl=${pnl:+.4f} | "
                          f"{pos['question'][:48]}")
            except Exception:
                pass

        if resolved_count:
            for v in self.variants:
                v.save()

    def print_leaderboard(self):
        now = time.time()
        if now - self.last_leaderboard < LEADERBOARD_INTERVAL:
            return
        self.last_leaderboard = now

        print(f"\n━━━ AlwaysNo VARIANTS LEADERBOARD ({datetime.now():%H:%M}) ━━━")
        print(f"  {'Variant':<18} {'Open':>4} {'Closed':>7} {'WR':>5} "
              f"{'PnL':>10} {'ROI':>7}")
        for v in self.variants:
            wins = sum(1 for h in v.history if h["pnl"] > 0)
            losses = sum(1 for h in v.history if h["pnl"] <= 0)
            total = wins + losses
            wr = wins / total * 100 if total else 0
            realized = sum(h["pnl"] for h in v.history)
            cost = total * BET_USD
            roi = realized / cost * 100 if cost else 0
            print(f"  {v.name:<18} {len(v.positions):>4} {total:>7} "
                  f"{wr:>4.0f}% ${realized:>+8.4f} {roi:>+6.1f}%")

    async def run(self):
        print(f"MultiNoBot starting with {len(self.variants)} variants")
        for v in self.variants:
            print(f"  {v.name}: price [{v.price_min}-{v.price_max}], "
                  f"fees={v.allow_fees}, cats={sorted(v.categories)}")

        while True:
            try:
                await self.scan_and_bet()
                await self.settle_resolutions()
                self.print_leaderboard()
            except Exception as e:
                print(f"Loop error: {e}")
            await asyncio.sleep(60)


async def main():
    bot = MultiNoBot()
    try:
        await bot.run()
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        for v in bot.variants:
            v.save()
        await bot.client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
