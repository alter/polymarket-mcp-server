#!/usr/bin/env python3
"""
ST-48 Sub-penny lottery bot — bet YES on markets with extremely low YES price
(0.01-0.05) ending soon. Edge case: occasional unexpected YES resolution gives
20-100x payout on rare wins.

Hypothesis: long-tail markets at sub-penny prices have residual probability of
YES — when 1-in-50 lottery hits, payoff = 1/0.02 - 1 = 49x.

State: bot-data/subpenny.json
"""
import asyncio, gc, json, os, time
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA = "data"
STATE_FILE = os.path.join(DATA, "subpenny.json")

BET_USD = 0.01
ARENA_BET_USD = 50.0
EQUITY_SCALE = ARENA_BET_USD / BET_USD
STARTING_BALANCE = 1000.0

PRICE_MIN = 0.01
PRICE_MAX = 0.05
MIN_HOURS_TO_CLOSE = 1
MAX_HOURS_TO_CLOSE = 36

SCAN_INTERVAL = 600
SETTLE_INTERVAL = 1800


class SubpennyBot:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.last_scan = 0
        self.last_settle = 0
        self.state = {
            "open_positions": {},
            "wins": 0, "losses": 0, "total_bets": 0,
            "realized_pnl": 0.0,
        }
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                self.state = json.load(open(STATE_FILE))
                self.state.setdefault("open_positions", {})
            except Exception:
                pass

    def _equity(self):
        return STARTING_BALANCE + self.state["realized_pnl"] * EQUITY_SCALE

    def save(self):
        out = {**self.state, "equity_arena_scale": round(self._equity(), 2),
               "updated_at": datetime.now(timezone.utc).isoformat()}
        with open(STATE_FILE, "w") as f:
            json.dump(out, f, indent=1)

    async def scan(self):
        markets = []
        for offset in [0, 200]:
            try:
                r = await self.client.get(f"{GAMMA}/markets",
                    params={"active": "true", "closed": "false", "limit": 200, "offset": offset},
                    timeout=15.0)
                if r.status_code == 200:
                    markets.extend(r.json())
            except Exception:
                pass
        now = time.time()
        n_added = 0
        for m in markets:
            cid = m.get("conditionId", "")
            if not cid or cid in self.state["open_positions"]:
                continue
            tokens_raw = m.get("clobTokenIds", "[]")
            try:
                tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
            except Exception:
                continue
            if not tokens: continue
            yes_token = tokens[0]
            end = m.get("endDate", "")
            try:
                end_ts = datetime.fromisoformat(end.replace("Z", "+00:00")).timestamp()
            except Exception:
                continue
            h = (end_ts - now) / 3600
            if not (MIN_HOURS_TO_CLOSE <= h <= MAX_HOURS_TO_CLOSE):
                continue
            try:
                rb = await self.client.get(f"{CLOB}/book?token_id={yes_token}", timeout=10.0)
                if rb.status_code != 200: continue
                book = rb.json()
            except Exception:
                continue
            asks, bids = book.get("asks", []), book.get("bids", [])
            if not asks or not bids: continue
            best_ask = float(asks[-1]["price"])
            mid = (best_ask + float(bids[-1]["price"])) / 2
            if not (PRICE_MIN <= mid <= PRICE_MAX):
                continue
            entry = best_ask
            if entry > 0.06:  # no headroom
                continue
            shares = BET_USD / entry
            self.state["open_positions"][cid] = {
                "cid": cid, "side": "YES",
                "entry": round(entry, 4), "shares": round(shares, 4),
                "cost": BET_USD,
                "trigger_mid": round(mid, 4),
                "hours_to_close": round(h, 1),
                "question": m.get("question", "")[:120],
                "opened_ts": now,
            }
            self.state["total_bets"] += 1
            n_added += 1
        print(f"[subpenny] {datetime.now():%H:%M} added={n_added} "
              f"open={len(self.state['open_positions'])} eq=${self._equity():.2f}")
        self.save()

    async def settle(self):
        if not self.state["open_positions"]:
            return
        for cid in list(self.state["open_positions"].keys()):
            try:
                r = await self.client.get(f"{CLOB}/markets/{cid}", timeout=10.0)
                if r.status_code != 200: continue
                d = r.json()
                if not d.get("closed"): continue
                tokens = d.get("tokens", [])
                if not tokens: continue
                yes_won = tokens[0].get("winner", False)
                pos = self.state["open_positions"][cid]
                won = pos["side"] == "YES" and yes_won
                pnl = (pos["shares"] - pos["cost"]) if won else -pos["cost"]
                if won: self.state["wins"] += 1
                else: self.state["losses"] += 1
                self.state["realized_pnl"] += pnl
                del self.state["open_positions"][cid]
                if won:
                    print(f"[subpenny] 🎰 WIN! pnl=${pnl:+.4f}")
            except Exception:
                pass
        self.save()

    async def run(self):
        print(f"[subpenny] starting, price [{PRICE_MIN}, {PRICE_MAX}], hours [{MIN_HOURS_TO_CLOSE}, {MAX_HOURS_TO_CLOSE}]")
        while True:
            try:
                now = time.time()
                if now - self.last_scan > SCAN_INTERVAL:
                    await self.scan()
                    self.last_scan = now
                if now - self.last_settle > SETTLE_INTERVAL:
                    await self.settle()
                    self.last_settle = now
            except Exception as e:
                print(f"[subpenny] err: {e}")
            gc.collect()
            await asyncio.sleep(60)


async def main():
    bot = SubpennyBot()
    try:
        await bot.run()
    finally:
        bot.save()
        await bot.client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
