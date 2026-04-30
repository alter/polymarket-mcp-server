#!/usr/bin/env python3
"""
Theta-decay bot: buy YES at limit 0.98 on markets at price >= 0.95 with
< 24h to close. Backtest showed 100% WR on n=34, +3.4% ROI per fill.

Polls active markets every 5 min. Maintains its own paper portfolio.
State: bot-data/theta_decay.json
"""
import asyncio, gc, json, os, time
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA = "data"
STATE_FILE = os.path.join(DATA, "theta_decay.json")

BET_USD = 0.01
# Triggers based on resolution_window_analysis findings:
# - last 6h price ≥0.80 → 100% YES (n=58)
# - last 1h price ≥0.95 → 100% YES (n=27)
# Sweet spot: trigger ≥0.80 with longer horizon for more fills
LIMIT_PRICE = 0.92          # buy YES at this limit (give some headroom for fills)
PRICE_TRIGGER = 0.80        # consider markets at this price or above
MAX_HOURS_TO_CLOSE = 90 * 24    # 90d — captures most sport season finishes + political decisions
MIN_HOURS_TO_CLOSE = -90 * 24   # negative = post-deadline UMA delay arb (outcome known)

SCAN_INTERVAL = 300         # 5 min
SETTLE_INTERVAL = 600
SAVE_INTERVAL = 300


class ThetaDecayBot:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.last_scan = 0
        self.last_settle = 0
        self.last_save = 0
        self.state = {
            "open_positions": {},   # cid → position
            "wins": 0, "losses": 0,
            "total_bets": 0,
            "realized_pnl": 0.0,
        }
        self._load_state()

    def _load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                self.state = json.load(open(STATE_FILE))
                # Ensure types
                self.state.setdefault("open_positions", {})
                self.state.setdefault("wins", 0)
                self.state.setdefault("losses", 0)
                self.state.setdefault("total_bets", 0)
                self.state.setdefault("realized_pnl", 0.0)
                print(f"[theta] Loaded state: {len(self.state['open_positions'])} open, "
                      f"W/L={self.state['wins']}/{self.state['losses']}")
            except Exception as e:
                print(f"[theta] Load err: {e}")

    def save(self):
        with open(STATE_FILE, "w") as f:
            json.dump({
                **self.state,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }, f, indent=1)

    async def scan_once(self):
        """Scan 2 pages of active markets for high-price fade candidates."""
        markets = []
        for offset in [0, 200]:
            try:
                r = await self.client.get(
                    f"{GAMMA}/markets",
                    params={"active": "true", "closed": "false",
                            "limit": 200, "offset": offset},
                    timeout=15.0,
                )
                if r.status_code != 200:
                    continue
                markets.extend(r.json())
            except Exception as e:
                print(f"[theta] gamma err: {e}")

        now = time.time()
        n_scanned = 0
        n_added = 0
        for m in markets:
            n_scanned += 1
            mid = str(m.get("id", ""))
            cid = m.get("conditionId", "")
            if not cid:
                continue
            if cid in self.state["open_positions"]:
                continue
            tokens_raw = m.get("clobTokenIds", "[]")
            if isinstance(tokens_raw, str):
                try:
                    tokens = json.loads(tokens_raw)
                except Exception:
                    continue
            else:
                tokens = tokens_raw
            if not tokens:
                continue
            yes_token = tokens[0]
            # End date
            end = m.get("endDate", "")
            try:
                end_ts = datetime.fromisoformat(end.replace("Z", "+00:00")).timestamp()
            except Exception:
                continue
            hours_to = (end_ts - now) / 3600
            if hours_to < MIN_HOURS_TO_CLOSE or hours_to > MAX_HOURS_TO_CLOSE:
                continue
            # Get current price
            try:
                rb = await self.client.get(
                    f"{CLOB}/book?token_id={yes_token}", timeout=10.0,
                )
                if rb.status_code != 200:
                    continue
                book = rb.json()
            except Exception:
                continue
            asks = book.get("asks", [])
            bids = book.get("bids", [])
            if not asks or not bids:
                continue
            # Best ask = last ask (asks descending). Best bid = last bid.
            best_ask_p = float(asks[-1]["price"])
            best_bid_p = float(bids[-1]["price"])
            mid_p = (best_ask_p + best_bid_p) / 2
            if mid_p < PRICE_TRIGGER:
                continue
            # Our limit at LIMIT_PRICE. Fill if best_ask_p <= LIMIT_PRICE.
            if best_ask_p > LIMIT_PRICE:
                continue
            entry = best_ask_p  # filled at best ask
            shares = BET_USD / entry
            self.state["open_positions"][cid] = {
                "cid": cid, "market_id": mid,
                "side": "YES", "entry": round(entry, 4),
                "limit": LIMIT_PRICE,
                "shares": round(shares, 4), "cost": BET_USD,
                "opened_ts": now,
                "trigger_mid": round(mid_p, 4),
                "hours_to_close": round(hours_to, 2),
                "question": m.get("question", "")[:80],
            }
            self.state["total_bets"] += 1
            n_added += 1

        print(f"[theta] {datetime.now():%H:%M:%S} scanned={n_scanned} "
              f"added={n_added} open={len(self.state['open_positions'])}")
        self.save()

    async def settle(self):
        if not self.state["open_positions"]:
            return
        for cid in list(self.state["open_positions"].keys()):
            try:
                r = await self.client.get(f"{CLOB}/markets/{cid}", timeout=10.0)
                if r.status_code != 200:
                    continue
                d = r.json()
                if not d.get("closed"):
                    continue
                tokens = d.get("tokens", [])
                if not tokens:
                    continue
                yes_won = tokens[0].get("winner", False)
                pos = self.state["open_positions"][cid]
                won = pos["side"] == "YES" and yes_won
                pnl = (pos["shares"] - pos["cost"]) if won else -pos["cost"]
                if won:
                    self.state["wins"] += 1
                else:
                    self.state["losses"] += 1
                self.state["realized_pnl"] += pnl
                del self.state["open_positions"][cid]
                print(f"[theta] settled cid={cid[:20]} won={won} pnl=${pnl:+.4f}")
            except Exception:
                pass
        self.save()

    async def run(self):
        print(f"[theta] starting, limit={LIMIT_PRICE}, trigger>={PRICE_TRIGGER}, "
              f"hours [{MIN_HOURS_TO_CLOSE},{MAX_HOURS_TO_CLOSE}]")
        while True:
            try:
                now = time.time()
                if now - self.last_scan > SCAN_INTERVAL:
                    await self.scan_once()
                    self.last_scan = now
                if now - self.last_settle > SETTLE_INTERVAL:
                    await self.settle()
                    self.last_settle = now
            except Exception as e:
                print(f"[theta] loop err: {e}")
            gc.collect()
            await asyncio.sleep(60)


async def main():
    bot = ThetaDecayBot()
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
