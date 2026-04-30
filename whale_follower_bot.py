#!/usr/bin/env python3
"""
ST-46 + ST-49 combined: Polymarket leaderboard tracker + whale positions follower.

Flow:
1. Poll lb-api.polymarket.com/profit?window=1d/7d/30d for top traders
2. For top-3 (1d profit), fetch their open positions via data-api.polymarket.com/positions
3. For each whale position in mid-band [0.10, 0.90], copy direction with paper bet
4. Hold to resolution

State: bot-data/whale_follower.json
Per-bot equity tracking, $1000 starting balance.
"""
import asyncio, gc, json, os, time
from datetime import datetime, timezone

import httpx

LB_API = "https://lb-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

DATA = "data"
STATE_FILE = os.path.join(DATA, "whale_follower.json")
TOP_TRADERS_FILE = os.path.join(DATA, "top_traders.json")

BET_USD = 0.01
ARENA_BET_USD = 50.0
EQUITY_SCALE = ARENA_BET_USD / BET_USD
STARTING_BALANCE = 1000.0

PRICE_MIN = 0.10
PRICE_MAX = 0.90
TOP_N_WHALES = 5             # follow top-5 from 1d leaderboard
MAX_POSITIONS_TOTAL = 30     # cap total open positions
MAX_PER_WHALE = 8            # avoid concentration per whale

SCAN_INTERVAL = 1800        # 30 min — leaderboard rotates slowly
SETTLE_INTERVAL = 1800
SAVE_INTERVAL = 600


class WhaleFollower:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.last_scan = 0
        self.last_settle = 0
        self.last_save = 0
        self.state = {
            "open_positions": {},     # cid → position info + whale_wallet
            "wins": 0, "losses": 0, "total_bets": 0,
            "realized_pnl": 0.0,
            "by_whale": {},
        }
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                self.state = json.load(open(STATE_FILE))
                self.state.setdefault("open_positions", {})
                self.state.setdefault("by_whale", {})
            except Exception as e:
                print(f"[whale] load err: {e}")

    def _equity(self):
        return STARTING_BALANCE + self.state["realized_pnl"] * EQUITY_SCALE

    def save(self):
        out = {**self.state,
               "equity_arena_scale": round(self._equity(), 2),
               "updated_at": datetime.now(timezone.utc).isoformat()}
        with open(STATE_FILE, "w") as f:
            json.dump(out, f, indent=1)

    async def fetch_top_traders(self):
        """Fetch top profit leaders for 1d/7d/30d windows."""
        out = {}
        for window in ["1d", "7d", "30d"]:
            try:
                r = await self.client.get(f"{LB_API}/profit", params={"window": window},
                                           timeout=15.0,
                                           headers={"User-Agent": "Mozilla/5.0"})
                if r.status_code != 200:
                    continue
                data = r.json()
                top = data[:TOP_N_WHALES] if isinstance(data, list) else []
                out[window] = [
                    {"wallet": t.get("proxyWallet", ""),
                     "amount": t.get("amount", 0),
                     "name": t.get("pseudonym") or t.get("name", "?")}
                    for t in top if t.get("proxyWallet")
                ]
            except Exception as e:
                print(f"[whale] lb err {window}: {e}")
        # Persist for analytics
        with open(TOP_TRADERS_FILE, "w") as f:
            json.dump({**out, "updated_at": datetime.now(timezone.utc).isoformat()}, f, indent=1)
        return out

    async def fetch_positions(self, wallet):
        try:
            r = await self.client.get(f"{DATA_API}/positions",
                                       params={"user": wallet, "limit": 100},
                                       timeout=15.0,
                                       headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                return []
            return r.json() or []
        except Exception:
            return []

    async def scan(self):
        leaders = await self.fetch_top_traders()
        if not leaders:
            return
        # Aggregate top wallets (deduplicated)
        top_wallets = {}
        for w in ["1d", "7d", "30d"]:
            for t in leaders.get(w, []):
                if t["wallet"] not in top_wallets:
                    top_wallets[t["wallet"]] = t
        print(f"[whale] {datetime.now():%H:%M} top wallets: {len(top_wallets)}")

        n_added = 0
        n_skipped = 0
        for wallet, info in top_wallets.items():
            if len(self.state["open_positions"]) >= MAX_POSITIONS_TOTAL:
                break
            per_whale = sum(1 for p in self.state["open_positions"].values()
                            if p.get("whale_wallet") == wallet)
            if per_whale >= MAX_PER_WHALE:
                continue
            positions = await self.fetch_positions(wallet)
            for pos in positions:
                # Sanity check
                cid = pos.get("conditionId", "")
                if not cid or cid in self.state["open_positions"]:
                    continue
                # Position fields: outcome ("Yes"/"No"), curPrice, etc.
                outcome = pos.get("outcome", "").lower()
                cur_price = float(pos.get("curPrice", 0))
                size = float(pos.get("size", 0))
                if size <= 0 or cur_price <= 0:
                    continue
                # Mid-band only
                if not (PRICE_MIN <= cur_price <= PRICE_MAX):
                    n_skipped += 1
                    continue
                # Bet same direction at current price
                side = "YES" if outcome == "yes" else "NO"
                entry = cur_price if side == "YES" else (1 - cur_price)
                if not (0.05 <= entry <= 0.95):
                    continue
                shares = BET_USD / entry
                self.state["open_positions"][cid] = {
                    "cid": cid,
                    "side": side,
                    "entry": round(entry, 4),
                    "shares": round(shares, 4),
                    "cost": BET_USD,
                    "whale_wallet": wallet,
                    "whale_name": info.get("name", "?"),
                    "whale_pnl_amount": info.get("amount", 0),
                    "trigger_mid": round(cur_price, 4),
                    "whale_size": size,
                    "question": pos.get("title", "")[:120] if pos.get("title") else pos.get("slug", "")[:80],
                    "opened_ts": time.time(),
                }
                self.state["total_bets"] += 1
                self.state["by_whale"].setdefault(wallet, {"name": info.get("name", "?"),
                                                            "opened": 0, "wins": 0, "losses": 0})
                self.state["by_whale"][wallet]["opened"] += 1
                n_added += 1
                if len(self.state["open_positions"]) >= MAX_POSITIONS_TOTAL:
                    break
                if sum(1 for p in self.state["open_positions"].values()
                       if p.get("whale_wallet") == wallet) >= MAX_PER_WHALE:
                    break

        print(f"[whale] added={n_added} skipped(price)={n_skipped} "
              f"open={len(self.state['open_positions'])} eq=${self._equity():.2f}")
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
                won = (pos["side"] == "YES" and yes_won) or (pos["side"] == "NO" and not yes_won)
                pnl = (pos["shares"] - pos["cost"]) if won else -pos["cost"]
                if won:
                    self.state["wins"] += 1
                    if pos.get("whale_wallet"):
                        self.state["by_whale"][pos["whale_wallet"]]["wins"] += 1
                else:
                    self.state["losses"] += 1
                    if pos.get("whale_wallet"):
                        self.state["by_whale"][pos["whale_wallet"]]["losses"] += 1
                self.state["realized_pnl"] += pnl
                del self.state["open_positions"][cid]
                print(f"[whale] settled {pos.get('whale_name','?')} won={won} pnl=${pnl:+.4f}")
            except Exception:
                pass
        self.save()

    async def run(self):
        print(f"[whale] starting, top {TOP_N_WHALES} per window, max {MAX_POSITIONS_TOTAL} positions")
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
                print(f"[whale] loop err: {e}")
            gc.collect()
            await asyncio.sleep(60)


async def main():
    bot = WhaleFollower()
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
