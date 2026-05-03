#!/usr/bin/env python3
"""
Maker Bot — paper-simulated market making via Polymarket WS book stream.

Logic:
  1. Periodically fetch top-volume active markets, pick those with spread >= 2¢
     and decent liquidity.
  2. Subscribe to their YES tokens via wss://ws-subscriptions-clob.polymarket.com.
  3. For each book update simulate maker orders:
        BUY  limit at best_bid + 0.01¢
        SELL limit at best_ask - 0.01¢
     If incoming book shows trades crossing our level → "filled" (paper).
  4. Hold filled position until either:
        a) opposite-side maker fills (capture spread → profit)
        b) book moves against us beyond stop tolerance (loss)
        c) time-out (60-min hold cap)
  5. State: bot-data/maker.json

This is paper-only and adds NOTHING risky to real funds. Goal: measure how much
edge a real maker strategy would have captured vs taker baseline. If it shows
positive P&L over a week, we know rebate route is worth implementing for real.

WS protocol notes:
  - Subscribe payload: {"assets_ids": [token_id], "type": "market"}
  - Server emits book snapshots and "price_change" events with bids/asks arrays
"""
import asyncio, json, os, time, gc
from collections import defaultdict
from datetime import datetime, timezone

import httpx
import websockets

GAMMA = "https://gamma-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DATA = "data"
STATE = os.path.join(DATA, "maker.json")
HEADERS = {"User-Agent": "Mozilla/5.0 maker-bot"}

MIN_SPREAD = 0.02            # only quote markets with >=2¢ spread
MAX_MARKETS = 30             # subscribe to top-30 by volume meeting criterion
QUOTE_OFFSET = 0.01          # post 1¢ inside the spread
HOLD_TIMEOUT_SEC = 3600      # close any position held >1h
STOP_TOLERANCE = 0.05        # close at -5¢ adverse move
SCAN_INTERVAL = 1800         # refresh market subscriptions every 30 min
SAVE_INTERVAL = 60           # save state every 60s
PAPER_QUANTITY = 50.0        # virtual $50 per order to match arena scale


class MakerBot:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        # tokens of interest -> market dict {cid, question, end_date, fees_on}
        self.tracked_tokens = {}
        # token_id -> latest book {best_bid, best_ask, ts}
        self.books = {}
        # token_id -> simulated open quotes [{side, price, ts}]
        self.open_quotes = defaultdict(list)
        # token_id -> filled position {side, fill_price, ts, qty}
        self.positions = {}
        self.fills = []           # log of all simulated fills
        self.realized_pnl = 0.0
        self.wins = 0
        self.losses = 0
        self.total_fills = 0
        self.last_save = 0
        self.last_scan = 0
        self.ws = None
        self._load()

    def _load(self):
        if not os.path.exists(STATE):
            return
        try:
            d = json.load(open(STATE))
            self.realized_pnl = d.get("realized_pnl", 0.0)
            self.wins = d.get("wins", 0)
            self.losses = d.get("losses", 0)
            self.total_fills = d.get("total_fills", 0)
        except Exception:
            pass

    def save(self):
        out = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "tracked_tokens": list(self.tracked_tokens.keys())[:50],
            "n_tracked": len(self.tracked_tokens),
            "n_open_positions": len(self.positions),
            "realized_pnl": round(self.realized_pnl, 4),
            "wins": self.wins, "losses": self.losses,
            "total_fills": self.total_fills,
            "win_rate": round(self.wins / max(self.wins + self.losses, 1) * 100, 1),
            "recent_fills": self.fills[-30:],
            "open_positions": [
                {"token": tid[:14], "side": p["side"],
                 "fill_price": round(p["fill_price"], 4),
                 "age_sec": int(time.time() - p["ts"])}
                for tid, p in self.positions.items()
            ],
        }
        with open(STATE, "w") as f:
            json.dump(out, f, indent=1)

    async def fetch_top_markets(self, client):
        """Fetch top-volume markets with sufficient spread."""
        markets = []
        try:
            r = await client.get(
                f"{GAMMA}/markets",
                params={"limit": 200, "order": "volume24hr",
                        "ascending": "false", "active": "true", "closed": "false"},
                headers=HEADERS, timeout=20.0,
            )
            if r.status_code != 200:
                return []
            markets = r.json()
        except Exception as e:
            print(f"[maker] fetch err: {e}")
            return []
        out = []
        for m in markets:
            tokens = m.get("clobTokenIds")
            if not tokens:
                continue
            try:
                if isinstance(tokens, str):
                    tokens = json.loads(tokens)
                yes_tok = tokens[0]
            except Exception:
                continue
            cid = m.get("conditionId") or m.get("id", "")
            out.append({
                "token": yes_tok, "cid": cid,
                "question": (m.get("question") or "")[:100],
                "vol24h": float(m.get("volume24hr", 0) or 0),
                "fees_on": bool(m.get("enableOrderBook", True) and m.get("makerFeeBps")),
                "end_date": m.get("endDate", ""),
            })
        out.sort(key=lambda m: -m["vol24h"])
        return out[:MAX_MARKETS]

    async def refresh_subscriptions(self, client):
        """Re-fetch top markets and update subscriptions."""
        new = await self.fetch_top_markets(client)
        new_tokens = {m["token"]: m for m in new}
        # Tokens to drop
        for tok in list(self.tracked_tokens.keys()):
            if tok not in new_tokens:
                del self.tracked_tokens[tok]
                if self.ws is not None:
                    try:
                        await self.ws.send(json.dumps({
                            "assets_ids": [tok], "type": "market",
                            "action": "unsubscribe",
                        }))
                    except Exception:
                        pass
        # New tokens
        for tok, m in new_tokens.items():
            if tok not in self.tracked_tokens:
                self.tracked_tokens[tok] = m
                if self.ws is not None:
                    try:
                        await self.ws.send(json.dumps({
                            "assets_ids": [tok], "type": "market",
                        }))
                    except Exception:
                        pass
        print(f"[maker] tracking {len(self.tracked_tokens)} markets")

    def _process_book_event(self, ev):
        """Handle book snapshot or price_change event."""
        token = ev.get("asset_id") or ev.get("market") or ""
        if token not in self.tracked_tokens:
            return
        bids = ev.get("bids") or []
        asks = ev.get("asks") or []
        if not bids or not asks:
            return
        try:
            best_bid = float(bids[0]["price"]) if isinstance(bids[0], dict) else float(bids[0][0])
            best_ask = float(asks[0]["price"]) if isinstance(asks[0], dict) else float(asks[0][0])
        except (KeyError, ValueError, IndexError):
            return
        if not (0.01 <= best_bid < best_ask <= 0.99):
            return
        prev = self.books.get(token)
        self.books[token] = {"bid": best_bid, "ask": best_ask, "ts": time.time()}
        spread = best_ask - best_bid

        # Try to simulate fills based on book moves
        self._simulate_fills(token, prev)
        # Try to "post" new quotes if no open position and spread is good
        if token not in self.positions and spread >= MIN_SPREAD:
            self._post_quotes(token, best_bid, best_ask)
        # Manage existing position
        if token in self.positions:
            self._manage_position(token, best_bid, best_ask)

    def _post_quotes(self, token, bid, ask):
        """Post a buy at bid+0.01 and a sell at ask-0.01 (paper)."""
        # Replace any prior unfilled quotes for this token
        self.open_quotes[token] = [
            {"side": "BUY",  "price": round(bid + QUOTE_OFFSET, 4), "ts": time.time()},
            {"side": "SELL", "price": round(ask - QUOTE_OFFSET, 4), "ts": time.time()},
        ]

    def _simulate_fills(self, token, prev):
        """If a quote price would have been crossed, mark as filled."""
        if token in self.positions:
            return
        cur = self.books.get(token)
        if not cur or not prev:
            return
        for q in self.open_quotes.get(token, []):
            # BUY filled if best_ask crosses our buy price (someone willing to sell at our price)
            if q["side"] == "BUY" and cur["ask"] <= q["price"]:
                self.positions[token] = {
                    "side": "LONG", "fill_price": q["price"],
                    "ts": time.time(), "quote_post_ts": q["ts"],
                }
                self.total_fills += 1
                self.fills.append({
                    "token": token[:14], "side": "BUY", "price": q["price"],
                    "spread_capture": round(prev["ask"] - q["price"], 4),
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "question": self.tracked_tokens.get(token, {}).get("question", "")[:60],
                })
                self.open_quotes[token] = []
                return
            if q["side"] == "SELL" and cur["bid"] >= q["price"]:
                self.positions[token] = {
                    "side": "SHORT", "fill_price": q["price"],
                    "ts": time.time(), "quote_post_ts": q["ts"],
                }
                self.total_fills += 1
                self.fills.append({
                    "token": token[:14], "side": "SELL", "price": q["price"],
                    "spread_capture": round(q["price"] - prev["bid"], 4),
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "question": self.tracked_tokens.get(token, {}).get("question", "")[:60],
                })
                self.open_quotes[token] = []
                return

    def _manage_position(self, token, bid, ask):
        """Close position if spread captured, stop hit, or timeout."""
        pos = self.positions[token]
        age = time.time() - pos["ts"]
        # Try opposite-side maker exit (capture full spread)
        if pos["side"] == "LONG":
            # Want to sell at bid+0.01. Did the bid lift to or above that?
            target = pos["fill_price"] + MIN_SPREAD  # need full spread to be profitable
            if bid >= target:
                pnl = (bid - pos["fill_price"]) * (PAPER_QUANTITY / pos["fill_price"])
                self._close(token, bid, pnl, "spread_capture")
                return
            # Stop loss
            if ask < pos["fill_price"] - STOP_TOLERANCE:
                pnl = (ask - pos["fill_price"]) * (PAPER_QUANTITY / pos["fill_price"])
                self._close(token, ask, pnl, "stop_loss")
                return
        else:  # SHORT
            target = pos["fill_price"] - MIN_SPREAD
            if ask <= target:
                pnl = (pos["fill_price"] - ask) * (PAPER_QUANTITY / pos["fill_price"])
                self._close(token, ask, pnl, "spread_capture")
                return
            if bid > pos["fill_price"] + STOP_TOLERANCE:
                pnl = (pos["fill_price"] - bid) * (PAPER_QUANTITY / pos["fill_price"])
                self._close(token, bid, pnl, "stop_loss")
                return
        # Timeout → flat at mid
        if age > HOLD_TIMEOUT_SEC:
            mid = (bid + ask) / 2
            if pos["side"] == "LONG":
                pnl = (mid - pos["fill_price"]) * (PAPER_QUANTITY / pos["fill_price"])
            else:
                pnl = (pos["fill_price"] - mid) * (PAPER_QUANTITY / pos["fill_price"])
            self._close(token, mid, pnl, "timeout")

    def _close(self, token, exit_price, pnl, reason):
        pos = self.positions.pop(token)
        self.realized_pnl += pnl
        if pnl > 0:
            self.wins += 1
        else:
            self.losses += 1
        self.fills.append({
            "token": token[:14], "side": "CLOSE",
            "entry": round(pos["fill_price"], 4),
            "exit": round(exit_price, 4),
            "pnl": round(pnl, 4),
            "reason": reason,
            "hold_sec": int(time.time() - pos["ts"]),
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    async def ws_loop(self, client):
        """Main WebSocket reader loop with reconnect."""
        retry_delay = 5
        while True:
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=20, ping_timeout=10,
                    max_size=2**24,
                ) as ws:
                    self.ws = ws
                    print(f"[maker] WS connected, resubscribing {len(self.tracked_tokens)} tokens")
                    if self.tracked_tokens:
                        for tok in self.tracked_tokens:
                            try:
                                await ws.send(json.dumps({
                                    "assets_ids": [tok], "type": "market",
                                }))
                            except Exception:
                                pass
                    retry_delay = 5
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                        except Exception:
                            continue
                        events = data if isinstance(data, list) else [data]
                        for ev in events:
                            if not isinstance(ev, dict):
                                continue
                            self._process_book_event(ev)
            except Exception as e:
                print(f"[maker] WS disconnected: {e}, retrying in {retry_delay}s")
                self.ws = None
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    async def scan_loop(self, client):
        """Periodically refresh which markets we track."""
        await self.refresh_subscriptions(client)  # initial
        while True:
            await asyncio.sleep(SCAN_INTERVAL)
            try:
                await self.refresh_subscriptions(client)
            except Exception as e:
                print(f"[maker] refresh err: {e}")

    async def save_loop(self):
        while True:
            await asyncio.sleep(SAVE_INTERVAL)
            try:
                self.save()
                gc.collect()
            except Exception as e:
                print(f"[maker] save err: {e}")

    async def status_loop(self):
        while True:
            await asyncio.sleep(300)
            wr = self.wins / max(self.wins + self.losses, 1) * 100
            print(f"[maker] {datetime.now():%H:%M} tracking={len(self.tracked_tokens)} "
                  f"open={len(self.positions)} fills={self.total_fills} "
                  f"W/L={self.wins}/{self.losses} ({wr:.0f}%) "
                  f"pnl=${self.realized_pnl:+.4f}")

    async def run(self):
        print(f"[maker] starting, MIN_SPREAD={MIN_SPREAD}¢, "
              f"OFFSET={QUOTE_OFFSET}¢, MAX_MARKETS={MAX_MARKETS}")
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            await asyncio.gather(
                self.ws_loop(client),
                self.scan_loop(client),
                self.save_loop(),
                self.status_loop(),
            )


async def main():
    bot = MakerBot()
    try:
        await bot.run()
    finally:
        bot.save()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
