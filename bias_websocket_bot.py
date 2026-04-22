#!/usr/bin/env python3
"""
WebSocket Bias Trading Bot (Paper Trading)

Event-driven architecture for capturing limit order fills instantly via Polymarket CLOB WebSocket.
Uses `WebSocketManager` from `src.polymarket_mcp.utils.websocket_manager`.
"""

import asyncio
import json
import logging
import os
import httpx
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import websockets
from collections import deque, defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("WSTrader")

GAMMA_API_URL = "https://gamma-api.polymarket.com"
BIAS_PATTERNS = {"below": 0.931, "less": 0.889, "between": 0.867, "win": 0.782, "reach": 0.780, "will": 0.755}
MIN_EV_THRESHOLD = 0.05
PAPER_TRADE_SIZE_USD = 1.0


class WSPortfolioTracker:
    def __init__(self):
        os.makedirs("data", exist_ok=True)
        self.file = "data/ws_portfolio.json"
        self.tick_file = "data/ws_tick_data.jsonl"
        self.balance_usd = 1000.0
        self.active_orders = {}
        self.positions = {}
        self.history = []
        self.load()

    def load(self):
        if os.path.exists(self.file):
            with open(self.file, "r") as f:
                d = json.load(f)
                self.balance_usd = d.get("balance_usd", 1000.0)
                self.active_orders = d.get("active_orders", {})
                self.positions = d.get("positions", {})
                self.history = d.get("history", [])

    def save(self):
        with open(self.file, "w") as f:
            json.dump({
                "balance_usd": self.balance_usd,
                "active_orders": self.active_orders,
                "positions": self.positions,
                "history": self.history
            }, f, indent=2)

    def log_tick(self, token_id: str, best_bid: float, best_ask: float):
        ts = datetime.now(timezone.utc).isoformat()
        with open(self.tick_file, "a") as f:
            f.write(json.dumps({"ts": ts, "token": token_id, "ask": best_ask, "bid": best_bid}) + "\n")


class BiasWSBot:
    def __init__(self):
        self.portfolio = WSPortfolioTracker()
        self.httpx_client = httpx.AsyncClient(timeout=15.0)
        self.subscribed_tokens = set()
        self.ws_connection = None
        # Track last 50 ticks for each token to detect Flash Crashes
        self.tick_history = defaultdict(lambda: deque(maxlen=50))

    async def on_ws_message(self, raw_msg: str):
        try:
            msgs = json.loads(raw_msg)
            if not isinstance(msgs, list): msgs = [msgs]
            
            for msg in msgs:
                if not isinstance(msg, dict): continue
                tid = msg.get("asset_id")
                
                # V2 orderbook pushes bids/asks directly
                bids = msg.get("bids", [])
                asks = msg.get("asks", [])
                if not bids or not asks: continue
                
                best_bid = float(bids[0].get("price", 0) if isinstance(bids[0], dict) else bids[0][0])
                best_ask = float(asks[0].get("price", 0) if isinstance(asks[0], dict) else asks[0][0])
                if best_ask <= 0 or best_bid <= 0: continue
                
                # --- [CORE DATA PRIORITY] ALWAYS LOG TICK ---
                self.portfolio.log_tick(tid, best_bid, best_ask)
                
                if tid not in self.portfolio.active_orders: continue
                
                # --- AGGRESSIVE LOGIC: FLASH CRASH DETECTION ---
                history = self.tick_history[tid]
                if len(history) >= 10:
                    avg_ask = sum(history) / len(history)
                    # If price drops 5% below recent average, consider it a Flash Crash opportunity
                    if best_ask < 0.95 * avg_ask:
                        logger.warning(f"★★★ FLASH CRASH DETECTED ★★★ Token {tid[:8]}: {best_ask} vs Avg {avg_ask:.3f}")
                        # Force fill even if it wasn't our target limit yet, if it is still a "good" price
                        order = self.portfolio.active_orders[tid]
                        if best_ask <= 1.1 * order["limit_price"]: # Still near our budget
                             self.execute_fill(tid, best_ask, "FlashCrash")
                             history.clear()
                             continue
                
                history.append(best_ask)
                
                # --- Standard Fill Logic ---
                order = self.portfolio.active_orders[tid]
                if best_ask <= order["limit_price"]:
                    self.execute_fill(tid, best_ask, "WS_Tick")
                    
        except Exception as e:
            pass

    def execute_fill(self, tid: str, filled_price: float, reason: str):
        if tid not in self.portfolio.active_orders: return
        order = self.portfolio.active_orders[tid]
        
        logger.info(f"★★★ FILL ({reason}) ★★★ {order['question'][:30]}... at ${filled_price}")
        
        shares_bought = order["size_usd"] / filled_price
        self.portfolio.positions[tid] = {
            "market_id": order["market_id"],
            "question": order["question"],
            "shares": shares_bought,
            "cost_basis": order["size_usd"],
            "filled_price": filled_price,
            "filled_at": datetime.now(timezone.utc).isoformat(),
            "fill_reason": reason
        }
        del self.portfolio.active_orders[tid]
        self.portfolio.save()

    def match_bias_pattern(self, question: str) -> Optional[str]:
        words = question.lower().translate(str.maketrans('','', '?,.!"')).split()
        for w in words:
            if w in BIAS_PATTERNS: return w
        return None

    async def scout_and_subscribe(self):
        """HTTP fetching to find initial targets, followed by WS subscription"""
        logger.info("Scouting HTTP top 1000 markets for new WS candidates...")
        try:
            r = await self.httpx_client.get(
                f"{GAMMA_API_URL}/markets",
                params={"active": "true", "closed": "false", "limit": 1000}
            )
            r.raise_for_status()
            markets = r.json()
            if isinstance(markets, dict) and "data" in markets: markets = markets["data"]
            
            found = 0
            for m in markets:
                q = m.get("question", "")
                keyword = self.match_bias_pattern(q)
                if not keyword: continue
                
                cti = m.get("clobTokenIds", "[]")
                if isinstance(cti, str):
                    try: cti = json.loads(cti)
                    except: cti = []
                if len(cti) < 2: continue
                no_token_id = str(cti[1])
                
                if not no_token_id: continue
                if no_token_id in self.portfolio.active_orders or no_token_id in self.portfolio.positions:
                    # Make sure we are subscribed if we have an active order or position
                    await self._ensure_subscribed(no_token_id)
                    continue

                # 1. FAIR PROBABILITY CALCULATION (AGGRESSIVE MODE)
                # Historical Bias for this keyword represents Fair YES Prob
                fair_yes_prob = BIAS_PATTERNS[keyword]
                fair_no_prob = 1.0 - fair_yes_prob
                
                # 2. EVALUATE CURRENT MARKET PRICE VS FAIR PRICE
                br = await self.httpx_client.get("https://clob.polymarket.com/book", params={"token_id": no_token_id})
                ob = br.json()
                bids, asks = ob.get("bids", []), ob.get("asks", [])
                if not bids or not asks: continue

                best_bid = float(bids[0]["price"])
                best_ask = float(asks[0]["price"])
                
                # We want to buy NO token. 
                # Current price to buy NO is best_ask (if we are taker) or best_bid + small spread (if we are maker)
                current_price_no = best_bid
                edge = fair_no_prob - current_price_no
                
                if edge > MIN_EV_THRESHOLD:
                    # AGGRESSIVE PRICING: Place limit at Top of Book (best_bid + 0.001)
                    # This ensures we are filled if any minor trade happens.
                    target_c = best_bid + 0.001
                    if target_c >= best_ask: target_c = best_ask - 0.001 # Don't cross spread
                    if target_c <= 0: target_c = 0.01

                    if self.portfolio.balance_usd >= PAPER_TRADE_SIZE_USD:
                        self.portfolio.balance_usd -= PAPER_TRADE_SIZE_USD
                        self.portfolio.active_orders[no_token_id] = {
                            "market_id": m.get("id"),
                            "question": q,
                            "limit_price": target_c,
                            "size_usd": PAPER_TRADE_SIZE_USD,
                            "placed_at": datetime.now(timezone.utc).isoformat(),
                            "fair_price": fair_no_prob,
                            "initial_edge": edge
                        }
                        self.portfolio.save()
                        logger.info(f"[AGGRESSIVE ORDER] '{q[:30]}...' @ ${target_c:.3f} (Edge: {edge:.2f})")
                        found += 1
                        await self._ensure_subscribed(no_token_id)
            
            logger.info(f"Scouting complete. Discovered {found} new opportunities.")
        except Exception as e:
            logger.error(f"HTTP Scouting failed: {e}")

    async def _ensure_subscribed(self, token_id: str):
        if token_id not in self.subscribed_tokens:
            self.subscribed_tokens.add(token_id)
            if self.ws_connection and not getattr(self.ws_connection, "closed", False):
                try:
                    await self.ws_connection.send(json.dumps({"assets_ids": [token_id], "type": "market"}))
                except: pass

    async def settle_markets(self):
        """Since we didn't subscribe WS to market_resolved for simplicity, we just check via HTTP every hour"""
        closed_tokens = []
        for tid, pos in self.portfolio.positions.items():
            r = await self.httpx_client.get(f"{GAMMA_API_URL}/markets/{pos['market_id']}")
            if r.status_code == 200:
                m = r.json()
                if m.get("closed", False) or not m.get("active", True):
                    prices = m.get("outcomePrices", [])
                    if isinstance(prices, str):
                        try: prices = json.loads(prices)
                        except: prices = [0, 0]
                    if len(prices) >= 2:
                        p1 = float(prices[1])
                        if p1 >= 0.9:
                            payout = pos["shares"] * 1.0
                            self.portfolio.balance_usd += payout
                            self.portfolio.history.append({"market": pos["question"], "result": "WIN"})
                            closed_tokens.append(tid)
                        elif float(prices[0]) >= 0.9:
                            self.portfolio.history.append({"market": pos["question"], "result": "LOSS"})
                            closed_tokens.append(tid)

        for tid in closed_tokens:
            del self.portfolio.positions[tid]
            # Try removing WS subscription nicely
            if self.ws_connection and not getattr(self.ws_connection, "closed", False):
                try: await self.ws_connection.send(json.dumps({"assets_ids": [tid], "type": "market", "action": "unsubscribe"}))
                except: pass
        if closed_tokens: self.portfolio.save()

    async def direct_ws_loop(self):
        url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        while True:
            try:
                logger.info("Connecting to Polymarket CLOB V2 WS...")
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    self.ws_connection = ws
                    # Resubscribe all known tokens on connect
                    tokens = list(self.subscribed_tokens)
                    if tokens:
                        # Chunk safely if many tokens
                        for i in range(0, len(tokens), 100):
                            await ws.send(json.dumps({"assets_ids": tokens[i:i+100], "type": "market"}))
                    
                    while True:
                        msg = await ws.recv()
                        await self.on_ws_message(msg)
            except Exception as e:
                logger.error(f"WS disconnected: {e}. Reconnecting in 5s...")
                self.ws_connection = None
                await asyncio.sleep(5)

    async def run(self):
        logger.info("Initializing Direct WS Bot...")
        
        asyncio.create_task(self.direct_ws_loop())
        
        # Give WS a moment to connect
        await asyncio.sleep(2)

        # Continuous background loop for Scouting & Settle (HTTP fallback). WS runs fully async on its own!
        while True:
            logger.info(f"--- WS STATUS --- Bankroll: ${self.portfolio.balance_usd:.2f} | Orders: {len(self.portfolio.active_orders)} | Pos: {len(self.portfolio.positions)}")
            await self.scout_and_subscribe()
            await self.settle_markets()
            
            # Sleep for 15 minutes before scouting for new markets.
            await asyncio.sleep(900)

    async def cleanup(self):
        if self.ws_connection:
            try: await self.ws_connection.close()
            except: pass
        await self.httpx_client.aclose()


async def main():
    bot = BiasWSBot()
    try:
        await bot.run()
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        await bot.cleanup()
        logger.info("WS Bot successfully shut down.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
