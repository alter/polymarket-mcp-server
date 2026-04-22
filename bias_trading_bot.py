#!/usr/bin/env python3
"""
Polymarket Bias Trading Bot WITH Portfolio & Tick Tracker
(Paper Trading Mode)

This script:
1. Scans the latest 1000 Polymarket markets for bias patterns.
2. Identifies high-EV Limit entry points.
3. Records simulated Limit Orders into `paper_portfolio.json`.
4. Saves orderbook ticks (Bids/Asks shifts) to `tick_data.jsonl` every 30s to prove execution.
5. Settles resolved markets based on victory criteria.
"""

import asyncio
import json
import logging
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
import httpx
from datetime import datetime, timezone
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("BiasTrader")

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

# =========================================================
# CONFIGURATION
# =========================================================
BIAS_PATTERNS = {
    "below": 0.931,
    "less": 0.889,
    "between": 0.867,
    "win": 0.782,
    "reach": 0.780,
    "will": 0.755,
}

MIN_EV_THRESHOLD = 0.05
PAPER_TRADE_SIZE_USD = 1.0
MAKER_REBATE = 0.0
PORTFOLIO_FILE = "paper_portfolio.json"
TICK_DATA_FILE = "tick_data.jsonl"


class PortfolioTracker:
    def __init__(self):
        self.balance_usd = 1000.0  # Starting Paper Bankroll
        self.active_orders = {}    # token_id -> order dict
        self.positions = {}        # token_id -> position dict
        self.history = []          # resolved trades history
        self.load()

    def load(self):
        if os.path.exists(PORTFOLIO_FILE):
            with open(PORTFOLIO_FILE, "r") as f:
                data = json.load(f)
                self.balance_usd = data.get("balance_usd", 1000.0)
                self.active_orders = data.get("active_orders", {})
                self.positions = data.get("positions", {})
                self.history = data.get("history", [])

    def save(self):
        with open(PORTFOLIO_FILE, "w") as f:
            json.dump({
                "balance_usd": self.balance_usd,
                "active_orders": self.active_orders,
                "positions": self.positions,
                "history": self.history
            }, f, indent=2)

    def log_tick(self, token_id: str, best_bid: float, best_ask: float, market_id: str):
        tick = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "token_id": token_id,
            "market_id": market_id,
            "best_bid": best_bid,
            "best_ask": best_ask
        }
        with open(TICK_DATA_FILE, "a") as f:
            f.write(json.dumps(tick) + "\n")


@dataclass
class PaperTrade:
    market_id: str
    question: str
    keyword: str
    historical_p: float
    token_id: str
    target_price: float
    ev: float
    bet_size_usd: float

class BiasTradingBot:
    def __init__(self):
        self.httpx_client = httpx.AsyncClient(timeout=15.0)
        self.portfolio = PortfolioTracker()

    async def fetch_active_markets(self) -> List[Dict]:
        try:
            response = await self.httpx_client.get(
                f"{GAMMA_API_URL}/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": 1000
                }
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and "data" in data:
                return data["data"]
            return []
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []

    async def fetch_market_details(self, market_id: str) -> Optional[Dict]:
        try:
            response = await self.httpx_client.get(f"{GAMMA_API_URL}/markets/{market_id}")
            if response.status_code == 200:
                return response.json()
        except:
            pass
        return None

    async def fetch_orderbook(self, token_id: str) -> Optional[Dict]:
        try:
            response = await self.httpx_client.get(
                f"{CLOB_API_URL}/book",
                params={"token_id": token_id}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return None

    def match_bias_pattern(self, question: str) -> Optional[str]:
        words = question.lower().translate(str.maketrans('','', '?,.!"')).split()
        for word in words:
            if word in BIAS_PATTERNS:
                return word
        return None

    async def check_order_fills_and_ticks(self):
        """
        Check if any active OPEN limit orders got filled by looking at current best Ask.
        If Best Ask <= our target price, our Bid was hit!
        """
        filled_tokens = []
        for token_id, order in self.portfolio.active_orders.items():
            ob = await self.fetch_orderbook(token_id)
            if not ob:
                continue
                
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
            best_bid = float(bids[0]["price"]) if bids else 0.0
            best_ask = float(asks[0]["price"]) if asks else 1.0
            
            # Log tick to prove tracking over time
            self.portfolio.log_tick(token_id, best_bid, best_ask, order["market_id"])

            my_price = order["limit_price"]
            
            # If someone is willing to sell at or below our Bid, we are filled
            if best_ask <= my_price:
                logger.info(f"★★★ ORDER FILLED ★★★ Token {token_id[:8]} at ${my_price}")
                
                # Move to positions
                shares_bought = order["size_usd"] / my_price
                self.portfolio.positions[token_id] = {
                    "market_id": order["market_id"],
                    "question": order["question"],
                    "shares": shares_bought,
                    "cost_basis": order["size_usd"],
                    "filled_price": my_price,
                    "filled_at": datetime.now(timezone.utc).isoformat()
                }
                filled_tokens.append(token_id)
                
            # If price moved against us severely, maybe we should cancel or reposition? 
            # For simplicity we let GTC orders hang unless the market closes.
                
        for token_id in filled_tokens:
            del self.portfolio.active_orders[token_id]
            
        self.portfolio.save()

    async def settle_resolved_markets(self):
        """Check positions to see if any markets have resolved."""
        closed_tokens = []
        for token_id, pos in self.portfolio.positions.items():
            market = await self.fetch_market_details(pos["market_id"])
            if not market:
                continue
                
            if market.get("closed", False) or market.get("active", True) == False:
                # Resolve it. Did NO win?
                prices_raw = market.get("outcomePrices", [])
                if isinstance(prices_raw, str):
                    try: prices = json.loads(prices_raw)
                    except: prices = [0, 0]
                else: prices = prices_raw
                
                if len(prices) >= 2:
                    p1 = float(prices[1]) # NO outcome price
                    if p1 >= 0.9: 
                        # NO Won!
                        payout = pos["shares"] * 1.0
                        self.portfolio.balance_usd += payout
                        profit = payout - pos["cost_basis"]
                        logger.info(f"$$$$ MARKET RESOLVED IN PROFITS $$$$ Won {payout:.2f} (Profit: {profit:.2f})")
                        self.portfolio.history.append({"market": pos["question"], "result": "WIN", "profit": profit})
                        closed_tokens.append(token_id)
                    elif float(prices[0]) >= 0.9:
                        # YES Won (We lost)
                        profit = -pos["cost_basis"]
                        logger.info(f"!!!! MARKET RESOLVED AS LOSS !!!! Lost {pos['cost_basis']:.2f}")
                        self.portfolio.history.append({"market": pos["question"], "result": "LOSS", "profit": profit})
                        closed_tokens.append(token_id)

        for token_id in closed_tokens:
            del self.portfolio.positions[token_id]
            
        if closed_tokens:
            self.portfolio.save()

    async def scan_for_opportunities(self):
        markets = await self.fetch_active_markets()
        logger.info(f"Scanning {len(markets)} active markets...")
        
        candidates = [m for m in markets if self.match_bias_pattern(m.get("question", ""))]
        
        for m in candidates:
            question = m.get("question", "")
            keyword = self.match_bias_pattern(question)
            historical_p = BIAS_PATTERNS[keyword]

            cti = m.get("clobTokenIds", "[]")
            if isinstance(cti, str):
                try: cti = json.loads(cti)
                except: cti = []
            if len(cti) < 2: continue
            no_token_id = str(cti[1])
            if not no_token_id: continue
            
            # Skip if we already have an order or position in this token
            if no_token_id in self.portfolio.active_orders or no_token_id in self.portfolio.positions:
                continue

            orderbook = await self.fetch_orderbook(no_token_id)
            if not orderbook: continue

            bids, asks = orderbook.get("bids", []), orderbook.get("asks", [])
            if not bids or not asks: continue
            
            best_bid = float(bids[0]["price"])
            best_ask = float(asks[0]["price"])

            self.portfolio.log_tick(no_token_id, best_bid, best_ask, m.get("id"))

            target_c = best_bid
            if target_c >= best_ask: target_c = best_ask - 0.01
            if target_c <= 0: target_c = 0.01

            ev_per_share = historical_p - target_c + MAKER_REBATE
            
            if ev_per_share > MIN_EV_THRESHOLD:
                # We have a valid opportunity! Place paper order.
                required_balance = PAPER_TRADE_SIZE_USD
                if self.portfolio.balance_usd >= required_balance:
                    self.portfolio.balance_usd -= required_balance
                    self.portfolio.active_orders[no_token_id] = {
                        "market_id": m.get("id"),
                        "question": question,
                        "limit_price": target_c,
                        "size_usd": required_balance,
                        "placed_at": datetime.now(timezone.utc).isoformat()
                    }
                    self.portfolio.save()
                    
                    logger.info(f"[NEW ORDER] Placed Maker Bid for '{question[:50]}...'")
                    logger.info(f"  Target Price: {target_c:.3f} | EV/Share: {ev_per_share:.3f}")
                else:
                    logger.warning("INSUFFICIENT PAPER BANKROLL TO PLACE ORDER")

    async def _run_iteration(self):
        # 1. Settle resolved markets
        await self.settle_resolved_markets()
        
        # 2. Check if resting orders got executed & record ticks
        await self.check_order_fills_and_ticks()
        
        # 3. Look for new opportunities
        await self.scan_for_opportunities()
        
        # Log summary
        pnl = sum([h["profit"] for h in self.portfolio.history])
        logger.info(f"--- STATUS --- Bankroll: ${self.portfolio.balance_usd:.2f} | Open Orders: {len(self.portfolio.active_orders)} | Open Positions: {len(self.portfolio.positions)} | Realized PnL: ${pnl:.2f}")

    async def run(self):
        logger.info(f"Starting BiasTrader Bot (Portfolio Tracker Mode)")
        while True:
            await self._run_iteration()
            logger.info("Sleeping for 30 seconds before next scan...\n")
            await asyncio.sleep(30)

    async def cleanup(self):
        await self.httpx_client.aclose()


async def main_bot():
    bot = BiasTradingBot()
    try:
        await bot.run()
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        await bot.cleanup()
        logger.info("Bot successfully shut down.")

if __name__ == "__main__":
    try:
        asyncio.run(main_bot())
    except KeyboardInterrupt:
        # KeyboardInterrupt might be raised before the loop starts or when caught by the top level
        pass
