#!/usr/bin/env python3
"""
Regime Router — paper bot that re-routes signals via regime classification.

Reads bot-data/regime_per_market.json (refreshed periodically by regime_classifier).
For each open arena market, looks up current regime, opens a paper trade ONLY if
the firing strategy family matches the regime's recommended_families list.

This is a PAPER ADAPTER — sits on top of arena, doesn't replace it. Adds a
parallel positions stream tagged "regime-aligned". Tracks separate equity to
measure: does regime gating improve PnL vs blanket arena?

Output: bot-data/regime_router.json with per-regime equity & comparison.
"""
import asyncio, json, os, time, gc
from collections import defaultdict, deque
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
DATA = "data"
REGIME_FILE = os.path.join(DATA, "regime_per_market.json")
ARENA_FILE = os.path.join(DATA, "arena_results.json")
OUT = os.path.join(DATA, "regime_router.json")
BACKTEST_FILE = os.path.join(DATA, "arena_backtest_full.json")

REFRESH_INTERVAL = 1800   # re-classify regimes every 30 min
SCAN_INTERVAL = 600       # scan arena positions every 10 min
SAVE_INTERVAL = 60
HEADERS = {"User-Agent": "Mozilla/5.0 regime-router"}

PAPER_BET = 50.0
STARTING = 1000.0


class RegimeRouter:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.regimes = {}             # cid → regime dict
        self.last_regime_load = 0
        self.last_scan = 0
        self.last_save = 0
        # Per-regime equity tracking
        self.regime_equity = defaultdict(lambda: STARTING)
        self.regime_trades = defaultdict(int)
        self.regime_wins = defaultdict(int)
        self.regime_losses = defaultdict(int)
        self.open_positions = {}      # cid → {regime, side, entry, opened_ts}
        self.recent_decisions = deque(maxlen=200)
        self._load()

    def _load(self):
        if os.path.exists(OUT):
            try:
                d = json.load(open(OUT))
                for k, v in d.get("regime_equity", {}).items():
                    self.regime_equity[k] = v
                for k, v in d.get("regime_trades", {}).items():
                    self.regime_trades[k] = v
                for k, v in d.get("regime_wins", {}).items():
                    self.regime_wins[k] = v
                for k, v in d.get("regime_losses", {}).items():
                    self.regime_losses[k] = v
                self.open_positions = d.get("open_positions", {})
            except Exception:
                pass

    def save(self):
        out = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "n_regimes_loaded": len(self.regimes),
            "n_open_positions": len(self.open_positions),
            "regime_equity": dict(self.regime_equity),
            "regime_trades": dict(self.regime_trades),
            "regime_wins": dict(self.regime_wins),
            "regime_losses": dict(self.regime_losses),
            "regime_pnl_summary": [
                {"regime": k,
                 "eq": round(v, 2),
                 "pnl": round(v - STARTING, 2),
                 "trades": self.regime_trades[k],
                 "wr": round(self.regime_wins[k] / max(self.regime_wins[k] + self.regime_losses[k], 1) * 100, 1)}
                for k, v in sorted(self.regime_equity.items(), key=lambda kv: -kv[1])
            ],
            "open_positions": self.open_positions,
            "recent_decisions": list(self.recent_decisions)[-30:],
        }
        with open(OUT, "w") as f:
            json.dump(out, f, indent=1)

    def load_regimes(self):
        if not os.path.exists(REGIME_FILE):
            return
        try:
            d = json.load(open(REGIME_FILE))
            self.regimes = d.get("regimes", {})
            self.last_regime_load = time.time()
            print(f"[router] loaded {len(self.regimes)} regimes")
        except Exception as e:
            print(f"[router] regime load err: {e}")

    def family_matches_regime(self, family, cid):
        """True if family is recommended for current regime of this market."""
        regime = self.regimes.get(cid)
        if not regime:
            return False
        return family in regime.get("recommended_families", [])

    async def scan_arena_signals(self, client):
        """Sample new arena entries: find recently-opened positions across
        the live arena, check if any are in markets with known regime.
        """
        if not os.path.exists(ARENA_FILE):
            return
        try:
            d = json.load(open(ARENA_FILE))
        except Exception:
            return
        # We can't extract individual entry events from arena_results
        # (only summary state). Instead: for each strategy with open positions,
        # cross-reference with regime recommendation.
        n_aligned = 0; n_misaligned = 0
        for r in d.get("results", []):
            if r.get("retired"):
                continue
            ind = r.get("params", {}).get("indicator", "")
            # Map indicator → family
            family = ind.split("_")[0] if "_" in ind else ind
            family_map = {"mean": "mean_rev_ema", "wavelet": "wavelet_mr",
                          "rsi": "rsi", "bollinger": "bollinger",
                          "breakout": "breakout", "momentum": "momentum",
                          "zscore": "zscore", "macd": "macd"}
            family = family_map.get(family, family)
            # Note: arena_results doesn't expose per-position details.
            # So we can only count aligned strategies, not aligned trades.
            # For real edge measurement we'd need event stream from arena.
            # This code is the framework; integration left for later.
            pass
        return n_aligned, n_misaligned

    async def fetch_book(self, client, cid):
        try:
            r = await client.get(f"https://clob.polymarket.com/markets/{cid}",
                                 timeout=10.0, headers=HEADERS)
            if r.status_code != 200:
                return None
            d = r.json()
            tokens = d.get("tokens", [])
            if not tokens:
                return None
            token = tokens[0].get("token_id")
            rb = await client.get(f"https://clob.polymarket.com/book?token_id={token}",
                                  timeout=10.0, headers=HEADERS)
            if rb.status_code != 200:
                return None
            book = rb.json()
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if not bids or not asks:
                return None
            return float(bids[-1]["price"]), float(asks[-1]["price"])
        except Exception:
            return None

    async def main_loop(self):
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            print(f"[router] starting")
            while True:
                now = time.time()
                if now - self.last_regime_load > REFRESH_INTERVAL:
                    self.load_regimes()
                if now - self.last_scan > SCAN_INTERVAL:
                    res = await self.scan_arena_signals(client)
                    self.last_scan = now
                if now - self.last_save > SAVE_INTERVAL:
                    self.save()
                    self.last_save = now
                    gc.collect()
                # Status print every 5 min
                if int(now) % 300 < 1:
                    n_pos = len(self.open_positions)
                    n_total = sum(self.regime_trades.values())
                    print(f"[router] {datetime.now():%H:%M} regimes={len(self.regimes)} "
                          f"open={n_pos} trades={n_total}")
                await asyncio.sleep(60)


async def main():
    router = RegimeRouter()
    router.load_regimes()
    try:
        await router.main_loop()
    finally:
        router.save()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
