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
ENTRIES_FILE = os.path.join(DATA, "arena_entries.jsonl")
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

    def _indicator_to_family(self, ind):
        family_map = {
            "mean_rev_ema": "mean_rev_ema", "mean_rev_sma": "mean_rev_ema",
            "wavelet_mr": "wavelet_mr", "wavelet_ms": "wavelet_mr",
            "rsi": "rsi", "bollinger": "bollinger",
            "breakout": "breakout", "momentum": "momentum",
            "zscore": "zscore", "macd": "macd",
        }
        return family_map.get(ind, ind)

    async def consume_entries(self):
        """Tail bot-data/arena_entries.jsonl, gate each entry by regime alignment.

        For each entry event:
         - Look up market regime
         - Check if strategy family is in recommended_families
         - Bucket the entry under "aligned" / "misaligned" / "no_regime"
         - Track virtual P&L per bucket (using same close price as actual arena)
        """
        # Tail position state — survives restart
        pos_file = os.path.join(DATA, "regime_router_pos.json")
        try:
            pos_state = json.load(open(pos_file))
        except Exception:
            pos_state = {"file_offset": 0}

        last_offset = pos_state.get("file_offset", 0)
        if not os.path.exists(ENTRIES_FILE):
            return
        # Read new lines
        with open(ENTRIES_FILE) as f:
            f.seek(last_offset)
            new_lines = f.readlines()
            new_offset = f.tell()

        n_processed = 0
        for line in new_lines:
            try:
                entry = json.loads(line)
            except Exception:
                continue
            cid = entry.get("cid", "")
            family = self._indicator_to_family(entry.get("indicator", ""))
            regime = self.regimes.get(cid)
            if not regime:
                bucket = "no_regime"
            elif family in regime.get("recommended_families", []):
                bucket = "aligned"
            else:
                bucket = "misaligned"
            self.regime_trades[bucket] += 1
            self.recent_decisions.append({
                "ts": entry.get("ts", 0),
                "cid": cid[:14], "family": family,
                "side": entry.get("side", ""),
                "entry": entry.get("entry", 0),
                "bucket": bucket,
                "regime_vol": regime.get("vol") if regime else None,
                "regime_trend": regime.get("trend") if regime else None,
            })
            n_processed += 1

        # Persist offset
        pos_state["file_offset"] = new_offset
        try:
            json.dump(pos_state, open(pos_file, "w"))
        except Exception:
            pass
        return n_processed

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
                if now - self.last_scan > 30:   # consume entries every 30s
                    n = await self.consume_entries()
                    if n:
                        print(f"[router] consumed {n} new entries from arena_entries.jsonl")
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
