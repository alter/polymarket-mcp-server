#!/usr/bin/env python3
"""
Whale Fade — mass grid search bot.

Generates 5000+ parameter combinations and runs them all in parallel
on the same tick stream. Each variant is a tuple of params with its
own state (positions + aggregate stats). Single shared file output.

Parameter grid:
  spike_threshold: 0.01, 0.015, 0.02, 0.03, 0.05, 0.08, 0.10  (7)
  lookback_min:    1, 2, 5, 10, 30                           (5)
  cooldown_sec:    0, 60, 300, 900, 1800                     (5)
  price_filter:    any, gt30, gt50, gt70, lt30, lt50, lt70   (7)
  direction:       fade, follow                              (2)
  fees_only:       free_only, any                            (2)

Total: 7 × 5 × 5 × 7 × 2 × 2 = 4900 variants
"""
import asyncio, json, os, time
import itertools
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
DATA = "data"
TICKS_PATH = os.path.join(DATA, "arena_ticks.jsonl")
RESULTS_FILE = os.path.join(DATA, "whale_fade_grid.json")
BET_USD = 0.01

HISTORY_WINDOW = 60  # last N ticks per market
SETTLE_INTERVAL = 600
LEADERBOARD_INTERVAL = 600
SAVE_INTERVAL = 300


# ─── Variant generator ─────────────────────────────────────────────────────

SPIKE_THRESHOLDS = [0.01, 0.015, 0.02, 0.03, 0.05, 0.08, 0.10]
LOOKBACK_MIN = [1, 2, 5, 10, 30]
COOLDOWN_SEC = [0, 60, 300, 900, 1800]
PRICE_FILTERS = ["any", "gt30", "gt50", "gt70", "lt30", "lt50", "lt70"]
DIRECTIONS = ["fade", "follow"]
FEES_FILTERS = ["free_only", "any"]


def make_variant_id(idx, st, lm, cs, pf, dr, ff):
    return f"V{idx:04d}_s{int(st*1000)}_l{lm}m_c{cs}_p{pf}_d{dr[0]}_f{ff[0]}"


def passes_price_filter(price, pf):
    if pf == "any": return True
    if pf == "gt30": return price > 0.30
    if pf == "gt50": return price > 0.50
    if pf == "gt70": return price > 0.70
    if pf == "lt30": return price < 0.30
    if pf == "lt50": return price < 0.50
    if pf == "lt70": return price < 0.70
    return True


def make_variants():
    variants = []
    idx = 0
    for st, lm, cs, pf, dr, ff in itertools.product(
            SPIKE_THRESHOLDS, LOOKBACK_MIN, COOLDOWN_SEC,
            PRICE_FILTERS, DIRECTIONS, FEES_FILTERS):
        variants.append({
            "id": idx,
            "name": make_variant_id(idx, st, lm, cs, pf, dr, ff),
            "spike_threshold": st,
            "lookback_min": lm,
            "cooldown_sec": cs,
            "price_filter": pf,
            "direction": dr,
            "fees_filter": ff,
            # State
            "open_cids": {},  # cid → position dict
            "wins": 0,
            "losses": 0,
            "total_bets": 0,
            "realized_pnl": 0.0,
            "last_signal_ts": {},  # mid → ts for cooldown
        })
        idx += 1
    return variants


# ─── Bot ─────────────────────────────────────────────────────────────────

class WhaleFadeGridBot:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.history_per_mkt = defaultdict(lambda: deque(maxlen=HISTORY_WINDOW))
        self.cid_for_mid = {}  # market_id → (cid, fees_on)
        self.variants = make_variants()
        self.tick_position = 0
        self.last_settle = 0
        self.last_leaderboard = 0
        self.last_save = 0
        # Per-variant signal counter (debug)
        self.signals_count = 0
        self.bets_placed = 0
        self._load()

    def _load(self):
        if os.path.exists(RESULTS_FILE):
            try:
                d = json.load(open(RESULTS_FILE))
                stored = {v["id"]: v for v in d.get("variants", [])}
                for v in self.variants:
                    if v["id"] in stored:
                        s = stored[v["id"]]
                        v["wins"] = s.get("wins", 0)
                        v["losses"] = s.get("losses", 0)
                        v["total_bets"] = s.get("total_bets", 0)
                        v["realized_pnl"] = s.get("realized_pnl", 0.0)
                        v["open_cids"] = s.get("open_cids", {})
                        v["last_signal_ts"] = s.get("last_signal_ts", {})
                # Reconstruct cid_for_mid from open positions
                for v in self.variants:
                    for cid, pos in v["open_cids"].items():
                        self.cid_for_mid[pos.get("market_id", "")] = (cid, pos.get("fees_on", False))
                self.tick_position = d.get("tick_position", 0)
                print(f"Loaded state: {len(self.variants)} variants, "
                      f"tick_pos={self.tick_position}, "
                      f"total bets across grid: {sum(v['total_bets'] for v in self.variants)}")
            except Exception as e:
                print(f"Load failed: {e}")

    def save(self):
        # Save trim version (no full history per variant — too big)
        slim = []
        for v in self.variants:
            slim.append({
                "id": v["id"], "name": v["name"],
                "spike_threshold": v["spike_threshold"],
                "lookback_min": v["lookback_min"],
                "cooldown_sec": v["cooldown_sec"],
                "price_filter": v["price_filter"],
                "direction": v["direction"],
                "fees_filter": v["fees_filter"],
                "open_cids": v["open_cids"],
                "wins": v["wins"], "losses": v["losses"],
                "total_bets": v["total_bets"],
                "realized_pnl": round(v["realized_pnl"], 4),
                "last_signal_ts": {k: v2 for k, v2 in
                    list(v["last_signal_ts"].items())[-50:]},  # cap memory
            })
        with open(RESULTS_FILE, "w") as f:
            json.dump({
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "n_variants": len(self.variants),
                "tick_position": self.tick_position,
                "total_signals": self.signals_count,
                "total_bets": self.bets_placed,
                "variants": slim,
            }, f, indent=1)

    async def fetch_cid(self, market_id):
        if market_id in self.cid_for_mid:
            return self.cid_for_mid[market_id]
        try:
            r = await self.client.get(f"{GAMMA}/markets/{market_id}", timeout=5.0)
            if r.status_code == 200:
                d = r.json()
                cid = d.get("conditionId", "")
                fees_on = d.get("feesEnabled", False)
                if cid:
                    self.cid_for_mid[market_id] = (cid, fees_on)
                    return (cid, fees_on)
        except Exception:
            pass
        return (None, False)

    async def consume_ticks(self):
        if not os.path.exists(TICKS_PATH):
            return []
        with open(TICKS_PATH, "rb") as f:
            f.seek(self.tick_position)
            data = f.read()
            self.tick_position = f.tell()
        ticks = []
        for line in data.decode("utf-8", errors="replace").split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
                ts = datetime.fromisoformat(t["ts"].replace("Z", "+00:00")).timestamp()
                ticks.append({
                    "ts": ts, "mid": t["market_id"], "price": float(t["mid"]),
                    "bid": float(t["bid"]), "ask": float(t["ask"]),
                    "fees": t.get("fees", False),
                })
            except Exception:
                continue
        return ticks

    async def process_tick(self, tick):
        mid = tick["mid"]
        ts = tick["ts"]
        price = tick["price"]
        bid = tick["bid"]
        ask = tick["ask"]
        fees_on = tick.get("fees", False)
        history = self.history_per_mkt[mid]
        history.append((ts, price, bid, ask))

        if len(history) < 5:
            return

        # Pre-compute lookback prices for various windows
        lookback_prices = {}  # minutes → old_price
        for lm in LOOKBACK_MIN:
            cutoff = ts - lm * 60
            for past_ts, past_price, _, _ in history:
                if past_ts >= cutoff and past_price > 0:
                    lookback_prices[lm] = past_price
                    break

        if not lookback_prices:
            return

        # Lazy fetch CID only when needed
        cid_pair = None

        any_signal = False
        for v in self.variants:
            old_price = lookback_prices.get(v["lookback_min"])
            if old_price is None or old_price <= 0:
                continue

            change = (price - old_price) / old_price
            if abs(change) < v["spike_threshold"]:
                continue

            if not passes_price_filter(price, v["price_filter"]):
                continue

            # Fees filter
            if v["fees_filter"] == "free_only" and fees_on:
                continue

            # Cooldown
            last_sig = v["last_signal_ts"].get(mid, 0)
            if ts - last_sig < v["cooldown_sec"]:
                continue

            # Need CID — fetch lazy
            if cid_pair is None:
                cid_pair = await self.fetch_cid(mid)
                if not cid_pair[0]:
                    return
            cid, _ = cid_pair

            if cid in v["open_cids"]:
                continue

            # Direction logic
            if v["direction"] == "fade":
                # Spike UP → bet NO; Spike DOWN → bet YES
                side = "NO" if change > 0 else "YES"
            else:  # follow
                side = "YES" if change > 0 else "NO"

            entry = (1 - price) if side == "NO" else price
            if entry < 0.05 or entry > 0.95:
                continue

            shares = BET_USD / entry
            v["open_cids"][cid] = {
                "cid": cid, "market_id": mid,
                "side": side, "entry": round(entry, 4),
                "spike_pct": round(change * 100, 2),
                "old_price": round(old_price, 4),
                "current_price": round(price, 4),
                "fees_on": fees_on,
                "cost": BET_USD, "shares": round(shares, 4),
                "opened_ts": ts,
            }
            v["last_signal_ts"][mid] = ts
            v["total_bets"] += 1
            self.bets_placed += 1
            any_signal = True

        if any_signal:
            self.signals_count += 1

    async def settle_resolutions(self):
        now = time.time()
        if now - self.last_settle < SETTLE_INTERVAL:
            return
        self.last_settle = now

        all_open_cids = set()
        for v in self.variants:
            all_open_cids.update(v["open_cids"].keys())
        if not all_open_cids:
            return

        print(f"\n[settle] checking {len(all_open_cids)} unique open cids...")
        resolved_count = 0
        for cid in all_open_cids:
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

                for v in self.variants:
                    if cid not in v["open_cids"]:
                        continue
                    pos = v["open_cids"][cid]
                    won = (pos["side"] == "YES" and yes_won) or (
                          pos["side"] == "NO" and not yes_won)
                    pnl = (pos["shares"] - pos["cost"]) if won else -pos["cost"]
                    if won:
                        v["wins"] += 1
                    else:
                        v["losses"] += 1
                    v["realized_pnl"] += pnl
                    del v["open_cids"][cid]
                    resolved_count += 1
            except Exception:
                pass

        if resolved_count:
            print(f"[settle] resolved {resolved_count} bet positions across grid")
            self.save()

    def print_leaderboard(self):
        now = time.time()
        if now - self.last_leaderboard < LEADERBOARD_INTERVAL:
            return
        self.last_leaderboard = now

        # Top 10 by realized PnL among variants with ≥10 closed bets
        eligible = [v for v in self.variants if (v["wins"] + v["losses"]) >= 10]
        eligible.sort(key=lambda v: -v["realized_pnl"])

        total_open = sum(len(v["open_cids"]) for v in self.variants)
        total_closed = sum(v["wins"] + v["losses"] for v in self.variants)
        total_pnl = sum(v["realized_pnl"] for v in self.variants)

        print(f"\n━━━ WhaleFade GRID ({datetime.now():%H:%M}) ━━━")
        print(f"  Total: {len(self.variants)} variants, "
              f"{total_open} open positions, {total_closed} closed bets, "
              f"realized=${total_pnl:+.2f}")
        print(f"  Eligible (≥10 closed): {len(eligible)}")
        if eligible:
            print(f"\n  TOP 10 by PnL:")
            for v in eligible[:10]:
                tot = v["wins"] + v["losses"]
                wr = v["wins"] / tot * 100 if tot else 0
                roi = v["realized_pnl"] / (tot * BET_USD) * 100
                print(f"    {v['name'][:55]:<55} "
                      f"{v['wins']:>4}W/{v['losses']:<4}L ({wr:>4.0f}%) "
                      f"${v['realized_pnl']:>+8.4f} ROI {roi:>+6.1f}%")
            print(f"\n  BOTTOM 5:")
            for v in eligible[-5:]:
                tot = v["wins"] + v["losses"]
                wr = v["wins"] / tot * 100 if tot else 0
                roi = v["realized_pnl"] / (tot * BET_USD) * 100
                print(f"    {v['name'][:55]:<55} "
                      f"{v['wins']:>4}W/{v['losses']:<4}L ({wr:>4.0f}%) "
                      f"${v['realized_pnl']:>+8.4f} ROI {roi:>+6.1f}%")

    async def run(self):
        print(f"WhaleFadeGridBot starting with {len(self.variants)} variants")
        print(f"  spike_thresholds: {SPIKE_THRESHOLDS}")
        print(f"  lookback_min: {LOOKBACK_MIN}")
        print(f"  cooldown_sec: {COOLDOWN_SEC}")
        print(f"  price_filters: {PRICE_FILTERS}")
        print(f"  directions: {DIRECTIONS}")
        print(f"  fees_filters: {FEES_FILTERS}")

        # Skip backlog: start from end of file unless tick_position already set
        if self.tick_position == 0 and os.path.exists(TICKS_PATH):
            self.tick_position = os.path.getsize(TICKS_PATH)
            print(f"  Starting from byte {self.tick_position} (skip backlog)")

        while True:
            try:
                ticks = await self.consume_ticks()
                if ticks:
                    for t in ticks:
                        await self.process_tick(t)

                await self.settle_resolutions()
                self.print_leaderboard()

                now = time.time()
                if now - self.last_save > SAVE_INTERVAL:
                    self.save()
                    self.last_save = now
            except Exception as e:
                print(f"Loop error: {e}")
            await asyncio.sleep(30)


async def main():
    bot = WhaleFadeGridBot()
    try:
        await bot.run()
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        bot.save()
        await bot.client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
