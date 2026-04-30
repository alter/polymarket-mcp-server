#!/usr/bin/env python3
"""
Council Bot — meta-aggregator across all sub-bots' open positions.

Logic:
1. Reads all sub-bot state files (LV, theta_decay, political_skeptic,
   whale_follower, news_trader, subpenny, tail_drift, always_no_v*).
2. For each market (cid), counts how many bots are open there + by side.
3. CONSENSUS: when K+ bots same direction on same market → high confidence.
4. CONFLICT: when bots disagree → flag for manual review.
5. Council places paper bet on CONSENSUS markets at HIGHER size (3x default).
6. Per-bot weight optional (bot quality * agreement).

Goal: convert independent signals into ensemble confidence.

State: bot-data/council.json
"""
import asyncio, gc, json, os, time
from collections import defaultdict
from datetime import datetime, timezone

import httpx

CLOB = "https://clob.polymarket.com"
DATA = "data"
STATE_FILE = os.path.join(DATA, "council.json")

ARENA_BET_USD = 50.0
EQUITY_SCALE = 5000.0  # arena-scale ($0.01 actual = $50 virtual)
STARTING_BALANCE = 1000.0
CONSENSUS_K_FAMILIES = 2      # min DISTINCT families agreeing (not raw bot count)
SCAN_INTERVAL = 600
SETTLE_INTERVAL = 1800

# Tiered bet sizing — more diverse families = larger bet
def bet_size_for_family_count(n_families):
    if n_families >= 5:
        return 0.10   # 10x — extremely high confidence
    if n_families >= 4:
        return 0.05   # 5x — strong consensus
    if n_families >= 3:
        return 0.03   # 3x — solid consensus
    return 0.02       # 2x — basic consensus

BET_USD = 0.02  # baseline (used for tracking; actual size varies)

# Bot family extraction — collapses LV's 200 variants into 1 family per primitive
def extract_family(bot_label):
    """RSI/BO/BB/ME/etc are families. LV:RS_p7... → RSI_LV. Skeptic → SKEP. etc."""
    if bot_label.startswith("LV:"):
        prim = bot_label[3:].split("_")[0]
        return f"LV_{prim}"          # LV_RS, LV_BO, LV_BB, LV_ME, etc.
    return bot_label                   # Skeptic, Theta, Whale, NewsTrader, ANv1..5, etc.

# Source state files to read
SOURCES = [
    ("live_validator.json", "LV"),
    ("political_skeptic.json", "Skeptic"),
    ("theta_decay.json", "Theta"),
    ("whale_follower.json", "Whale"),
    ("news_trader.json", "NewsTrader"),
    ("subpenny.json", "Subpenny"),
    ("tail_drift.json", "TailDrift"),
    ("always_no_v1_fee_free.json", "ANv1"),
    ("always_no_v2_with_fees.json", "ANv2"),
    ("always_no_v3_aggressive.json", "ANv3"),
    ("always_no_v4_conservative.json", "ANv4"),
    ("always_no_v5_all_cats.json", "ANv5"),
]


def aggregate_open_positions():
    """Aggregate open positions per cid across all bots.
    Returns:
      consensus: {cid: {'side': X, 'count': N, 'bots': [...], 'question': '...'}}
      conflict:  {cid: {'yes_bots': [...], 'no_bots': [...]}}
      single:    {cid: {'side', 'bot'}}
    """
    by_cid = defaultdict(lambda: {"yes_bots": set(), "no_bots": set(),
                                    "question": "", "any_pos": None})
    for fn, bot_name in SOURCES:
        p = os.path.join(DATA, fn)
        if not os.path.exists(p):
            continue
        try:
            d = json.load(open(p))
        except Exception:
            continue
        # LV variants have nested open_cids per variant
        if fn == "live_validator.json":
            for v in d.get("variants", []):
                if v.get("retired"):
                    continue
                for cid, pos in v.get("open_cids", {}).items():
                    if not cid:
                        continue
                    side = pos.get("side", "")
                    by_cid[cid]["any_pos"] = pos
                    if side == "YES":
                        by_cid[cid]["yes_bots"].add(f"LV:{v['variant'][:30]}")
                    elif side == "NO":
                        by_cid[cid]["no_bots"].add(f"LV:{v['variant'][:30]}")
        else:
            for cid, pos in d.get("open_positions", {}).items():
                if not cid:
                    continue
                side = pos.get("side", "")
                by_cid[cid]["any_pos"] = pos
                if "question" in pos and not by_cid[cid]["question"]:
                    by_cid[cid]["question"] = pos["question"][:120]
                if side == "YES":
                    by_cid[cid]["yes_bots"].add(bot_name)
                elif side == "NO":
                    by_cid[cid]["no_bots"].add(bot_name)

    consensus = {}
    conflict = {}
    single = {}
    for cid, info in by_cid.items():
        yc = len(info["yes_bots"])
        nc = len(info["no_bots"])
        total = yc + nc

        # Collapse to FAMILIES (deduplicate RSI×20 → 1 family)
        yes_fams = set(extract_family(b) for b in info["yes_bots"])
        no_fams = set(extract_family(b) for b in info["no_bots"])
        yfc = len(yes_fams)
        nfc = len(no_fams)

        if yc > 0 and nc > 0:
            # Family-level conflict (not just bot conflict)
            conflict[cid] = {
                "yes_bots": list(info["yes_bots"])[:8],
                "no_bots": list(info["no_bots"])[:8],
                "yes_families": list(yes_fams),
                "no_families": list(no_fams),
                "question": info["question"],
            }
        elif yfc >= CONSENSUS_K_FAMILIES:
            consensus[cid] = {
                "side": "YES",
                "count": yc,
                "n_families": yfc,
                "families": list(yes_fams),
                "bots": list(info["yes_bots"])[:5],
                "question": info["question"],
            }
        elif nfc >= CONSENSUS_K_FAMILIES:
            consensus[cid] = {
                "side": "NO",
                "count": nc,
                "n_families": nfc,
                "families": list(no_fams),
                "bots": list(info["no_bots"])[:5],
                "question": info["question"],
            }
        elif total == 1:
            side = "YES" if yc > 0 else "NO"
            single[cid] = {
                "side": side,
                "bot": list(info["yes_bots"] | info["no_bots"])[0],
                "question": info["question"],
            }
    return consensus, conflict, single


class CouncilBot:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.last_scan = 0
        self.last_settle = 0
        self.state = {
            "open_positions": {},   # cid → council position
            "wins": 0, "losses": 0, "total_bets": 0,
            "realized_pnl": 0.0,
            "by_side": {"YES": 0, "NO": 0},
            "consensus_observed": [],   # last N consensus markets seen
            "conflicts_observed": 0,
        }
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                self.state = json.load(open(STATE_FILE))
                self.state.setdefault("open_positions", {})
                self.state.setdefault("by_side", {"YES": 0, "NO": 0})
                self.state.setdefault("consensus_observed", [])
            except Exception:
                pass

    def _equity(self):
        return STARTING_BALANCE + self.state["realized_pnl"] * EQUITY_SCALE

    def save(self):
        out = {**self.state, "equity_arena_scale": round(self._equity(), 2),
               "updated_at": datetime.now(timezone.utc).isoformat()}
        with open(STATE_FILE, "w") as f:
            json.dump(out, f, indent=1)

    async def fetch_book_price(self, cid):
        """Fetch current best bid/ask from CLOB for cid (need market data)."""
        try:
            r = await self.client.get(f"{CLOB}/markets/{cid}", timeout=10.0)
            if r.status_code != 200:
                return None
            d = r.json()
            tokens = d.get("tokens", [])
            if not tokens:
                return None
            yes_token = tokens[0].get("token_id")
            if not yes_token:
                return None
            rb = await self.client.get(f"{CLOB}/book?token_id={yes_token}", timeout=10.0)
            if rb.status_code != 200:
                return None
            book = rb.json()
            asks = book.get("asks", [])
            bids = book.get("bids", [])
            if not asks or not bids:
                return None
            return float(bids[-1]["price"]), float(asks[-1]["price"])
        except Exception:
            return None

    async def scan(self):
        consensus, conflict, single = aggregate_open_positions()
        self.state["conflicts_observed"] = len(conflict)
        # Track top consensus
        recent = []
        for cid, info in list(consensus.items())[:30]:
            recent.append({
                "cid": cid[:12],
                "side": info["side"],
                "count": info["count"],
                "bots": info["bots"][:5],
                "question": info["question"][:80],
            })
        self.state["consensus_observed"] = recent

        # Open new council positions on consensus markets (family-based tiered sizing)
        n_added = 0
        size_dist = defaultdict(int)
        for cid, info in consensus.items():
            if cid in self.state["open_positions"]:
                continue
            # Family count is the threshold (not raw bot count)
            n_fams = info["n_families"]
            if n_fams < CONSENSUS_K_FAMILIES:
                continue
            bet_size = bet_size_for_family_count(n_fams)
            book = await self.fetch_book_price(cid)
            if book is None:
                continue
            best_bid, best_ask = book
            if info["side"] == "YES":
                entry = best_ask
            else:
                entry = 1 - best_bid
            if not (0.05 <= entry <= 0.95):
                continue
            shares = bet_size / entry
            self.state["open_positions"][cid] = {
                "cid": cid,
                "side": info["side"],
                "entry": round(entry, 4),
                "shares": round(shares, 4),
                "cost": bet_size,
                "bet_size": bet_size,
                "consensus_bot_count": info["count"],
                "consensus_family_count": n_fams,
                "families": info["families"],
                "bots": info["bots"][:5],
                "question": info["question"][:120],
                "opened_ts": time.time(),
            }
            self.state["total_bets"] += 1
            self.state["by_side"][info["side"]] += 1
            size_dist[f"size_{int(bet_size*100)}c"] += 1
            n_added += 1
        if size_dist:
            print(f"[council] new bets sizing: {dict(size_dist)}")

        print(f"[council] {datetime.now():%H:%M} consensus={len(consensus)} "
              f"conflict={len(conflict)} single={len(single)} "
              f"council_open={len(self.state['open_positions'])} added={n_added} "
              f"eq=${self._equity():.2f}")
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
                cost = pos.get("cost") or pos.get("bet_size") or BET_USD
                pnl = (pos["shares"] - cost) if won else -cost
                if won:
                    self.state["wins"] += 1
                else:
                    self.state["losses"] += 1
                self.state["realized_pnl"] += pnl
                del self.state["open_positions"][cid]
                print(f"[council] settled won={won} pnl=${pnl:+.4f} "
                      f"families={pos.get('families')} cost=${cost:.3f}")
            except Exception:
                pass
        self.save()

    async def run(self):
        print(f"[council] starting, K_families={CONSENSUS_K_FAMILIES} (distinct families), "
              f"tiered sizing: K2=$0.02 K3=$0.03 K4=$0.05 K5+=$0.10")
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
                print(f"[council] err: {e}")
            gc.collect()
            await asyncio.sleep(60)


async def main():
    bot = CouncilBot()
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
