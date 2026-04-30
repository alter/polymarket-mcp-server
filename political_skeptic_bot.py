#!/usr/bin/env python3
"""
Political Skeptic Bot — bets NO on:
A) Low-base-rate political events ("Putin out", "Trump divorce", "Kim US visit")
   that Polymarket sometimes prices 5-15%, when historical base rate is <2%.
B) Structural-impossibility markets ("Trump reduce deficit", "Ukraine NATO 2025",
   "Musk trillionaire") where institutional analysts (CBO/CRFB/TPC) say no.

Triggered by question matching pattern AND current YES price in safe range.
Bets NO at (1 - bid). Holds to resolution (UMA close).

Per-variant equity (matching arena): $1000 start, $0.01 actual bet, scaled.
State: bot-data/political_skeptic.json
"""
import asyncio, gc, json, os, re, time
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA = "data"
STATE_FILE = os.path.join(DATA, "political_skeptic.json")

BET_USD = 0.01
ARENA_BET_USD = 50.0
EQUITY_SCALE = ARENA_BET_USD / BET_USD
STARTING_BALANCE = 1000.0

# YES price range to enter NO bet (skeptic mode)
PRICE_MIN = 0.02   # below this = trivial 1-bid≈1, but small bet still ok
PRICE_MAX = 0.25   # above this = market sees real probability, fade unsafe

# Per-strategy max price (some patterns can fade at higher prices safely)
STRATEGY_MAX_PRICE = {
    "base_rate": 0.20,
    "impossible": 0.15,
    "stat_pattern": 0.30,  # broader fade for high-base-rate-NO patterns
}

# Time constraints
MIN_HOURS_TO_CLOSE = 6        # avoid only super-late-stage UMA chaos
MAX_HOURS_TO_CLOSE = 1500 * 24  # 4 years (capture 2028 election joke candidates)

SCAN_INTERVAL = 600           # 10 min scan
SETTLE_INTERVAL = 1800        # 30 min settle check
SAVE_INTERVAL = 600

# Pattern groups: (label, regex, strategy_type)
# Statistical basis: from 720 resolved political markets, base rate is 86.9% NO.
# Patterns below have empirically lower YES rates than base.
PATTERNS = [
    # Statistical patterns from 720-market backtest
    ("X_announces",        re.compile(r'\b(announce|declare|sign|publish)\b', re.I), "stat_pattern"),  # 0% YES on n=7
    ("X_wins",             re.compile(r'\b(win|wins|winning|won)\b.*\b(election|primary|nomination|caucus)\b', re.I), "stat_pattern"),  # 5.6% YES n=142


    # A — Base-rate ultra-low events (NEVER in modern history)
    ("putin_out",          re.compile(r'\bputin\b.*\b(out|step\s*down|removed|resign|impeached|killed|ousted)\b', re.I), "base_rate"),
    ("kim_visit_us",       re.compile(r'\bkim\s+jong.*\bvisit\b.*\b(us|usa|america)\b', re.I), "base_rate"),
    ("trump_divorce",      re.compile(r'\b(trump|melania|donald)\b.*\bdivorce\b', re.I), "base_rate"),
    ("scotus_vacancy",     re.compile(r'\b(supreme\s*court|scotus)\b.*\bvacancy\b', re.I), "base_rate"),
    ("xi_out",             re.compile(r'\bxi\s+jinping\b.*\b(out|removed|resign|step)\b', re.I), "base_rate"),
    ("netanyahu_out",      re.compile(r'\bnetanyahu\b.*\b(out|removed|resign|impeached|step)\b', re.I), "base_rate"),
    ("musk_trillionaire",  re.compile(r'\bmusk\b.*\btrillionaire\b', re.I), "base_rate"),
    ("trump_nobel",        re.compile(r'\btrump\b.*\bnobel\s*peace', re.I), "base_rate"),

    # B — Structural impossibility (institutional analysts agree won't happen)
    ("deficit_reduce",     re.compile(r'\btrump\b.*\b(reduce|cut)\b.*\bdeficit\b', re.I), "impossible"),
    ("ukraine_nato",       re.compile(r'\bukraine\b.*\bjoin\b.*\bnato\b', re.I), "impossible"),
    ("ru_ua_ceasefire",    re.compile(r'(russia|ru).*(ukraine|ua).*ceasefire', re.I), "impossible"),
    ("iran_regime_fall",   re.compile(r'\biranian?\s+regime\b.*\b(fall|change)\b', re.I), "impossible"),
    ("trump_remove_eos",   re.compile(r'\btrump\b.*\bremove\b.*\bbiden\s*eo', re.I), "impossible"),
    ("revoke_clearance",   re.compile(r'\brevoke\b.*\bsecurity\s*clearance', re.I), "impossible"),

    # Extended patterns for current active markets
    ("trump_visit_country", re.compile(r'\btrump\b.*\bvisit\b.*\b(china|north\s*korea|russia|iran|cuba|venezuela)\b', re.I), "base_rate"),
    ("china_invades_taiwan", re.compile(r'\bchina\b.*\binvad\w*\b.*\btaiwan\b', re.I), "base_rate"),
    ("trump_third_term",   re.compile(r'\btrump\b.*\bthird\s+term\b', re.I), "base_rate"),
    ("musk_president",     re.compile(r'\bmusk\b.*\bpresident\b', re.I), "base_rate"),
    ("ai_president",       re.compile(r'\b(LeBron|kim\s+kardashian|Tom\s+Brady|Eric\s+Trump|Hunter\s+Biden|hegseth)\b.*\bpresident\b', re.I), "base_rate"),
    ("uk_election_called", re.compile(r'\b(uk|united\s+kingdom)\b.*election\b.*\bcalled\b', re.I), "impossible"),
    ("israel_palestine_peace", re.compile(r'\bisrael\b.*\bpalestin\w*\b.*\b(peace|deal|agreement)\b', re.I), "impossible"),
]


def match_pattern(question):
    for label, regex, strategy_type in PATTERNS:
        if regex.search(question):
            return label, strategy_type
    return None, None


class SkepticBot:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.last_scan = 0
        self.last_settle = 0
        self.last_save = 0
        self.state = {
            "open_positions": {},
            "wins": 0, "losses": 0, "total_bets": 0,
            "realized_pnl": 0.0,
            "by_pattern": {},
        }
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                self.state = json.load(open(STATE_FILE))
                self.state.setdefault("open_positions", {})
                self.state.setdefault("by_pattern", {})
                print(f"[skeptic] loaded: {len(self.state['open_positions'])} open, "
                      f"W/L={self.state['wins']}/{self.state['losses']}, "
                      f"pnl=${self.state['realized_pnl']:+.4f}")
            except Exception as e:
                print(f"[skeptic] load err: {e}")

    def _equity(self):
        return STARTING_BALANCE + self.state["realized_pnl"] * EQUITY_SCALE

    def save(self):
        out = {
            **self.state,
            "equity_arena_scale": round(self._equity(), 2),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(STATE_FILE, "w") as f:
            json.dump(out, f, indent=1)

    async def scan(self):
        # Fetch in 2 pages for broader coverage
        markets = []
        for offset in [0, 200]:
            try:
                r = await self.client.get(
                    f"{GAMMA}/markets",
                    params={"active": "true", "closed": "false", "limit": 200, "offset": offset},
                    timeout=20.0,
                )
                if r.status_code != 200:
                    continue
                batch = r.json()
                markets.extend(batch)
                if len(batch) < 200:
                    break
            except Exception as e:
                print(f"[skeptic] scan err: {e}")

        now = time.time()
        n_pattern_matches = 0
        n_added = 0
        skip_reasons = {"already_open": 0, "no_tokens": 0, "bad_end": 0,
                        "out_of_hours": 0, "no_book": 0, "out_of_price": 0,
                        "bad_entry": 0}
        for m in markets:
            q = m.get("question", "")
            if not q:
                continue
            label, strategy = match_pattern(q)
            if not label:
                continue
            n_pattern_matches += 1

            cid = m.get("conditionId", "")
            if not cid or cid in self.state["open_positions"]:
                skip_reasons["already_open"] += 1
                continue

            tokens_raw = m.get("clobTokenIds", "[]")
            try:
                tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
            except Exception:
                skip_reasons["no_tokens"] += 1
                continue
            if not tokens:
                skip_reasons["no_tokens"] += 1
                continue
            yes_token = tokens[0]

            end = m.get("endDate", "")
            try:
                end_ts = datetime.fromisoformat(end.replace("Z", "+00:00")).timestamp()
            except Exception:
                skip_reasons["bad_end"] += 1
                continue
            hours_to = (end_ts - now) / 3600
            if hours_to < MIN_HOURS_TO_CLOSE or hours_to > MAX_HOURS_TO_CLOSE:
                skip_reasons["out_of_hours"] += 1
                continue

            # Get current orderbook
            try:
                rb = await self.client.get(
                    f"{CLOB}/book?token_id={yes_token}", timeout=10.0)
                if rb.status_code != 200:
                    skip_reasons["no_book"] += 1
                    continue
                book = rb.json()
            except Exception:
                skip_reasons["no_book"] += 1
                continue
            asks = book.get("asks", [])
            bids = book.get("bids", [])
            if not asks or not bids:
                skip_reasons["no_book"] += 1
                continue
            best_ask = float(asks[-1]["price"])
            best_bid = float(bids[-1]["price"])
            mid = (best_ask + best_bid) / 2

            max_p = STRATEGY_MAX_PRICE.get(strategy, PRICE_MAX)
            if not (PRICE_MIN <= mid <= max_p):
                skip_reasons["out_of_price"] += 1
                continue

            # Bet NO at (1 - best_bid) = pay this for NO share
            no_entry = 1 - best_bid
            if not (0.05 <= no_entry <= 0.97):
                skip_reasons["bad_entry"] += 1
                continue
            shares = BET_USD / no_entry

            # Per-pattern concentration cap (avoid loading up on one pattern)
            pattern_count = sum(1 for p in self.state["open_positions"].values()
                                if p.get("label") == label)
            if pattern_count >= 8:
                continue

            self.state["open_positions"][cid] = {
                "cid": cid,
                "market_id": str(m.get("id", "")),
                "side": "NO",
                "entry": round(no_entry, 4),
                "shares": round(shares, 4),
                "cost": BET_USD,
                "trigger_mid": round(mid, 4),
                "hours_to_close": round(hours_to, 1),
                "label": label,
                "strategy": strategy,
                "question": q[:120],
                "opened_ts": now,
            }
            self.state["total_bets"] += 1
            self.state["by_pattern"].setdefault(label, {"opened": 0, "wins": 0, "losses": 0})
            self.state["by_pattern"][label]["opened"] += 1
            n_added += 1

        print(f"[skeptic] {datetime.now():%H:%M} scanned={len(markets)} "
              f"matches={n_pattern_matches} added={n_added} "
              f"skips={skip_reasons} "
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
                # We bet NO → win iff !yes_won
                won = not yes_won
                pnl = (pos["shares"] - pos["cost"]) if won else -pos["cost"]
                if won:
                    self.state["wins"] += 1
                    self.state["by_pattern"][pos["label"]]["wins"] += 1
                else:
                    self.state["losses"] += 1
                    self.state["by_pattern"][pos["label"]]["losses"] += 1
                self.state["realized_pnl"] += pnl
                del self.state["open_positions"][cid]
                print(f"[skeptic] settled {pos['label']} won={won} pnl=${pnl:+.4f}")
            except Exception:
                pass
        self.save()

    async def run(self):
        print(f"[skeptic] starting, {len(PATTERNS)} patterns, "
              f"price [{PRICE_MIN},{PRICE_MAX}], "
              f"hours [{MIN_HOURS_TO_CLOSE/24:.0f},{MAX_HOURS_TO_CLOSE/24:.0f}]d")
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
                print(f"[skeptic] loop err: {e}")
            gc.collect()
            await asyncio.sleep(60)


async def main():
    bot = SkepticBot()
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
