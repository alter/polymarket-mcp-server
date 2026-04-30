#!/usr/bin/env python3
"""
News Trader Bot — reads bot-data/news_signals.jsonl, maps signals to active
Polymarket markets via keyword overlap, places paper bets when high-confidence
match found.

Trading rules:
- Signal score >= 0.6 AND keyword overlap with market question >= 2 distinct words
- Market price 0.10-0.90 (uncertain enough to have edge)
- Direction inferred from keywords:
  * vote_against / filibuster / impeach / resign / oppos → bet NO
  * vote_for / support / sign / pass → bet YES
- Paper bet $0.01 (arena-equiv $50 via 5000x)
- Hold to UMA resolution

State: bot-data/news_trader.json
Per-bet equity tracking, $1000 starting balance.
"""
import asyncio, gc, json, os, re, time
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA = "data"
SIGNALS_FILE = os.path.join(DATA, "news_signals.jsonl")
STATE_FILE = os.path.join(DATA, "news_trader.json")

BET_USD = 0.01
ARENA_BET_USD = 50.0
EQUITY_SCALE = ARENA_BET_USD / BET_USD
STARTING_BALANCE = 1000.0

MIN_SIGNAL_SCORE = 0.6
MIN_KEYWORD_OVERLAP = 2
PRICE_MIN = 0.10
PRICE_MAX = 0.90
MIN_HOURS_TO_CLOSE = 24
MAX_HOURS_TO_CLOSE = 365 * 24

CHECK_INTERVAL = 600       # 10 min
SETTLE_INTERVAL = 1800     # 30 min
SAVE_INTERVAL = 300

# Direction inference from keywords (CONSERVATIVE — only unambiguous signals)
# Per stricter analysis, removed: "vote_general", "senate_action", "house_vote",
# "ceasefire" (could be "fails"), "treaty_sign" (could be "delays"), "confirmation"
# (could be reject) — these are direction-neutral mentions.
NO_KEYWORDS = {"vote_against", "filibuster", "impeach"}     # 0.9, 0.8, 0.7 base
YES_KEYWORDS = {"vote_for", "election_call"}                 # 0.85, 0.7 base
# "resign" removed — depends on market wording (Y resigns = NO on "X stays" markets but YES on "Y resigns by date")
# Other keywords (trump, putin, netanyahu, indictment, sanction) are entity-only — not directional

# Generic stopwords for keyword overlap
STOPWORDS = {"the","a","an","is","are","was","were","be","been","being",
             "to","of","in","on","at","by","for","with","from","up","about",
             "into","through","during","before","after","above","below",
             "and","but","or","not","that","this","these","those","will",
             "would","could","should","may","might","must","shall","can",
             "do","does","did","done","done","have","has","had","having",
             "as","if","while","because","since","when","where","why","how",
             "what","which","who","whom","whose","also","just","only","own",
             "same","so","than","too","very","over","again","further","then"}


def tokens(text):
    return set(w for w in re.sub(r'[^\w\s]', ' ', text.lower()).split()
               if len(w) > 3 and w not in STOPWORDS)


def infer_direction(keywords):
    """Returns 'YES'/'NO'/None — strict requires unambiguous directional keyword.
    Direction-neutral keywords (vote_general, senate_action, etc) → None (skip).
    """
    yes_hits = sum(1 for k in keywords if k in YES_KEYWORDS)
    no_hits = sum(1 for k in keywords if k in NO_KEYWORDS)
    # Strict — must have direction-specific keyword AND no ambiguity
    if yes_hits > 0 and no_hits == 0:
        return "YES"
    if no_hits > 0 and yes_hits == 0:
        return "NO"
    return None  # ambiguous or no directional keyword → skip


class NewsTraderBot:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.last_check = 0
        self.last_settle = 0
        self.last_save = 0
        self.signals_pos = 0  # byte position in signals file
        self.state = {
            "open_positions": {},
            "wins": 0, "losses": 0, "total_bets": 0,
            "realized_pnl": 0.0,
            "signals_processed": 0,
            "signals_matched": 0,
        }
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                self.state = json.load(open(STATE_FILE))
                self.signals_pos = self.state.get("signals_pos", 0)
                self.state.setdefault("open_positions", {})
            except Exception as e:
                print(f"[news_trader] load err: {e}")

    def _equity(self):
        return STARTING_BALANCE + self.state["realized_pnl"] * EQUITY_SCALE

    def save(self):
        out = {
            **self.state,
            "signals_pos": self.signals_pos,
            "equity_arena_scale": round(self._equity(), 2),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(STATE_FILE, "w") as f:
            json.dump(out, f, indent=1)

    def read_new_signals(self):
        """Tail signals file from last position, return list of new signals."""
        if not os.path.exists(SIGNALS_FILE):
            return []
        signals = []
        with open(SIGNALS_FILE, "rb") as f:
            f.seek(self.signals_pos)
            data = f.read()
            self.signals_pos = f.tell()
        for line in data.decode("utf-8", errors="replace").split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                s = json.loads(line)
                if s.get("score", 0) >= MIN_SIGNAL_SCORE:
                    signals.append(s)
            except Exception:
                continue
        return signals

    async def find_matching_market(self, signal):
        """Find active Polymarket market matching signal keywords.
        Returns (market_dict, mid_price, overlap_score) or None.
        """
        sig_tokens = tokens(signal["title"] + " " + signal.get("summary", ""))
        if len(sig_tokens) < 3:
            return None
        # Search gamma for active markets (paginate 2 pages)
        all_markets = []
        for offset in [0, 200]:
            try:
                r = await self.client.get(
                    f"{GAMMA}/markets",
                    params={"active": "true", "closed": "false", "limit": 200, "offset": offset},
                    timeout=15.0,
                )
                if r.status_code != 200:
                    continue
                all_markets.extend(r.json())
            except Exception:
                continue

        best_match = None
        best_overlap = 0
        now = time.time()
        for m in all_markets:
            q = m.get("question", "")
            m_tokens = tokens(q)
            overlap = len(sig_tokens & m_tokens)
            if overlap < MIN_KEYWORD_OVERLAP:
                continue
            # Filter time
            end = m.get("endDate", "")
            try:
                end_ts = datetime.fromisoformat(end.replace("Z", "+00:00")).timestamp()
            except Exception:
                continue
            hours_to = (end_ts - now) / 3600
            if not (MIN_HOURS_TO_CLOSE <= hours_to <= MAX_HOURS_TO_CLOSE):
                continue
            if overlap > best_overlap:
                best_overlap = overlap
                best_match = (m, hours_to)
        return best_match, best_overlap

    async def process_signal(self, signal):
        self.state["signals_processed"] += 1
        direction = infer_direction(signal["keywords"])
        if not direction:
            return
        match_result, overlap = await self.find_matching_market(signal)
        if not match_result:
            return
        m, hours_to = match_result
        cid = m.get("conditionId", "")
        if not cid or cid in self.state["open_positions"]:
            return

        tokens_raw = m.get("clobTokenIds", "[]")
        try:
            mtokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
        except Exception:
            return
        if not mtokens:
            return
        yes_token = mtokens[0]

        try:
            rb = await self.client.get(f"{CLOB}/book?token_id={yes_token}", timeout=10.0)
            if rb.status_code != 200:
                return
            book = rb.json()
        except Exception:
            return
        asks, bids = book.get("asks", []), book.get("bids", [])
        if not asks or not bids:
            return
        best_ask = float(asks[-1]["price"])
        best_bid = float(bids[-1]["price"])
        mid = (best_ask + best_bid) / 2

        if not (PRICE_MIN <= mid <= PRICE_MAX):
            return

        # Place bet
        if direction == "YES":
            entry = best_ask
        else:  # NO
            entry = 1 - best_bid
        if not (0.05 <= entry <= 0.95):
            return
        shares = BET_USD / entry

        self.state["open_positions"][cid] = {
            "cid": cid,
            "side": direction,
            "entry": round(entry, 4),
            "shares": round(shares, 4),
            "cost": BET_USD,
            "trigger_mid": round(mid, 4),
            "hours_to_close": round(hours_to, 1),
            "signal_keywords": signal["keywords"],
            "signal_score": signal["score"],
            "overlap": overlap,
            "question": m.get("question", "")[:120],
            "signal_title": signal["title"][:100],
            "signal_source": signal["source"],
            "opened_ts": time.time(),
        }
        self.state["total_bets"] += 1
        self.state["signals_matched"] += 1
        print(f"[news_trader] OPEN {direction} on '{m.get('question','')[:60]}' "
              f"(overlap={overlap}, mid={mid:.3f}, signal={signal['source']})")

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
                tokens_d = d.get("tokens", [])
                if not tokens_d:
                    continue
                yes_won = tokens_d[0].get("winner", False)
                pos = self.state["open_positions"][cid]
                won = (pos["side"] == "YES" and yes_won) or (pos["side"] == "NO" and not yes_won)
                pnl = (pos["shares"] - pos["cost"]) if won else -pos["cost"]
                if won:
                    self.state["wins"] += 1
                else:
                    self.state["losses"] += 1
                self.state["realized_pnl"] += pnl
                del self.state["open_positions"][cid]
                print(f"[news_trader] settled won={won} pnl=${pnl:+.4f}")
            except Exception:
                pass
        self.save()

    async def run(self):
        print(f"[news_trader] starting, score>={MIN_SIGNAL_SCORE}, "
              f"overlap>={MIN_KEYWORD_OVERLAP}, price [{PRICE_MIN},{PRICE_MAX}]")
        while True:
            try:
                now = time.time()
                if now - self.last_check > CHECK_INTERVAL:
                    new_signals = self.read_new_signals()
                    print(f"[news_trader] {datetime.now():%H:%M} "
                          f"new signals={len(new_signals)} "
                          f"open={len(self.state['open_positions'])} "
                          f"matched_total={self.state['signals_matched']} "
                          f"eq=${self._equity():.2f}")
                    for sig in new_signals:
                        await self.process_signal(sig)
                    self.last_check = now
                    self.save()
                if now - self.last_settle > SETTLE_INTERVAL:
                    await self.settle()
                    self.last_settle = now
            except Exception as e:
                print(f"[news_trader] loop err: {e}")
            gc.collect()
            await asyncio.sleep(60)


async def main():
    bot = NewsTraderBot()
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
