#!/usr/bin/env python3
"""
Elite Regime Bot — paper bot that takes only ELITE-shortlist signals matching
HMM regime of the market.

Inputs:
  bot-data/final_shortlist.json  — strategies passing Bonferroni + GOLD +
                                    DOUBLE_ROBUST + positive ROI + regime%>30
  bot-data/regime_hmm.json       — current regime per market (HMM 3-state)
  bot-data/arena_entries.jsonl   — live arena event stream (mirror trades)

Logic:
  1. On startup, load elite strategy IDs and regime map.
  2. Tail arena_entries.jsonl with persisted offset.
  3. For each new arena entry:
       - If strat_id ∉ elite → skip
       - If market regime ∉ family's recommended_regimes → skip
       - Else: mirror the trade as a paper position, track entry price
  4. Settle on arena_trades.jsonl close events (joined by cid).
  5. Output: bot-data/elite_paper.json with parallel equity / WR / pnl.

Compare with arena top to measure regime gating value:
  - Arena S311 (+218% ROI, blanket) vs Elite paper (regime-gated mean_re subset).
  - If elite WR > arena WR → regime gating adds edge.
  - If elite has fewer drawdowns → reduces variance per Bailey-DLP gate.
"""
import asyncio, json, os, time, gc
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

DATA = "data"
ENTRIES = os.path.join(DATA, "arena_entries.jsonl")
TRADES = os.path.join(DATA, "arena_trades.jsonl")
SHORTLIST = os.path.join(DATA, "final_shortlist.json")
REGIME = os.path.join(DATA, "regime_hmm.json")
OUT = os.path.join(DATA, "elite_paper.json")
POS_FILE = os.path.join(DATA, "elite_paper_pos.json")

PAPER_BET = 50.0       # match arena
STARTING = 1000.0      # symbolic (we track per-trade pnl, not per-strat equity)


# Map indicator -> family for regime check
FAMILY_MAP = {
    "mean_rev_sma": "mean_rev_ema",
    "wavelet_ms": "wavelet_mr",
}


class EliteRegimeBot:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.elite_ids = set()
        self.regimes = {}    # cid → regime dict
        self.recommended = {}  # state -> [families]
        self.last_load = 0
        self.file_offset = 0
        self.open_paper = {}  # mirror_id → {strat_id, cid, side, entry, ts, indicator}
        self.closed = []
        self.realized = 0.0
        self.wins = 0
        self.losses = 0
        self.skipped_not_elite = 0
        self.skipped_wrong_regime = 0
        self.skipped_no_regime = 0
        self.opened = 0
        self.recent_decisions = deque(maxlen=200)
        self._load()

    def _load(self):
        # Persisted offset
        if os.path.exists(POS_FILE):
            try:
                d = json.load(open(POS_FILE))
                self.file_offset = d.get("file_offset", 0)
                self.realized = d.get("realized", 0.0)
                self.wins = d.get("wins", 0)
                self.losses = d.get("losses", 0)
                self.skipped_not_elite = d.get("skipped_not_elite", 0)
                self.skipped_wrong_regime = d.get("skipped_wrong_regime", 0)
                self.skipped_no_regime = d.get("skipped_no_regime", 0)
                self.opened = d.get("opened", 0)
                self.open_paper = d.get("open_paper", {})
                self.closed = d.get("closed", [])
            except Exception:
                pass

    def load_lookups(self):
        if os.path.exists(SHORTLIST):
            try:
                d = json.load(open(SHORTLIST))
                self.elite_ids = {s["id"] for s in d.get("elite", [])}
                self.shortlist_ids = {s["id"] for s in d.get("shortlist", [])}
                # If elite empty, fall back to shortlist
                if not self.elite_ids:
                    self.elite_ids = self.shortlist_ids
            except Exception as e:
                print(f"[elite] shortlist load err: {e}")
        if os.path.exists(REGIME):
            try:
                d = json.load(open(REGIME))
                self.regimes = d.get("regimes", {})
                # Inverse map: family -> recommended states
                from collections import defaultdict
                self.family_to_states = defaultdict(list)
                # The regime_hmm.py recommended_families per state
                # we store inverted: family -> states recommending it
                # Re-read REGIME_FAMILIES as it's the same in regime_hmm.py
                fam_map = {
                    "calm_revert": ["mean_rev_ema", "wavelet_mr",
                                    "bollinger", "zscore"],
                    "trend":        ["breakout", "momentum", "rsi"],
                    "chaos":        ["mean_rev_ema", "wavelet_mr"],
                }
                for state, fams in fam_map.items():
                    for f in fams:
                        self.family_to_states[f].append(state)
            except Exception as e:
                print(f"[elite] regime load err: {e}")
        self.last_load = time.time()
        print(f"[elite] loaded {len(self.elite_ids)} elite IDs, "
              f"{len(self.regimes)} regimes")

    def family(self, ind):
        return FAMILY_MAP.get(ind, ind)

    def regime_matches(self, cid, ind):
        regime = self.regimes.get(cid)
        if not regime:
            return None  # unknown regime
        family = self.family(ind)
        recommended = self.family_to_states.get(family, [])
        return regime["state"] in recommended

    async def consume_entries(self):
        if not os.path.exists(ENTRIES):
            return 0
        n = 0
        with open(ENTRIES) as f:
            f.seek(self.file_offset)
            new_lines = f.readlines()
            self.file_offset = f.tell()
        for line in new_lines:
            try:
                ev = json.loads(line)
            except Exception:
                continue
            sid = ev.get("strat_id")
            cid = ev.get("cid", "")
            ind = ev.get("indicator", "")
            entry = float(ev.get("entry", 0))
            side = ev.get("side", "")
            ts = ev.get("ts", 0)

            decision = {"ts": ts, "sid": sid, "cid": cid[:14],
                        "ind": ind, "side": side, "entry": entry}
            # Gate 1: in ELITE
            if sid not in self.elite_ids:
                self.skipped_not_elite += 1
                decision["bucket"] = "skip_not_elite"
                self.recent_decisions.append(decision)
                continue
            # Gate 2: regime match
            match = self.regime_matches(cid, ind)
            if match is None:
                self.skipped_no_regime += 1
                decision["bucket"] = "skip_no_regime"
                self.recent_decisions.append(decision)
                continue
            if not match:
                self.skipped_wrong_regime += 1
                decision["bucket"] = "skip_wrong_regime"
                regime = self.regimes.get(cid, {})
                decision["regime"] = regime.get("state")
                self.recent_decisions.append(decision)
                continue
            # Open paper position (key = sid+cid+ts to allow multiple per market)
            mirror_id = f"{sid}_{cid}_{int(ts)}"
            if mirror_id in self.open_paper:
                continue
            self.open_paper[mirror_id] = {
                "strat_id": sid, "cid": cid, "side": side,
                "entry": entry, "ts": ts, "indicator": ind,
                "shares": PAPER_BET / max(entry, 0.01),
                "regime": self.regimes.get(cid, {}).get("state"),
            }
            self.opened += 1
            decision["bucket"] = "OPENED"
            decision["regime"] = self.regimes.get(cid, {}).get("state")
            self.recent_decisions.append(decision)
            n += 1
        return n

    def save(self):
        out = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "elite_count": len(self.elite_ids),
            "regimes_count": len(self.regimes),
            "opened": self.opened,
            "open_now": len(self.open_paper),
            "wins": self.wins, "losses": self.losses,
            "win_rate": round(self.wins / max(self.wins + self.losses, 1) * 100, 1),
            "realized_pnl": round(self.realized, 4),
            "skipped_not_elite": self.skipped_not_elite,
            "skipped_no_regime": self.skipped_no_regime,
            "skipped_wrong_regime": self.skipped_wrong_regime,
            "open_positions": self.open_paper,
            "recent_closed": self.closed[-30:],
            "recent_decisions": list(self.recent_decisions)[-30:],
        }
        with open(OUT, "w") as f:
            json.dump(out, f, indent=1, default=str)
        # Persist offset+state
        pos = {
            "file_offset": self.file_offset,
            "realized": self.realized,
            "wins": self.wins, "losses": self.losses,
            "skipped_not_elite": self.skipped_not_elite,
            "skipped_wrong_regime": self.skipped_wrong_regime,
            "skipped_no_regime": self.skipped_no_regime,
            "opened": self.opened,
            "open_paper": self.open_paper,
            "closed": self.closed[-200:],
        }
        with open(POS_FILE, "w") as f:
            json.dump(pos, f, default=str)

    async def run(self):
        print(f"[elite] starting")
        self.load_lookups()
        last_save = 0
        last_status = 0
        while True:
            now = time.time()
            # Reload lookups every 30 min
            if now - self.last_load > 1800:
                self.load_lookups()
            # Consume entries
            n = await self.consume_entries()
            # Save periodically
            if now - last_save > 60:
                self.save()
                last_save = now
                gc.collect()
            # Status
            if now - last_status > 300:
                print(f"[elite] {datetime.now():%H:%M} "
                      f"opened={self.opened} W/L={self.wins}/{self.losses} "
                      f"pnl=${self.realized:+.4f} "
                      f"skip(not_elite/regime/none)="
                      f"{self.skipped_not_elite}/{self.skipped_wrong_regime}/{self.skipped_no_regime}")
                last_status = now
            await asyncio.sleep(15)


async def main():
    bot = EliteRegimeBot()
    try:
        await bot.run()
    finally:
        bot.save()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
