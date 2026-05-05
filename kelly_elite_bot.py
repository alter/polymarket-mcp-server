#!/usr/bin/env python3
"""
Kelly-sized Elite Bot — paper bot that takes ELITE-shortlist signals and
sizes positions by HALF-KELLY criterion based on per-strategy historical WR.

Math (binary Polymarket markets):
  Buy YES at price p_entry:
    Payoff at win:  $1.00 - p_entry  (per $1 bet, gross win = (1-p)/p × bet)
    Loss:           -p_entry         (per $1 bet, lose 100% of bet)
  Net odds: b = (1 - p_entry) / p_entry

  Kelly fraction: f* = (W × b - L) / b = W - L/b
    where W = win probability (our historical WR), L = 1 - W

  HALF-KELLY (industry standard, halves variance for ~75% return):
    f_used = max(0, min(0.4, f* / 2))    # cap at 40% of bankroll

  Position size: bet = f_used × balance

Reference:
  Kelly (1956) "A New Interpretation of Information Rate"
  Thorp (1997) "The Kelly Criterion in Blackjack and Stock Market"
  Half-Kelly recommended by Markowitz, Thorp for real-money sizing under
  parameter uncertainty.

Why Half-Kelly here:
  - Our WR estimates are based on 5 days × ~1500 trades = noisy
  - Uncertainty in true edge → halve to add safety margin
  - Full Kelly maximises log-wealth but with 40-50% drawdowns common
  - Half-Kelly: ~75% of Kelly return, ~25% drawdown — better risk-adjusted

Inputs same as elite_regime_bot: arena_entries.jsonl + final_shortlist + regime_hmm.
Output: bot-data/kelly_paper.json with per-strategy bet sizes and PnL vs flat $50.
"""
import asyncio, json, os, time, gc
from collections import defaultdict, deque
from datetime import datetime, timezone

DATA = "data"
ENTRIES = os.path.join(DATA, "arena_entries.jsonl")
SHORTLIST = os.path.join(DATA, "final_shortlist.json")
REGIME = os.path.join(DATA, "regime_hmm.json")
ARENA = os.path.join(DATA, "arena_results.json")
OUT = os.path.join(DATA, "kelly_paper.json")
POS_FILE = os.path.join(DATA, "kelly_paper_pos.json")

STARTING = 1000.0     # symbolic per-strategy bankroll
HALF_KELLY = 0.5      # use 50% of Kelly fraction
KELLY_CAP = 0.40      # max fraction per trade — never bet >40% bankroll
KELLY_FLOOR = 0.001   # min fraction (skip trade below 0.1%)
DEFAULT_WR = 0.55     # if no history, assume 55% WR (slightly above coin-flip)


FAMILY_MAP = {"mean_rev_sma": "mean_rev_ema", "wavelet_ms": "wavelet_mr"}


def kelly_fraction(wr, entry_price):
    """Half-Kelly fraction for binary Polymarket bet.

    Args:
      wr: win probability (0-1)
      entry_price: cost per share, in $ (e.g. 0.30 = buying YES at 30¢)

    Returns:
      Fraction of bankroll to bet (0-KELLY_CAP). 0 if edge negative.
    """
    p = max(0.01, min(0.99, entry_price))
    if wr <= p:
        return 0.0  # No edge — skip trade
    # Net odds
    b = (1 - p) / p
    L = 1 - wr
    f_full = wr - L / b
    if f_full <= 0:
        return 0.0
    f = f_full * HALF_KELLY
    return min(KELLY_CAP, max(KELLY_FLOOR, f))


class KellyEliteBot:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.elite_ids = set()
        self.strat_wr = {}        # sid → historical WR
        self.regimes = {}
        self.family_to_states = {}
        self.balances = defaultdict(lambda: STARTING)
        self.positions = {}       # mirror_id → pos
        self.realized = 0.0
        self.wins = 0; self.losses = 0
        self.opened = 0
        self.skip_no_edge = 0
        self.skip_not_elite = 0
        self.skip_wrong_regime = 0
        self.file_offset = 0
        self.bet_sizes = []       # all bets for stats
        self.recent = deque(maxlen=200)
        self.last_load = 0
        self._load()

    def _load(self):
        if os.path.exists(POS_FILE):
            try:
                d = json.load(open(POS_FILE))
                self.file_offset = d.get("file_offset", 0)
                self.realized = d.get("realized", 0.0)
                self.wins = d.get("wins", 0)
                self.losses = d.get("losses", 0)
                self.opened = d.get("opened", 0)
                self.skip_no_edge = d.get("skip_no_edge", 0)
                self.skip_not_elite = d.get("skip_not_elite", 0)
                self.skip_wrong_regime = d.get("skip_wrong_regime", 0)
                self.balances = defaultdict(lambda: STARTING, d.get("balances", {}))
                self.positions = d.get("positions", {})
                self.bet_sizes = d.get("bet_sizes", [])[-1000:]
            except Exception:
                pass

    def load_lookups(self):
        # ELITE IDs
        if os.path.exists(SHORTLIST):
            try:
                d = json.load(open(SHORTLIST))
                self.elite_ids = {s["id"] for s in d.get("elite", [])}
                if not self.elite_ids:
                    self.elite_ids = {s["id"] for s in d.get("shortlist", [])}
            except Exception as e:
                print(f"[kelly] shortlist err: {e}")
        # Per-strategy WR
        if os.path.exists(ARENA):
            try:
                d = json.load(open(ARENA))
                for r in d.get("results", []):
                    w, l = r.get("wins", 0), r.get("losses", 0)
                    if w + l >= 50:
                        self.strat_wr[r["id"]] = w / (w + l)
            except Exception as e:
                print(f"[kelly] arena WR err: {e}")
        # Regimes + family map
        if os.path.exists(REGIME):
            try:
                d = json.load(open(REGIME))
                self.regimes = d.get("regimes", {})
                fam_map = {
                    "calm_revert": ["mean_rev_ema","wavelet_mr","bollinger","zscore"],
                    "trend":        ["breakout","momentum","rsi"],
                    "chaos":        ["mean_rev_ema","wavelet_mr"],
                }
                self.family_to_states = defaultdict(list)
                for state, fams in fam_map.items():
                    for f in fams:
                        self.family_to_states[f].append(state)
            except Exception as e:
                print(f"[kelly] regime err: {e}")
        self.last_load = time.time()
        wr_avg = sum(self.strat_wr.values())/len(self.strat_wr) if self.strat_wr else 0
        print(f"[kelly] loaded: elite={len(self.elite_ids)}, "
              f"WR-history={len(self.strat_wr)} (mean WR={wr_avg:.2f}), "
              f"regimes={len(self.regimes)}")

    def regime_match(self, cid, ind):
        regime = self.regimes.get(cid)
        if not regime:
            return None
        family = FAMILY_MAP.get(ind, ind)
        return regime["state"] in self.family_to_states.get(family, [])

    async def consume(self):
        if not os.path.exists(ENTRIES):
            return 0
        with open(ENTRIES) as f:
            f.seek(self.file_offset)
            new = f.readlines()
            self.file_offset = f.tell()
        n = 0
        for line in new:
            try:
                ev = json.loads(line)
            except Exception:
                continue
            sid = ev.get("strat_id")
            if sid not in self.elite_ids:
                self.skip_not_elite += 1
                continue
            cid = ev.get("cid", "")
            ind = ev.get("indicator", "")
            entry = float(ev.get("entry", 0))
            side = ev.get("side", "")
            ts = ev.get("ts", 0)
            match = self.regime_match(cid, ind)
            if match is False:
                self.skip_wrong_regime += 1
                continue
            wr = self.strat_wr.get(sid, DEFAULT_WR)
            f = kelly_fraction(wr, entry)
            if f <= 0:
                self.skip_no_edge += 1
                continue
            bal = self.balances[sid]
            bet = f * bal
            if bet < 1:  # too small
                continue
            shares = bet / max(entry, 0.01)
            mirror_id = f"{sid}_{cid}_{int(ts)}"
            if mirror_id in self.positions:
                continue
            self.positions[mirror_id] = {
                "sid": sid, "cid": cid, "side": side,
                "entry": entry, "ts": ts, "indicator": ind,
                "shares": shares, "bet": bet, "kelly_f": f, "wr_used": wr,
                "regime": self.regimes.get(cid, {}).get("state"),
            }
            self.balances[sid] -= bet
            self.bet_sizes.append(bet)
            self.opened += 1
            self.recent.append({
                "ts": ts, "sid": sid, "cid": cid[:14], "ind": ind,
                "entry": entry, "wr": round(wr, 2),
                "kelly_f": round(f, 3), "bet": round(bet, 2),
                "regime": self.regimes.get(cid, {}).get("state"),
            })
            n += 1
        return n

    def save(self):
        avg_bet = sum(self.bet_sizes)/len(self.bet_sizes) if self.bet_sizes else 0
        max_bet = max(self.bet_sizes) if self.bet_sizes else 0
        out = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "elite_ids": len(self.elite_ids),
            "regimes_loaded": len(self.regimes),
            "WR_history_loaded": len(self.strat_wr),
            "opened": self.opened,
            "open_now": len(self.positions),
            "wins": self.wins, "losses": self.losses,
            "realized_pnl": round(self.realized, 4),
            "avg_bet": round(avg_bet, 2),
            "max_bet": round(max_bet, 2),
            "min_bet": round(min(self.bet_sizes), 2) if self.bet_sizes else 0,
            "skip_not_elite": self.skip_not_elite,
            "skip_wrong_regime": self.skip_wrong_regime,
            "skip_no_edge": self.skip_no_edge,
            "kelly_params": {"half_kelly": HALF_KELLY,
                             "cap": KELLY_CAP, "floor": KELLY_FLOOR},
            "open_positions": dict(list(self.positions.items())[:30]),
            "recent_decisions": list(self.recent)[-30:],
            "balances_top10": dict(sorted(self.balances.items(),
                                          key=lambda kv: -kv[1])[:10]),
        }
        with open(OUT, "w") as f:
            json.dump(out, f, indent=1, default=str)
        with open(POS_FILE, "w") as f:
            json.dump({
                "file_offset": self.file_offset,
                "realized": self.realized,
                "wins": self.wins, "losses": self.losses,
                "opened": self.opened,
                "skip_no_edge": self.skip_no_edge,
                "skip_not_elite": self.skip_not_elite,
                "skip_wrong_regime": self.skip_wrong_regime,
                "balances": dict(self.balances),
                "positions": self.positions,
                "bet_sizes": self.bet_sizes[-1000:],
            }, f, default=str)

    async def run(self):
        print(f"[kelly] starting (HALF_KELLY={HALF_KELLY}, cap={KELLY_CAP})")
        self.load_lookups()
        last_save = 0
        last_status = 0
        while True:
            now = time.time()
            if now - self.last_load > 1800:
                self.load_lookups()
            await self.consume()
            if now - last_save > 60:
                self.save()
                last_save = now
                gc.collect()
            if now - last_status > 300:
                avg = sum(self.bet_sizes)/len(self.bet_sizes) if self.bet_sizes else 0
                print(f"[kelly] {datetime.now():%H:%M} opened={self.opened} "
                      f"avg_bet=${avg:.0f} skip(no_edge/regime/not_elite)="
                      f"{self.skip_no_edge}/{self.skip_wrong_regime}/{self.skip_not_elite}")
                last_status = now
            await asyncio.sleep(15)


async def main():
    bot = KellyEliteBot()
    try:
        await bot.run()
    finally:
        bot.save()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
