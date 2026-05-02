#!/usr/bin/env python3
"""
Live forward validator for top mass_backtest variants.

Reads `bot-data/mass_backtest.json`, picks top N variants by PnL,
deploys them as live paper trading strategies. Tails arena_ticks.jsonl,
computes signals identically to backtest, places paper bets,
settles via CLOB.

State: bot-data/live_validator.json (single file with all variant stats).

Variants run identical primitive logic as mass_backtest.py
(WF, ME, BB, ZS, BO, MO) so live results are comparable to backtest.
"""
import asyncio, gc, json, os, time
from collections import defaultdict, deque
from datetime import datetime, timezone

import httpx

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
DATA = "data"
TICKS_PATH = os.path.join(DATA, "arena_ticks.jsonl")
RESULTS_FILE = os.path.join(DATA, "live_validator.json")
BACKTEST_FILE = os.path.join(DATA, "mass_backtest.json")

BET_USD = 0.01
SETTLE_INTERVAL = 600
LEADERBOARD_INTERVAL = 600
SAVE_INTERVAL = 300
HISTORY_WINDOW = 200  # last N ticks per market

TOP_N = 200  # take top 200 backtest variants for live forward validation

# Per-variant equity tracking (matching arena's STARTING_BALANCE/POSITION_USD).
# Each variant gets virtual $1000 starting equity. We bet BET_USD ($0.01) per
# trade actually but report equity scaled to ARENA-style $50 bet equivalent
# for fair comparison vs arena strategies.
STARTING_BALANCE = 1000.0
ARENA_BET_USD = 50.0  # arena's position_usd
EQUITY_SCALE = ARENA_BET_USD / BET_USD  # 5000x — multiplies realized_pnl
RUIN_EQUITY = 800.0  # retire variant at -20% drawdown (stricter than arena's -50% rule)

# Adaptive bet sizing — promote proven winners, demote regressors
PROMOTE_EQUITY = 1100.0   # equity threshold for promotion to bigger bet
PROMOTE_MIN_N = 30        # min closed bets before promotion
PROMOTED_BET_USD = 0.05   # 5x for promoted variants
DEMOTE_EQUITY = 1030.0    # below this, fall back to base bet (hysteresis vs PROMOTE_EQUITY)

# Multi-tick signal persistence — enter only if last K ticks all show same signal.
# Backtest mass_backtest_persistence.py: k=5 boosts top variants +2 to +6pp ROI.
SIGNAL_PERSISTENCE_K = 3  # require 3 consecutive matching signals (less aggressive than backtest k=5)

# Time-of-day filter — backtest tod_indicator_analysis.py shows US hours (14-22 UTC)
# dominate ALL top variants by huge margin (e.g. BO_p100_follow swide: -43% Asia → +106% US).
# Restrict trading to US+Europe hours (8-22 UTC) by default.
TOD_TRADE_HOURS = (8, 22)  # UTC hour range; signals outside are skipped

# Default TP/SL exit policy matching backtest mass_backtest_tpsl winners.
# Each position is monitored against entry price; on hit, close at TP/SL pnl.
DEFAULT_TP_PCT = 0.10    # exit at +10% of entry (rotated from 0.20 after 2h regression)
DEFAULT_SL_PCT = 0.20    # exit at -20% of entry (rotated from 0.50)


# ─── Primitive signals (same logic as backtest) ────────────────────────────

def sig_wf(mids, ts, spike, lookback_min, direction):
    """Whale fade: spike up → bet (fade=NO/follow=YES)."""
    n = len(mids)
    if n < 2: return 0
    cutoff = ts[-1] - lookback_min * 60
    for j in range(n - 1, -1, -1):
        if ts[j] <= cutoff:
            old = mids[j]
            break
    else:
        return 0
    if old <= 0:
        return 0
    change = (mids[-1] - old) / old
    if abs(change) < spike:
        return 0
    if direction == "fade":
        return -1 if change > 0 else 1
    else:
        return 1 if change > 0 else -1


def sig_me(mids, period, dev_threshold, direction):
    """Mean rev EMA."""
    n = len(mids)
    if n < period + 2: return 0
    alpha = 2.0 / (period + 1)
    e = mids[0]
    for p in mids[1:]:
        e = alpha * p + (1 - alpha) * e
    if e <= 0: return 0
    dev = (mids[-1] - e) / abs(e)
    if abs(dev) < dev_threshold: return 0
    if direction == "fade":
        return -1 if dev > 0 else 1
    else:
        return 1 if dev > 0 else -1


def sig_bb(mids, period, std_mult, direction):
    n = len(mids)
    if n < period: return 0
    window = mids[-period:]
    sma = sum(window) / period
    var = sum((p - sma) ** 2 for p in window) / period
    sd = var ** 0.5
    upper = sma + std_mult * sd
    lower = sma - std_mult * sd
    if mids[-1] > upper:
        return -1 if direction == "fade" else 1
    if mids[-1] < lower:
        return 1 if direction == "fade" else -1
    return 0


def sig_zs(mids, period, threshold, direction):
    n = len(mids)
    if n < period: return 0
    window = mids[-period:]
    sma = sum(window) / period
    var = sum((p - sma) ** 2 for p in window) / period
    sd = var ** 0.5
    if sd == 0: return 0
    z = (mids[-1] - sma) / sd
    if abs(z) < threshold: return 0
    if direction == "fade":
        return -1 if z > 0 else 1
    else:
        return 1 if z > 0 else -1


def sig_bo(mids, period, direction):
    n = len(mids)
    if n < period: return 0
    window = mids[-period:]
    hi, lo = max(window), min(window)
    if mids[-1] >= hi * 0.999:
        return -1 if direction == "fade" else 1
    if mids[-1] <= lo * 1.001:
        return 1 if direction == "fade" else -1
    return 0


def sig_mo(mids, period, threshold, direction):
    n = len(mids)
    if n <= period: return 0
    old = mids[-period - 1]
    if old <= 0: return 0
    m = (mids[-1] - old) / old
    if abs(m) < threshold: return 0
    if direction == "follow":
        return 1 if m > 0 else -1
    else:
        return -1 if m > 0 else 1


def sig_mv(mids, ts, K, direction="follow"):
    """MetaVote K-of-8 ensemble: 8 voters from forward-validated families.
    Returns +1 if K+ voters say YES, -1 if K+ NO, else 0.
    direction kept for naming compatibility but always treated as follow.
    """
    voters = [
        sig_rs(mids, 7, 75, "follow"),
        sig_rs(mids, 14, 70, "follow"),
        sig_rs(mids, 21, 80, "follow"),
        sig_bo(mids, 100, "follow"),
        sig_bo(mids, 50, "follow"),
        sig_bb(mids, 20, 2.0, "follow"),
        sig_bb(mids, 10, 1.5, "follow"),
        sig_zs(mids, 20, 2.0, "follow"),
    ]
    yes_n = sum(1 for v in voters if v == 1)
    no_n = sum(1 for v in voters if v == -1)
    if yes_n >= K:
        return 1
    if no_n >= K:
        return -1
    return 0


def sig_rs(mids, period, ob_threshold, direction):
    """Wilder's RSI on last `period+1` ticks, return +1/-1/0."""
    n = len(mids)
    if n < period + 2:
        return 0
    diffs = [mids[i] - mids[i-1] for i in range(1, n)]
    gains = [d if d > 0 else 0 for d in diffs]
    losses = [-d if d < 0 else 0 for d in diffs]
    # Wilder's smoothing
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(diffs)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss < 1e-9:
        rs = 100.0
    else:
        rs = avg_gain / avg_loss
    rsi = 100 - 100 / (1 + rs)
    os_threshold = 100 - ob_threshold
    if rsi >= ob_threshold:
        return 1 if direction == "follow" else -1
    if rsi <= os_threshold:
        return -1 if direction == "follow" else 1
    return 0


def parse_variant(name):
    """Parse variant name → params.

    Supports 3-part legacy: 'BO_p10_fade|pgt30|fany'
    and 4-part with spread filter: 'BO_p10_fade|pgt30|stight|fany'
    """
    parts = name.split("|")
    sig_part = parts[0]  # e.g. BO_p10_fade
    pf = "any"
    sf = "any"
    ff = "any"
    # Detect by prefix: p=price, s=spread, f=fees
    for tok in parts[1:]:
        if tok.startswith("p"):
            pf = tok[1:]
        elif tok.startswith("s"):
            sf = tok[1:]
        elif tok.startswith("f"):
            ff = tok[1:]

    sig_parts = sig_part.split("_")
    fam = sig_parts[0]
    # MV variants encode K in family name: "MV5", "MV3" → fam=MV, K=5/3
    if fam.startswith("MV") and len(fam) > 2 and fam[2:].isdigit():
        K_extracted = int(fam[2:])
        fam = "MV"
    else:
        K_extracted = None
    direction = sig_parts[-1]  # last token is fade/follow

    params = {
        "fam": fam, "direction": direction,
        "price_filter": pf, "spread_filter": sf, "fees_filter": ff,
    }

    if fam == "WF":
        # WF_s10_l1_fade
        spike_int = int(sig_parts[1].lstrip("s"))
        lm = int(sig_parts[2].lstrip("l"))
        params["spike"] = spike_int / 1000
        params["lookback_min"] = lm
    elif fam == "ME":
        # ME_p5_t10_fade
        params["period"] = int(sig_parts[1].lstrip("p"))
        params["thresh"] = int(sig_parts[2].lstrip("t")) / 1000
    elif fam == "BB":
        # BB_p20_sd20_fade
        params["period"] = int(sig_parts[1].lstrip("p"))
        params["std_mult"] = int(sig_parts[2].lstrip("sd")) / 10
    elif fam == "ZS":
        # ZS_p20_t20_fade
        params["period"] = int(sig_parts[1].lstrip("p"))
        params["thresh"] = int(sig_parts[2].lstrip("t")) / 10
    elif fam == "BO":
        # BO_p10_fade
        params["period"] = int(sig_parts[1].lstrip("p"))
    elif fam == "MO":
        # MO_p10_t20_fade
        params["period"] = int(sig_parts[1].lstrip("p"))
        params["thresh"] = int(sig_parts[2].lstrip("t")) / 1000
    elif fam == "RS":
        # RS_p7_t80_follow
        params["period"] = int(sig_parts[1].lstrip("p"))
        params["ob"] = int(sig_parts[2].lstrip("t"))
    elif fam == "MV":
        # MV5_follow → K=5; direction kept "follow" for consistency
        params["K"] = K_extracted
    elif fam == "SR":
        # SR_t90_fade  (spread regime, threshold = sp_thr*100 int)
        params["sp_thr"] = int(sig_parts[1].lstrip("t")) / 100
    return params


def passes_spread(bid, ask, mid, sf):
    """Spread filter: matches mass_backtest.py spread_mask()."""
    if sf == "any":
        return True
    if mid <= 0:
        return False
    spr_pct = (ask - bid) / mid
    if sf == "tight":
        return spr_pct < 0.02
    if sf == "wide":
        return spr_pct > 0.05
    return True


def sig_sr(mids, ts, bids, asks, sp_thr, direction):
    """Spread regime — last 50-tick baseline, drift on compression."""
    n = len(mids)
    if n < 60:
        return 0
    if mids[-1] <= 0:
        return 0
    cur_spr = (asks[-1] - bids[-1]) / mids[-1] if mids[-1] > 0 else 1.0
    base = sum((asks[i] - bids[i]) / max(mids[i], 1e-9) for i in range(-50, 0)) / 50.0
    if cur_spr >= sp_thr * base:
        return 0
    if mids[-6] <= 0:
        return 0
    drift = (mids[-1] - mids[-6]) / mids[-6]
    if abs(drift) < 0.005:
        return 0
    if direction == "follow":
        return 1 if drift > 0 else -1
    else:
        return -1 if drift > 0 else 1


def compute_signal(params, mids, ts, bids=None, asks=None):
    """Apply primitive based on params."""
    fam = params["fam"]
    if fam == "WF":
        return sig_wf(mids, ts, params["spike"], params["lookback_min"], params["direction"])
    if fam == "ME":
        return sig_me(mids, params["period"], params["thresh"], params["direction"])
    if fam == "BB":
        return sig_bb(mids, params["period"], params["std_mult"], params["direction"])
    if fam == "ZS":
        return sig_zs(mids, params["period"], params["thresh"], params["direction"])
    if fam == "BO":
        return sig_bo(mids, params["period"], params["direction"])
    if fam == "MO":
        return sig_mo(mids, params["period"], params["thresh"], params["direction"])
    if fam == "RS":
        return sig_rs(mids, params["period"], params["ob"], params["direction"])
    if fam == "SR" and bids is not None and asks is not None:
        return sig_sr(mids, ts, bids, asks, params["sp_thr"], params["direction"])
    if fam == "MV":
        return sig_mv(mids, ts, params["K"], params["direction"])
    return 0


def passes_price(price, pf):
    if pf == "any": return True
    if pf == "gt30": return price > 0.30
    if pf == "gt50": return price > 0.50
    if pf == "gt70": return price > 0.70
    if pf == "lt30": return price < 0.30
    if pf == "lt50": return price < 0.50
    if pf == "lt70": return price < 0.70
    return True


# ─── Live bot ──────────────────────────────────────────────────────────────

class LiveValidator:
    def __init__(self):
        os.makedirs(DATA, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=15.0)
        self.history_per_mkt = defaultdict(lambda: deque(maxlen=HISTORY_WINDOW))
        self.cid_for_mid = {}  # market_id → (cid, fees_on)
        self.tick_position = 0
        self.last_settle = 0
        self.last_leaderboard = 0
        self.last_save = 0

        # Load top variants from backtest
        self.variants = self._load_top_variants(TOP_N)
        # Seed MetaVote ensemble variants from mass_backtest_metavote.json
        # (forward-validated K-of-N voting; not in baseline mass_backtest grid)
        self._seed_metavote_variants()
        # State per variant
        for v in self.variants:
            v["open_cids"] = {}  # cid → position dict
            v["wins"] = 0
            v["losses"] = 0
            v["total_bets"] = 0
            v["realized_pnl"] = 0.0
            v["last_signal_ts"] = {}
            v["exit_reason_counts"] = {}
            # Virtual equity (arena-comparable), retired if depleted
            v["equity"] = STARTING_BALANCE
            v["retired"] = False
            v["retired_at"] = None
            # tpsl-tuned variants set tp_pct/sl_pct in load; default for others
            if "tp_pct" not in v:
                v["tp_pct"] = DEFAULT_TP_PCT
            if "sl_pct" not in v:
                v["sl_pct"] = DEFAULT_SL_PCT
            v["params"] = parse_variant(v["variant"])

        self._load_state()

    def _load_top_variants(self, top_n):
        """Mix three sources:
        (A) 50% by PnL from baseline (n>=1000) — robust large-sample
        (B) 25% by ROI from baseline (100 <= n < 1000, ROI>=30%) — high-edge small
        (C) 25% from mass_backtest_tpsl with per-variant best tp/sl combo
        """
        if not os.path.exists(BACKTEST_FILE):
            print(f"WARN: {BACKTEST_FILE} not found")
            return []
        d = json.load(open(BACKTEST_FILE))
        results = d.get("results", [])

        big = [r for r in results if r.get("n_bets", 0) >= 1000]
        big.sort(key=lambda r: -r.get("pnl", 0))
        n_a = int(top_n * 0.50)
        a = big[:n_a]
        seen = {r["variant"] for r in a}

        small_high = [
            r for r in results
            if 100 <= r.get("n_bets", 0) < 1000
            and r.get("roi_pct", 0) >= 30
            and r["variant"] not in seen
        ]
        small_high.sort(key=lambda r: -r.get("roi_pct", 0))
        n_b = int(top_n * 0.25)
        b = small_high[:n_b]
        seen.update(r["variant"] for r in b)

        # (C) Add MORE variants from baseline by ROI (no overlap), expand to top_n
        more = [r for r in big if r["variant"] not in seen]
        more.sort(key=lambda r: -r.get("pnl", 0))
        n_c = top_n - n_a - n_b
        c = more[:n_c]
        seen.update(r["variant"] for r in c)

        merged = a + b + c

        # Override tp/sl per-variant from mass_backtest_tpsl for known winners
        tpsl_file = "data/mass_backtest_tpsl.json"
        n_tuned = 0
        if os.path.exists(tpsl_file):
            try:
                tpsl_d = json.load(open(tpsl_file))
                # Build base -> best tpsl combo
                best_per_base = {}
                for r in tpsl_d.get("results", []):
                    parts = r["variant"].split("|")
                    if len(parts) < 5:
                        continue
                    base = "|".join(parts[:4])
                    suffix = parts[4]
                    if not suffix.startswith("tp"):
                        continue
                    try:
                        tp_str, sl_str = suffix[2:].split("sl")
                        tp_pct = int(tp_str) / 100
                        sl_pct = int(sl_str) / 100
                    except Exception:
                        continue
                    if r.get("n_bets", 0) < 100:
                        continue
                    cur = best_per_base.get(base)
                    if cur is None or r.get("pnl", 0) > cur["pnl"]:
                        best_per_base[base] = {
                            "pnl": r.get("pnl", 0),
                            "tp_pct": tp_pct, "sl_pct": sl_pct,
                            "tpsl_roi": r.get("roi_pct", 0),
                        }
                # Apply override
                for v in merged:
                    base = v["variant"]
                    if base in best_per_base:
                        v["tp_pct"] = best_per_base[base]["tp_pct"]
                        v["sl_pct"] = best_per_base[base]["sl_pct"]
                        n_tuned += 1
            except Exception as e:
                print(f"  tpsl override err: {e}")

        print(f"  Loaded {len(a)} by-PnL + {len(b)} high-ROI + {len(c)} extra "
              f"= {len(merged)} variants ({n_tuned} tpsl-tuned)")
        return merged

    def _seed_metavote_variants(self):
        """Append top MetaVote configs (from mass_backtest_metavote.json) as
        new variants. Each config is K∈{2..5}-of-8 voting + price/spread/fee filter.
        """
        path = "data/mass_backtest_metavote.json"
        if not os.path.exists(path):
            return
        try:
            d = json.load(open(path))
        except Exception:
            return
        # Top eligible (n>=300 — ensure not noise) ranked by ROI
        eligible = [r for r in d.get("results", []) if r.get("n_bets", 0) >= 300]
        eligible.sort(key=lambda r: -r.get("roi_pct", 0))
        # Take top 6 — keeps fleet manageable
        existing = {v["variant"] for v in self.variants}
        added = 0
        for r in eligible[:6]:
            # config like "K5|pgt30|sany|fany" → variant "MV5_follow|pgt30|sany|fany"
            cfg = r["config"]
            parts = cfg.split("|")
            K_part = parts[0]              # K5
            K_num = int(K_part.lstrip("K"))
            filters = "|".join(parts[1:])  # pgt30|sany|fany
            variant_name = f"MV{K_num}_follow|{filters}"
            if variant_name in existing:
                continue
            self.variants.append({
                "variant": variant_name,
                "backtest_roi": r.get("roi_pct", 0),
                "backtest_n": r.get("n_bets", 0),
                "backtest_wr": r.get("wr", 0),
                "source": "metavote",
            })
            existing.add(variant_name)
            added += 1
        if added:
            print(f"  Seeded {added} MetaVote variants from {path}")

    def _load_state(self):
        if os.path.exists(RESULTS_FILE):
            try:
                d = json.load(open(RESULTS_FILE))
                stored = {v["variant"]: v for v in d.get("variants", [])}
                for v in self.variants:
                    s = stored.get(v["variant"])
                    if s:
                        v["wins"] = s.get("wins_live", 0)
                        v["losses"] = s.get("losses_live", 0)
                        v["total_bets"] = s.get("total_bets_live", 0)
                        v["realized_pnl"] = s.get("realized_pnl_live", 0.0)
                        v["open_cids"] = s.get("open_cids", {})
                        v["last_signal_ts"] = s.get("last_signal_ts", {})
                        v["exit_reason_counts"] = s.get("exit_reason_counts", {})
                        v["win3_last3"] = s.get("win3_last3", [])
                        v["win3_skip_next"] = s.get("win3_skip_next", False)
                        v["win3_skips"] = s.get("win3_skips", 0)
                        v["equity"] = s.get("equity", STARTING_BALANCE + v["realized_pnl"] * EQUITY_SCALE)
                        v["retired"] = s.get("retired", False)
                        v["retired_at"] = s.get("retired_at")
                        v["promoted"] = s.get("promoted", False)
                        # One-time retroactive prune — apply current RUIN_EQUITY threshold
                        # to existing variants regardless of when their last close was
                        if not v["retired"] and v["equity"] < RUIN_EQUITY:
                            v["retired"] = True
                            v["retired_at"] = datetime.now(timezone.utc).isoformat()
                        # Stored tp/sl is restored only if not already set by tpsl loader.
                        # This preserves per-variant tuning after restart.
                        if "tp_pct" not in v:
                            v["tp_pct"] = s.get("tp_pct", DEFAULT_TP_PCT)
                            v["sl_pct"] = s.get("sl_pct", DEFAULT_SL_PCT)
                self.tick_position = d.get("tick_position", 0)
                print(f"Loaded state: {len(self.variants)} variants, "
                      f"tick_pos={self.tick_position}")
            except Exception as e:
                print(f"Load failed: {e}")

    def save(self):
        slim = []
        for v in self.variants:
            slim.append({
                "variant": v["variant"],
                "backtest_pnl": v.get("pnl", 0),
                "backtest_wr": v.get("wr", 0),
                "backtest_n_bets": v.get("n_bets", 0),
                "backtest_roi": v.get("roi_pct", 0),
                # Live forward stats
                "wins_live": v["wins"],
                "losses_live": v["losses"],
                "total_bets_live": v["total_bets"],
                "realized_pnl_live": round(v["realized_pnl"], 4),
                "live_roi_pct": round(
                    v["realized_pnl"] / max(v["total_bets"] * BET_USD, 0.01) * 100, 2),
                "open_cids": v["open_cids"],
                "exit_reason_counts": v.get("exit_reason_counts", {}),
                "tp_pct": v.get("tp_pct", DEFAULT_TP_PCT),
                "sl_pct": v.get("sl_pct", DEFAULT_SL_PCT),
                "win3_last3": v.get("win3_last3", []),
                "win3_skip_next": v.get("win3_skip_next", False),
                "win3_skips": v.get("win3_skips", 0),
                "equity": round(v.get("equity", STARTING_BALANCE), 2),
                "retired": v.get("retired", False),
                "retired_at": v.get("retired_at"),
                "promoted": v.get("promoted", False),
                "last_signal_ts": {k: vv for k, vv in
                    list(v["last_signal_ts"].items())[-30:]},
            })
        with open(RESULTS_FILE, "w") as f:
            json.dump({
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "n_variants": len(self.variants),
                "tick_position": self.tick_position,
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

    def _record_close(self, v, pnl, reason):
        """Update variant stats on close + Win3-skip filter state.
        Win3: if sum of last 3 closed PnLs < 0, set skip_next.
        Equity: scaled to arena-equivalent ($50/bet vs $0.01).
        Retire variant if equity drops below RUIN_EQUITY.
        """
        v["realized_pnl"] += pnl
        v["equity"] = STARTING_BALANCE + v["realized_pnl"] * EQUITY_SCALE
        if v["equity"] < RUIN_EQUITY and not v.get("retired"):
            v["retired"] = True
            v["retired_at"] = datetime.now(timezone.utc).isoformat()
        if pnl > 0:
            v["wins"] += 1
        else:
            v["losses"] += 1
        v.setdefault("exit_reason_counts", {})
        v["exit_reason_counts"][reason] = v["exit_reason_counts"].get(reason, 0) + 1
        # Win3 update
        last3 = v.setdefault("win3_last3", [])
        last3.append(round(pnl, 6))
        if len(last3) > 3:
            last3.pop(0)
        if len(last3) == 3 and sum(last3) < 0:
            v["win3_skip_next"] = True

    def check_tpsl_exits(self, mid, ts, price):
        """For each variant with open position on this market, check TP/SL hit.
        Uses pos['bet_size'] for actual cost (supports adaptive sizing).
        """
        for v in self.variants:
            tp_pct = v.get("tp_pct", DEFAULT_TP_PCT)
            sl_pct = v.get("sl_pct", DEFAULT_SL_PCT)
            cids_to_close = []
            for cid, pos in v["open_cids"].items():
                if pos.get("market_id") != mid:
                    continue
                e = pos["entry"]
                p_adj = price if pos["side"] == "YES" else (1 - price)
                bet_size = pos.get("bet_size", BET_USD)
                if p_adj >= e * (1 + tp_pct):
                    cids_to_close.append((cid, "tp", bet_size * tp_pct))
                elif p_adj <= e * (1 - sl_pct):
                    cids_to_close.append((cid, "sl", -bet_size * sl_pct))
            for cid, reason, pnl in cids_to_close:
                self._record_close(v, pnl, reason)
                del v["open_cids"][cid]

    async def process_tick(self, tick):
        mid = tick["mid"]
        ts = tick["ts"]
        price = tick["price"]
        bid = tick["bid"]
        ask = tick["ask"]
        fees_on = tick.get("fees", False)
        history = self.history_per_mkt[mid]
        history.append((ts, price, bid, ask))

        # Check TP/SL on existing open positions for this market BEFORE signals
        self.check_tpsl_exits(mid, ts, price)

        if len(history) < 5:
            return

        # Build mids/bids/asks/ts arrays for signal computation
        mids = [h[1] for h in history]
        bids_arr = [h[2] for h in history]
        asks_arr = [h[3] for h in history]
        ts_arr = [h[0] for h in history]

        cid_pair = None  # lazy fetch

        # Time-of-day check (UTC hour) — skip Asia/Off hours where backtest shows losses
        utc_hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour
        if not (TOD_TRADE_HOURS[0] <= utc_hour < TOD_TRADE_HOURS[1]):
            return

        for v in self.variants:
            # Skip retired variants (equity ruined)
            if v.get("retired"):
                continue
            params = v["params"]
            # Fees filter
            if params["fees_filter"] == "free_only" and fees_on:
                continue
            # Price filter
            if not passes_price(price, params["price_filter"]):
                continue
            # Spread filter
            if not passes_spread(bid, ask, price, params.get("spread_filter", "any")):
                continue
            # Cooldown 60s default
            last_sig = v["last_signal_ts"].get(mid, 0)
            if ts - last_sig < 60:
                continue

            sig = compute_signal(params, mids, ts_arr, bids_arr, asks_arr)
            if sig == 0:
                continue

            # Multi-tick persistence: require K-1 prior ticks also signaled same direction.
            # We compute signal at lag 1..K-1 windows over history. Cheap approximation:
            # check sigs[-K:] all == sig by recomputing on truncated history.
            if SIGNAL_PERSISTENCE_K > 1 and len(mids) >= SIGNAL_PERSISTENCE_K + 5:
                persistent = True
                for lag in range(1, SIGNAL_PERSISTENCE_K):
                    prior = compute_signal(
                        params,
                        mids[:-lag], ts_arr[:-lag],
                        bids_arr[:-lag] if bids_arr else None,
                        asks_arr[:-lag] if asks_arr else None,
                    )
                    if prior != sig:
                        persistent = False
                        break
                if not persistent:
                    continue

            # Win3-skip filter: if last 3 closed PnLs summed < 0, skip this entry.
            # Flag is single-use (consumed on next signal).
            if v.get("win3_skip_next"):
                v["win3_skip_next"] = False
                v["win3_skips"] = v.get("win3_skips", 0) + 1
                continue

            # Need CID
            if cid_pair is None:
                cid_pair = await self.fetch_cid(mid)
                if not cid_pair[0]:
                    return
            cid = cid_pair[0]
            if cid in v["open_cids"]:
                continue

            side = "YES" if sig == 1 else "NO"
            entry = (1 - price) if side == "NO" else price
            if entry < 0.05 or entry > 0.95:
                continue

            # Adaptive bet sizing — promote/demote with hysteresis
            n_closed = v.get("wins", 0) + v.get("losses", 0)
            cur_eq = v.get("equity", STARTING_BALANCE)
            was_promoted = v.get("promoted", False)
            if was_promoted and cur_eq < DEMOTE_EQUITY:
                # Demote: dropped below hysteresis threshold
                v["promoted"] = False
                bet_size = BET_USD
            elif not was_promoted and cur_eq >= PROMOTE_EQUITY and n_closed >= PROMOTE_MIN_N:
                # Promote: hit threshold with sufficient sample
                v["promoted"] = True
                bet_size = PROMOTED_BET_USD
            elif was_promoted:
                bet_size = PROMOTED_BET_USD
            else:
                bet_size = BET_USD

            shares = bet_size / entry
            v["open_cids"][cid] = {
                "cid": cid, "market_id": mid,
                "side": side, "entry": round(entry, 4),
                "current_price": round(price, 4),
                "fees_on": fees_on,
                "cost": bet_size, "shares": round(shares, 4),
                "bet_size": bet_size,
                "opened_ts": ts,
            }
            v["last_signal_ts"][mid] = ts
            v["total_bets"] += 1

    async def settle_resolutions(self):
        now = time.time()
        if now - self.last_settle < SETTLE_INTERVAL:
            return
        self.last_settle = now

        all_open = set()
        for v in self.variants:
            all_open.update(v["open_cids"].keys())
        if not all_open:
            return

        for cid in all_open:
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
                    self._record_close(v, pnl, "htr")
                    del v["open_cids"][cid]
            except Exception:
                pass
        self.save()

    def print_leaderboard(self):
        now = time.time()
        if now - self.last_leaderboard < LEADERBOARD_INTERVAL:
            return
        self.last_leaderboard = now

        eligible = [v for v in self.variants if (v["wins"] + v["losses"]) >= 5]
        eligible.sort(key=lambda v: -v["realized_pnl"])

        total_open = sum(len(v["open_cids"]) for v in self.variants)
        total_closed = sum(v["wins"] + v["losses"] for v in self.variants)
        total_pnl = sum(v["realized_pnl"] for v in self.variants)

        print(f"\n━━━ LIVE VALIDATOR ({datetime.now():%H:%M}) ━━━")
        print(f"  {len(self.variants)} variants, {total_open} open, "
              f"{total_closed} closed, realized=${total_pnl:+.2f}")
        if eligible:
            print(f"  TOP 10 with ≥5 closed bets:")
            for v in eligible[:10]:
                tot = v["wins"] + v["losses"]
                wr = v["wins"] / max(tot, 1) * 100
                roi = v["realized_pnl"] / (tot * BET_USD) * 100
                print(f"    {v['variant'][:55]:<55} "
                      f"BT={v['roi_pct']:+5.1f}% LIVE={roi:+5.1f}% "
                      f"({v['wins']}/{v['losses']}, WR {wr:.0f}%)")

    def _prune_inactive_markets(self, max_age_sec=7200):
        """Remove markets from history_per_mkt that haven't seen ticks in 2h.
        Frees RAM on long-running validator with many transient markets.
        """
        now = time.time()
        to_drop = []
        for mid, hist in self.history_per_mkt.items():
            if not hist:
                to_drop.append(mid)
                continue
            last_ts = hist[-1][0]
            if now - last_ts > max_age_sec:
                to_drop.append(mid)
        for mid in to_drop:
            del self.history_per_mkt[mid]
        # Also drop CID cache for old markets
        if len(self.cid_for_mid) > 500:
            # Simple LRU-ish: drop oldest entries
            keys = list(self.cid_for_mid.keys())[:len(self.cid_for_mid) - 300]
            for k in keys:
                del self.cid_for_mid[k]
        return len(to_drop)

    async def run(self):
        print(f"LiveValidator starting, {len(self.variants)} top variants from backtest")
        for v in self.variants[:10]:
            print(f"  {v['variant']}: BT ROI={v.get('roi_pct',0)}% "
                  f"on {v.get('n_bets',0)} bets")

        # Skip backlog
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
                    # RAM hygiene: prune inactive markets + GC
                    pruned = self._prune_inactive_markets()
                    gc.collect()
                    if pruned:
                        print(f"[LV] pruned {pruned} inactive markets, "
                              f"history now {len(self.history_per_mkt)} markets")
            except Exception as e:
                print(f"Loop error: {e}")
            await asyncio.sleep(30)


async def main():
    bot = LiveValidator()
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
