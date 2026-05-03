#!/usr/bin/env python3
"""
News Monitor — polls public political news RSS feeds, extracts predictive
signals about Senate confirmations / political events.

Target signals (matching Strategy C from political research):
- Senate whip count: "X senators say they will vote NO/YES on confirmation Y"
- Confirmation pre-vote: "Z faces confirmation vote on date W"
- Geopolitical action: "Putin/Xi/etc. signals/announces..."
- Treaty/sanction announcements

Output: bot-data/news_signals.jsonl (append-only)
Each line: {ts, source, title, url, summary, keywords_matched, score}

Score 0-1 based on how predictive the signal is (specific dates/numbers/names).
Future: integrate with political_skeptic_bot or new news_trader_bot.
"""
import asyncio, gc, json, os, re, time
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import httpx

DATA = "data"
OUT_FILE = os.path.join(DATA, "news_signals.jsonl")
STATE_FILE = os.path.join(DATA, "news_monitor_state.json")

# Public RSS feeds (no auth required)
# Coverage matched to Polymarket topic distribution from
# bot-data/polymarket_topic_analysis.json (2026-05-03 sample of 1000 active markets):
#   us_politics $9M | crypto $8.5M | geopolitics $5M | soccer $4.6M | tech $4.1M
#   celebrity $3.8M | nba $3.1M | f1 $2.4M | baseball $1.6M | macro $1.5M
RSS_FEEDS = [
    # ── US politics (Polymarket #1, $9M/24h) ──
    ("politico_congress",    "https://rss.politico.com/congress.xml"),
    ("politico_white_house", "https://rss.politico.com/politics-news.xml"),
    ("politico_breaking",    "https://rss.politico.com/breaking-news.xml"),
    ("npr_politics",         "https://feeds.npr.org/1014/rss.xml"),
    ("ap_politics",          "https://rsshub.app/apnews/topics/politics"),
    # ── World/geopolitics ($5M) ──
    ("bbc_world",            "http://feeds.bbci.co.uk/news/world/rss.xml"),
    ("aljazeera_world",      "https://www.aljazeera.com/xml/rss/all.xml"),
    ("reuters_world",        "https://feeds.reuters.com/Reuters/worldNews"),
    # ── Crypto ($8.5M — was uncovered) ──
    ("coindesk",             "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("cointelegraph",        "https://cointelegraph.com/rss"),
    ("decrypt",              "https://decrypt.co/feed"),
    # ── Sports: soccer/football ($4.6M — was uncovered) ──
    ("bbc_sport_football",   "http://feeds.bbci.co.uk/sport/football/rss.xml"),
    ("espn_soccer",          "https://www.espn.com/espn/rss/soccer/news"),
    # ── NBA ($3.1M) ──
    ("espn_nba",             "https://www.espn.com/espn/rss/nba/news"),
    # ── F1 ($2.4M) ──
    ("bbc_sport_f1",         "http://feeds.bbci.co.uk/sport/formula1/rss.xml"),
    ("espn_f1",              "https://www.espn.com/espn/rss/f1/news"),
    # ── Tech/business ($4.1M) ──
    ("techcrunch",           "https://techcrunch.com/feed/"),
    ("ars_technica",         "https://feeds.arstechnica.com/arstechnica/index"),
    # ── Macro/finance ($1.5M — Fed/CPI/jobs) ──
    ("reuters_business",     "https://feeds.reuters.com/reuters/businessNews"),
    ("ft_markets",           "https://www.ft.com/markets?format=rss"),
]

# Predictive keyword patterns with score weights
# Broader patterns catch more political news; specific phrases score higher.
KEYWORD_PATTERNS = [
    # Senate confirmations (highest signal)
    ("vote_against",     re.compile(r'\bvote\s+against\b|\boppos\w+\s+(?:the\s+)?(?:nomination|confirmation)|\bno\s+vote\s+on', re.I), 0.9),
    ("vote_for",         re.compile(r'\bvote\s+(?:for|in\s+favor)\b|\bsupport\s+(?:the\s+)?(?:nomination|confirmation)', re.I), 0.85),
    ("filibuster",       re.compile(r'\bfilibuster\b', re.I), 0.8),
    ("whip_count",       re.compile(r'\bwhip\s+count\b|\b\d+\s+senators?\s+(?:have\s+)?(?:said|stated|announced)\b', re.I), 0.95),
    ("confirmation",     re.compile(r'\bconfirm\w*\b.*\b(?:vote|hearing|nominee|nomination)', re.I), 0.7),
    ("vote_general",     re.compile(r'\b(?:floor\s+vote|roll\s+call|cloture|cast\s+vote)\b', re.I), 0.6),
    ("senate_action",    re.compile(r'\bsenate\b.*\b(?:pass|advance|block|reject|kill|vote|confirm)', re.I), 0.55),
    ("house_vote",       re.compile(r'\bhouse\b.*\b(?:pass|advance|reject|vote)', re.I), 0.55),
    # Geopolitical signals
    ("ceasefire",        re.compile(r'\bceasefire\b', re.I), 0.7),
    ("sanction",         re.compile(r'\bsanctions?\b', re.I), 0.55),
    ("treaty_sign",      re.compile(r'\btreaty\b.*\b(?:sign|ratify)', re.I), 0.7),
    ("trump_action",     re.compile(r'\btrump\b.*\b(?:sign|veto|order|fire|nominate|appoint|remove)', re.I), 0.55),
    ("meet_summit",      re.compile(r'\b(?:trump|putin|xi|kim|netanyahu|biden|zelensky)\b.*\b(?:meet|summit|visit|call)', re.I), 0.5),
    ("election_call",    re.compile(r'\belection\s+(?:called|scheduled|set|won)\s+(?:for|by)?', re.I), 0.7),
    # Resignation/removal/impeachment
    ("resign",           re.compile(r'\b(?:resign|step\s+down|step\s+aside|removed)\b', re.I), 0.6),
    ("impeach",          re.compile(r'\bimpeach', re.I), 0.7),
    ("indictment",       re.compile(r'\bindict\w*\b|\bcharged\s+with\b', re.I), 0.65),
    # Specific named leaders
    ("trump",            re.compile(r'\btrump\b', re.I), 0.4),
    ("putin",            re.compile(r'\bputin\b', re.I), 0.5),
    ("netanyahu",        re.compile(r'\bnetanyahu\b', re.I), 0.5),
    ("xi_jinping",       re.compile(r'\bxi\s+jinping\b|\bchina.*president', re.I), 0.5),
    ("zelensky",         re.compile(r'\bzelensky\b', re.I), 0.5),
    # ── Crypto signals (predictive of price-target markets) ──
    ("crypto_etf",       re.compile(r'\b(?:etf|spot etf)\b.*\b(?:approve|approval|sec|filing|launch|deny)', re.I), 0.85),
    ("crypto_listing",   re.compile(r'\b(?:listing|listed|delist)\b.*\b(?:coinbase|binance|kraken)', re.I), 0.7),
    ("btc_price_call",   re.compile(r'\bbitcoin\b.*\b(?:hit|reach|target|crash|surge|rally)\b.*\$\d', re.I), 0.6),
    ("eth_upgrade",      re.compile(r'\b(?:ethereum|eth)\b.*\b(?:upgrade|merge|fork|hardfork)', re.I), 0.6),
    ("sec_action",       re.compile(r'\bsec\b.*\b(?:sue|lawsuit|charge|settle|approve|crypto)', re.I), 0.7),
    # ── Sports signals (transfers/injuries flip win-likelihood markets) ──
    ("transfer_signing", re.compile(r'\b(?:sign|signing|transfer|joins|moves to)\b.*\b(?:contract|deal|million|fc)\b', re.I), 0.6),
    ("injury_out",       re.compile(r'\b(?:injur\w+|out for|sidelined|ruled out|surgery|torn|fracture)\b', re.I), 0.7),
    ("coach_fired",      re.compile(r'\b(?:fire|sack|dismiss|step down|resign)\b.*\b(?:coach|manager|head)', re.I), 0.75),
    ("match_result",     re.compile(r'\b(?:beat|defeat|win|lost|draw|tie)\b.*\b(?:\d+[-:]\d+|final|championship)', re.I), 0.55),
    # ── Macro/finance ──
    ("fed_decision",     re.compile(r'\b(?:fed|fomc|powell)\b.*\b(?:rate|cut|hike|hold|pause|raise)', re.I), 0.85),
    ("cpi_release",      re.compile(r'\b(?:cpi|inflation)\b.*\b(?:report|release|data|reading|expect)', re.I), 0.7),
    ("jobless_claims",   re.compile(r'\b(?:jobless|unemployment|nonfarm|payroll|jobs)\b.*\b(?:report|claim|data)', re.I), 0.65),
    # ── Tech/business catalysts ──
    ("earnings_beat",    re.compile(r'\b(?:earnings|q[1-4])\b.*\b(?:beat|miss|surprise|guidance)\b', re.I), 0.6),
    ("ipo_announce",     re.compile(r'\bipo\b.*\b(?:announce|file|launch|valuation)', re.I), 0.7),
    ("ai_milestone",     re.compile(r'\b(?:gpt-?\d|openai|anthropic|claude|gemini)\b.*\b(?:release|launch|announce)', re.I), 0.6),
]


async def fetch_feed(client, name, url):
    try:
        r = await client.get(
            url, timeout=15.0, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; NewsMonitor/1.0)"},
        )
        if r.status_code != 200:
            return name, None
        return name, r.text
    except Exception as e:
        print(f"[news] fetch err {name}: {e}")
        return name, None


def parse_rss(name, xml_text):
    """Parse RSS XML, return list of {title, link, description, pubDate}."""
    items = []
    if not xml_text:
        return items
    try:
        # Strip namespace declarations AND prefixed tags (e.g. <dc:creator>, <media:content>)
        xml_text = re.sub(r'\bxmlns(:\w+)?="[^"]+"', '', xml_text)
        xml_text = re.sub(r'<(/?)\w+:', r'<\1', xml_text)  # strip ns prefix from tags
        root = ET.fromstring(xml_text)
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            desc = (item.findtext("description") or "").strip()
            pub = (item.findtext("pubDate") or "").strip()
            if title:
                items.append({"title": title, "link": link, "summary": desc[:500], "pubDate": pub})
        # Atom-style entries
        for entry in root.iter("entry"):
            title = (entry.findtext("title") or "").strip()
            summary = (entry.findtext("summary") or "").strip()
            link_el = entry.find("link")
            link = link_el.get("href") if link_el is not None else ""
            pub = (entry.findtext("published") or entry.findtext("updated") or "").strip()
            if title:
                items.append({"title": title, "link": link, "summary": summary[:500], "pubDate": pub})
    except Exception:
        pass
    return items


def score_item(item):
    """Returns list of (keyword, score) matches and total weighted score."""
    text = item["title"] + " " + item.get("summary", "")
    hits = []
    for kw, pat, w in KEYWORD_PATTERNS:
        if pat.search(text):
            hits.append((kw, w))
    if not hits:
        return [], 0.0
    # Aggregate score: max + 0.1*sum others (rewards multi-keyword)
    weights = [h[1] for h in hits]
    score = max(weights) + 0.1 * (sum(weights) - max(weights))
    return [h[0] for h in hits], min(score, 1.0)


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            return json.load(open(STATE_FILE))
        except Exception:
            pass
    return {"seen_titles": []}


def save_state(state):
    # Cap seen_titles to last 1000 to avoid unbounded growth
    state["seen_titles"] = state["seen_titles"][-1000:]
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


async def run_once(client, state):
    seen = set(state["seen_titles"])
    new_signals = 0
    total_items = 0
    tasks = [fetch_feed(client, name, url) for name, url in RSS_FEEDS]
    results = await asyncio.gather(*tasks)
    for name, xml in results:
        items = parse_rss(name, xml)
        total_items += len(items)
        for item in items:
            tkey = f"{name}|{item['title'][:100]}"
            if tkey in seen:
                continue
            seen.add(tkey)
            kws, score = score_item(item)
            if not kws:
                continue
            # Only signals with score >= 0.4
            if score < 0.4:
                continue
            entry = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "source": name,
                "title": item["title"][:200],
                "url": item.get("link", "")[:300],
                "summary": item.get("summary", "")[:300],
                "pubDate": item.get("pubDate", ""),
                "keywords": kws,
                "score": round(score, 2),
            }
            with open(OUT_FILE, "a") as f:
                f.write(json.dumps(entry, separators=(",", ":")) + "\n")
            new_signals += 1
    state["seen_titles"] = list(seen)
    return total_items, new_signals


async def main():
    os.makedirs(DATA, exist_ok=True)
    state = load_state()
    print(f"[news] starting, {len(RSS_FEEDS)} feeds, {len(KEYWORD_PATTERNS)} patterns")
    async with httpx.AsyncClient(timeout=20.0) as client:
        while True:
            try:
                total, new_sigs = await run_once(client, state)
                save_state(state)
                print(f"[news] {datetime.now():%H:%M} feeds={len(RSS_FEEDS)} "
                      f"items={total} new_signals={new_sigs} seen_total={len(state['seen_titles'])}")
            except Exception as e:
                print(f"[news] err: {e}")
            gc.collect()  # release per-cycle XML/parse data
            await asyncio.sleep(900)  # 15 min


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
