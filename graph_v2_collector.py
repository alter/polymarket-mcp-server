#!/usr/bin/env python3
"""
Graph V2: Extended data collector.
Collects markets, trades, news (RSS + GNews), and builds hourly price snapshots.
Saves everything to bot-data/graph_v2/ in compact formats.

Usage: python3 graph_v2_collector.py
"""
import json, os, re, time, hashlib, struct
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET

import httpx
import numpy as np

GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com"
GNEWS_KEY = os.environ.get("GNEWS_API_KEY", "")  # optional
OUT = Path("bot-data/graph_v2")
OUT.mkdir(parents=True, exist_ok=True)

client = httpx.Client(timeout=20, follow_redirects=True)

# ─── 1. MARKETS ─────────────────────────────────────────────────────────────

def collect_markets(n=2000):
    """Fetch active + recently closed markets."""
    markets = []
    for offset in range(0, n, 100):
        try:
            r = client.get(f"{GAMMA}/markets", params={
                "limit": 100, "offset": offset, "order": "volume24hr",
                "ascending": "false", "active": "true"})
            r.raise_for_status()
            batch = r.json()
            if not batch: break
            markets.extend(batch)
        except Exception as e:
            print(f"  [warn] markets offset={offset}: {e}")
        time.sleep(0.15)
    print(f"  Markets fetched: {len(markets)}")
    return markets


# ─── 2. TRADES (top N markets) ──────────────────────────────────────────────

def collect_trades(markets, n_markets=300, per_market=500):
    """Fetch trades for top markets by volume."""
    sorted_m = sorted(markets, key=lambda m: float(m.get("volume24hr", 0) or 0), reverse=True)
    top = sorted_m[:n_markets]
    trade_data = {}
    for i, mkt in enumerate(top):
        cid = mkt.get("conditionId") or ""
        if not cid: continue
        try:
            r = client.get(f"{DATA}/trades", params={"market": cid, "limit": per_market})
            r.raise_for_status()
            trades = r.json()
            if trades:
                trade_data[cid] = trades
        except:
            pass
        time.sleep(0.12)
        if (i+1) % 50 == 0:
            print(f"    trades: {i+1}/{len(top)}, {len(trade_data)} with data")
    print(f"  Trades: {len(trade_data)} markets, {sum(len(v) for v in trade_data.values())} total")
    return trade_data


# ─── 3. HOURLY PRICE SNAPSHOTS ──────────────────────────────────────────────

def build_price_snapshots(trade_data):
    """Build hourly VWAP series per market from trades."""
    snapshots = {}
    for cid, trades in trade_data.items():
        buckets = defaultdict(lambda: {"pv": 0, "vol": 0, "cnt": 0})
        for t in trades:
            ts = int(t.get("timestamp") or 0)
            price = float(t.get("price", 0))
            size = float(t.get("size", 0))
            if ts > 0 and price > 0 and size > 0:
                h = (ts // 3600) * 3600
                b = buckets[h]
                b["pv"] += price * size
                b["vol"] += size
                b["cnt"] += 1
        if buckets:
            series = []
            for h in sorted(buckets):
                b = buckets[h]
                series.append([h, round(b["pv"]/b["vol"], 6) if b["vol"] > 0 else 0,
                              round(b["vol"], 2), b["cnt"]])
            snapshots[cid] = series  # [[ts, vwap, vol, cnt], ...]
    print(f"  Price snapshots: {len(snapshots)} markets, "
          f"{sum(len(v) for v in snapshots.values())} hourly points")
    return snapshots


# ─── 4. NEWS (RSS feeds) ────────────────────────────────────────────────────

RSS_FEEDS = [
    ("reuters_world", "https://feeds.reuters.com/Reuters/worldNews"),
    ("reuters_biz", "https://feeds.reuters.com/Reuters/businessNews"),
    ("reuters_tech", "https://feeds.reuters.com/Reuters/technologyNews"),
    ("bbc_world", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("bbc_biz", "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("cnbc_world", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362"),
    ("crypto_coindesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("crypto_cointelegraph", "https://cointelegraph.com/rss"),
    ("sports_espn", "https://www.espn.com/espn/rss/news"),
    ("politics_hill", "https://thehill.com/feed/"),
    ("ap_news", "https://rsshub.app/apnews/topics/world-news"),
    ("nyt_world", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
]


def collect_rss_news():
    """Collect news from RSS feeds."""
    articles = []
    for name, url in RSS_FEEDS:
        try:
            r = client.get(url, timeout=10)
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.content)
            # RSS 2.0 format
            for item in root.iter("item"):
                title = (item.findtext("title") or "").strip()
                desc = (item.findtext("description") or "").strip()
                link = (item.findtext("link") or "").strip()
                pub = (item.findtext("pubDate") or "").strip()
                if title:
                    # Clean HTML from description
                    desc = re.sub(r"<[^>]+>", "", desc)[:500]
                    articles.append({
                        "source": name, "title": title, "desc": desc[:300],
                        "link": link, "pub": pub,
                        "id": hashlib.md5((title + link).encode()).hexdigest()[:12],
                    })
            # Atom format
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
                title = (entry.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
                summary = (entry.findtext("{http://www.w3.org/2005/Atom}summary") or "").strip()
                link_el = entry.find("{http://www.w3.org/2005/Atom}link")
                link = link_el.get("href", "") if link_el is not None else ""
                pub = (entry.findtext("{http://www.w3.org/2005/Atom}published") or "").strip()
                if title:
                    summary = re.sub(r"<[^>]+>", "", summary)[:500]
                    articles.append({
                        "source": name, "title": title, "desc": summary[:300],
                        "link": link, "pub": pub,
                        "id": hashlib.md5((title + link).encode()).hexdigest()[:12],
                    })
        except Exception as e:
            print(f"    RSS {name}: {e}")
        time.sleep(0.3)

    # Deduplicate by id
    seen = set()
    unique = []
    for a in articles:
        if a["id"] not in seen:
            seen.add(a["id"])
            unique.append(a)
    print(f"  RSS news: {len(unique)} articles from {len(RSS_FEEDS)} feeds")
    return unique


def collect_gnews(queries, per_query=10):
    """Collect news from GNews API (free: 100 req/day)."""
    if not GNEWS_KEY:
        print("  GNews: no API key, skipping")
        return []
    articles = []
    for q in queries[:20]:  # max 20 queries to stay within limits
        try:
            r = client.get("https://gnews.io/api/v4/search", params={
                "q": q, "lang": "en", "max": per_query, "apikey": GNEWS_KEY})
            if r.status_code == 200:
                data = r.json()
                for a in data.get("articles", []):
                    articles.append({
                        "source": "gnews",
                        "title": a.get("title", ""),
                        "desc": a.get("description", "")[:300],
                        "link": a.get("url", ""),
                        "pub": a.get("publishedAt", ""),
                        "id": hashlib.md5(a.get("title", "").encode()).hexdigest()[:12],
                    })
        except:
            pass
        time.sleep(1)
    print(f"  GNews: {len(articles)} articles for {len(queries)} queries")
    return articles


# ─── 5. ENTITY EXTRACTION ───────────────────────────────────────────────────

PEOPLE_RE = re.compile(r"\b[A-Z][a-z]{2,} [A-Z][a-z]{2,}\b")
FALSE_NAMES = {"Will", "How", "What", "When", "Which", "Does", "Can", "The",
               "New", "Super", "World", "South", "North", "West", "East",
               "United", "Saudi", "Los", "San", "Las", "Real", "Club"}
COUNTRIES = {"United States", "US", "China", "Russia", "Ukraine", "India", "Israel",
             "Iran", "North Korea", "South Korea", "Japan", "Taiwan", "Germany",
             "France", "UK", "Brazil", "Mexico", "Canada", "Turkey", "Saudi Arabia"}
ORGS = {"Fed", "Federal Reserve", "SEC", "NATO", "UN", "EU", "WHO", "IMF",
        "OPEC", "CIA", "FBI", "DOJ", "NASA", "SpaceX", "OpenAI", "Google",
        "Apple", "Meta", "Microsoft", "Amazon", "Tesla", "Nvidia"}
CRYPTO = {"Bitcoin", "BTC", "Ethereum", "ETH", "Solana", "SOL", "XRP",
          "Dogecoin", "DOGE", "Cardano", "ADA", "Toncoin", "TON"}


def extract_entities(text):
    ents = {"person": set(), "country": set(), "org": set(), "crypto": set()}
    if not text: return ents
    for m in PEOPLE_RE.finditer(text):
        name = m.group()
        if name.split()[0] not in FALSE_NAMES:
            ents["person"].add(name)
    for c in COUNTRIES:
        if c in text:
            ents["country"].add(c)
    for o in ORGS:
        if re.search(r"\b" + re.escape(o) + r"\b", text):
            ents["org"].add(o)
    for t in CRYPTO:
        if re.search(r"\b" + re.escape(t) + r"\b", text, re.IGNORECASE):
            ents["crypto"].add(t.upper() if len(t) <= 4 else t)
    return ents


# ─── 6. NEWS-MARKET MATCHING ────────────────────────────────────────────────

def match_news_to_markets(articles, markets):
    """Match news articles to markets by shared entities and keywords."""
    # Build market entity index
    market_entities = {}
    market_keywords = {}
    for m in markets:
        cid = m.get("conditionId") or ""
        q = m.get("question", "")
        if not cid or not q: continue
        ents = extract_entities(q)
        market_entities[cid] = ents
        # Simple keyword extraction: significant words from question
        words = set(re.findall(r'\b[A-Za-z]{4,}\b', q.lower()))
        stop = {"will", "does", "what", "when", "which", "that", "this", "have",
                "been", "from", "they", "with", "would", "could", "should", "about",
                "more", "than", "after", "before", "into", "over", "under"}
        market_keywords[cid] = words - stop

    # Match each article to markets
    matches = []
    for art in articles:
        text = (art.get("title", "") + " " + art.get("desc", "")).strip()
        art_ents = extract_entities(text)
        art_words = set(re.findall(r'\b[A-Za-z]{4,}\b', text.lower()))

        for cid, m_ents in market_entities.items():
            score = 0
            shared = []
            for etype in ("person", "country", "org", "crypto"):
                overlap = m_ents[etype] & art_ents[etype]
                if overlap:
                    score += len(overlap) * 3
                    shared.extend(overlap)
            # Keyword overlap
            kw_overlap = market_keywords.get(cid, set()) & art_words
            score += len(kw_overlap)

            if score >= 3:
                matches.append({
                    "article_id": art["id"],
                    "market_cid": cid,
                    "score": score,
                    "shared_entities": list(shared)[:5],
                    "keyword_overlap": len(kw_overlap),
                })

    # Sort and keep top matches
    matches.sort(key=lambda x: -x["score"])
    print(f"  News-market matches: {len(matches)} (top score: {matches[0]['score'] if matches else 0})")
    return matches


# ─── 7. TRADER ANALYSIS ─────────────────────────────────────────────────────

def analyze_traders(trade_data):
    """Build trader profiles: which markets, volume, direction."""
    trader_activity = defaultdict(lambda: defaultdict(lambda: {"buy_vol": 0, "sell_vol": 0, "count": 0}))
    for cid, trades in trade_data.items():
        for t in trades:
            wallet = t.get("proxyWallet") or ""
            if not wallet: continue
            side = (t.get("side") or "").upper()
            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            value = size * price
            act = trader_activity[wallet][cid]
            if side == "BUY":
                act["buy_vol"] += value
            else:
                act["sell_vol"] += value
            act["count"] += 1

    # Only keep traders active in 2+ markets
    active_traders = {w: mkts for w, mkts in trader_activity.items() if len(mkts) >= 2}
    total_trades = sum(sum(v["count"] for v in mkts.values()) for mkts in active_traders.values())
    print(f"  Active traders (2+ markets): {len(active_traders)}, {total_trades} trades")
    return active_traders


# ─── 8. SAVE COMPACT ────────────────────────────────────────────────────────

def save_data(markets, trade_data, snapshots, articles, news_matches, traders):
    """Save all data in compact formats."""

    # Markets: only essential fields
    compact_markets = []
    for m in markets:
        cid = m.get("conditionId") or ""
        if not cid: continue
        op = m.get("outcomePrices", "")
        if isinstance(op, str):
            try: op = json.loads(op)
            except: op = []
        yes_p = float(op[0]) if op else 0
        compact_markets.append({
            "c": cid, "q": m.get("question", "")[:200],
            "s": m.get("slug", ""), "y": round(yes_p, 4),
            "v": round(float(m.get("volume", 0) or 0)),
            "v24": round(float(m.get("volume24hr", 0) or 0)),
            "sd": m.get("startDate", ""), "ed": m.get("endDate", ""),
            "g": m.get("groupItemTitle", ""),
        })

    # Save
    with open(OUT / "markets.json", "w") as f:
        json.dump(compact_markets, f, separators=(",", ":"))

    # Price snapshots: compact binary-like JSON
    with open(OUT / "snapshots.json", "w") as f:
        json.dump(snapshots, f, separators=(",", ":"))

    # News articles
    with open(OUT / "news.json", "w") as f:
        json.dump(articles, f, separators=(",", ":"))

    # News-market matches
    with open(OUT / "news_matches.json", "w") as f:
        json.dump(news_matches[:10000], f, separators=(",", ":"))

    # Trader activity (compact: wallet[:16] → {cid: {b,s,n}})
    compact_traders = {}
    for wallet, mkts in traders.items():
        w = wallet[:16]
        compact_traders[w] = {
            cid: {"b": round(v["buy_vol"]), "s": round(v["sell_vol"]), "n": v["count"]}
            for cid, v in mkts.items()
        }
    with open(OUT / "traders.json", "w") as f:
        json.dump(compact_traders, f, separators=(",", ":"))

    # Summary
    sizes = {}
    for f_name in ["markets.json", "snapshots.json", "news.json", "news_matches.json", "traders.json"]:
        p = OUT / f_name
        sizes[f_name] = p.stat().st_size / 1024
    total_kb = sum(sizes.values())
    print(f"\n  Saved to {OUT}/:")
    for f_name, kb in sizes.items():
        print(f"    {f_name}: {kb:.0f} KB")
    print(f"    TOTAL: {total_kb:.0f} KB ({total_kb/1024:.1f} MB)")


# ─── MAIN ───────────────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    print("=== Graph V2 Data Collector ===\n")

    print("[1/7] Collecting markets...")
    markets = collect_markets(2000)

    print("[2/7] Collecting trades (top 300 markets)...")
    trade_data = collect_trades(markets, n_markets=300)

    print("[3/7] Building price snapshots...")
    snapshots = build_price_snapshots(trade_data)

    print("[4/7] Collecting RSS news...")
    rss_articles = collect_rss_news()

    # Extract top topics from high-volume markets for GNews queries
    top_questions = [m.get("question", "")[:60] for m in
                     sorted(markets, key=lambda m: float(m.get("volume24hr", 0) or 0), reverse=True)[:20]]
    gnews_queries = []
    for q in top_questions:
        # Extract key phrase
        q_clean = re.sub(r"^(Will |Does |Is |Has |Can )", "", q)
        q_clean = re.sub(r"\?$", "", q_clean).strip()
        if len(q_clean) > 10:
            gnews_queries.append(q_clean[:50])

    print("[5/7] Collecting GNews articles...")
    gnews_articles = collect_gnews(gnews_queries)
    all_articles = rss_articles + gnews_articles

    print("[6/7] Matching news to markets...")
    news_matches = match_news_to_markets(all_articles, markets)

    print("[7/7] Analyzing traders...")
    traders = analyze_traders(trade_data)

    print("\n[SAVE] Writing data...")
    save_data(markets, trade_data, snapshots, all_articles, news_matches, traders)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"  Markets: {len(markets)}")
    print(f"  Trades: {len(trade_data)} markets")
    print(f"  Snapshots: {len(snapshots)} markets, {sum(len(v) for v in snapshots.values())} points")
    print(f"  News: {len(all_articles)} articles")
    print(f"  News-market matches: {len(news_matches)}")
    print(f"  Active traders: {len(traders)}")


if __name__ == "__main__":
    main()
