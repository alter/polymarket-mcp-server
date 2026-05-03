#!/usr/bin/env python3
"""
Build entity→market index by extracting named entities from market questions.
Saves bot-data/market_entities.json with mapping:
   entity_key -> [list of cid's mentioning that entity]
   cid -> [list of entity keys for that market]

News bot can match incoming events to entities and trade ALL matched markets.

Run periodically (every 6h via watchdog or cron) to refresh as markets open/close.
"""
import json, re, time
import httpx
from collections import defaultdict
from pathlib import Path

GAMMA = "https://gamma-api.polymarket.com"
OUT = Path("bot-data/market_entities.json")
HEADERS = {"User-Agent": "Mozilla/5.0 entity-indexer"}


# ─── Entity vocabularies (curated, deterministic) ──────────────────────────
COUNTRIES = [
    "United States", "USA", "U.S.", "China", "Russia", "Ukraine", "India",
    "Israel", "Iran", "North Korea", "South Korea", "Japan", "Taiwan",
    "Germany", "France", "United Kingdom", "U.K.", "Brazil", "Mexico",
    "Canada", "Turkey", "Saudi Arabia", "Australia", "Italy", "Spain",
    "Poland", "Argentina", "Colombia", "Hungary", "Egypt", "Pakistan",
    "Indonesia", "Venezuela", "Cuba", "Syria", "Yemen", "Lebanon", "Qatar",
]

CRYPTOS = [
    "Bitcoin", "BTC", "Ethereum", "ETH", "Solana", "SOL", "XRP", "Ripple",
    "Dogecoin", "DOGE", "Cardano", "ADA", "Avalanche", "AVAX", "Polygon",
    "MATIC", "Chainlink", "LINK", "Polkadot", "DOT", "Litecoin", "LTC",
    "Shiba", "SHIB", "Toncoin", "TON", "Tron", "TRX", "Pepe", "PEPE",
    "Sui", "SUI", "Aptos", "APT", "Stellar", "XLM", "Hedera", "HBAR",
]

US_POLITICIANS = [
    "Trump", "Donald Trump", "Biden", "Joe Biden", "Harris", "Kamala Harris",
    "Vance", "JD Vance", "Pence", "Mike Pence", "DeSantis", "Ron DeSantis",
    "Newsom", "Gavin Newsom", "Pelosi", "Nancy Pelosi", "Schumer", "McConnell",
    "Johnson", "Mike Johnson", "Jeffries", "Hakeem Jeffries", "Sanders",
    "Bernie Sanders", "Warren", "Elizabeth Warren", "Manchin", "Sinema",
    "Cotton", "Tom Cotton", "Cruz", "Ted Cruz", "Hawley", "Josh Hawley",
    "RFK Jr", "Kennedy", "Powell", "Jerome Powell", "Yellen", "Janet Yellen",
    "Musk", "Elon Musk", "Ramaswamy", "Vivek Ramaswamy",
]

WORLD_LEADERS = [
    "Putin", "Vladimir Putin", "Xi", "Xi Jinping", "Kim", "Kim Jong Un",
    "Netanyahu", "Benjamin Netanyahu", "Zelensky", "Zelenskyy", "Volodymyr Zelensky",
    "Modi", "Narendra Modi", "Macron", "Emmanuel Macron", "Merz", "Scholz",
    "Starmer", "Keir Starmer", "Sunak", "Rishi Sunak", "Meloni", "Giorgia Meloni",
    "Erdogan", "Recep Erdogan", "Orban", "Viktor Orban", "MBS", "Mohammed bin Salman",
    "Lula", "Milei", "Javier Milei", "Trudeau", "Justin Trudeau",
]

ATHLETES = [
    # Soccer
    "Messi", "Lionel Messi", "Ronaldo", "Cristiano Ronaldo", "Mbappe", "Mbappé",
    "Haaland", "Erling Haaland", "Vinicius", "Bellingham", "Lewandowski",
    "Salah", "Mohamed Salah", "Foden", "De Bruyne",
    # NBA
    "LeBron", "LeBron James", "Curry", "Stephen Curry", "Jokic", "Nikola Jokic",
    "Doncic", "Luka Doncic", "Tatum", "Jayson Tatum", "Embiid", "Joel Embiid",
    "Giannis", "Antetokounmpo", "Durant", "Kevin Durant", "Wembanyama", "Wemby",
    # NFL
    "Mahomes", "Patrick Mahomes", "Allen", "Josh Allen", "Burrow", "Joe Burrow",
    "Lamar Jackson", "Brady", "Tom Brady",
    # Tennis
    "Djokovic", "Novak Djokovic", "Alcaraz", "Carlos Alcaraz", "Sinner",
    "Jannik Sinner", "Nadal", "Rafael Nadal", "Swiatek", "Iga Swiatek",
    # F1
    "Verstappen", "Max Verstappen", "Hamilton", "Lewis Hamilton", "Leclerc",
    "Charles Leclerc", "Norris", "Lando Norris", "Russell", "Piastri",
    # Boxing/UFC
    "Tyson Fury", "Usyk", "Oleksandr Usyk", "Joshua", "Anthony Joshua",
    "McGregor", "Conor McGregor", "Jon Jones",
]

CLUBS = [
    # Soccer
    "Real Madrid", "Barcelona", "Manchester City", "Liverpool", "Manchester United",
    "Arsenal", "Chelsea", "Tottenham", "PSG", "Paris Saint-Germain", "Bayern Munich",
    "Borussia Dortmund", "Inter Milan", "AC Milan", "Juventus", "Atlético Madrid",
    # NBA
    "Lakers", "Celtics", "Warriors", "Nuggets", "Heat", "Bucks", "76ers",
    "Knicks", "Mavericks", "Clippers", "Suns", "Timberwolves",
    # NFL
    "Chiefs", "Bills", "Cowboys", "Eagles", "49ers", "Ravens", "Bengals",
    "Patriots", "Lions", "Packers",
]

ORGS = [
    "Fed", "Federal Reserve", "FOMC", "SEC", "FBI", "DOJ", "CIA", "Pentagon",
    "Congress", "Senate", "Supreme Court", "SCOTUS", "NATO", "UN",
    "United Nations", "EU", "European Union", "WHO", "IMF", "World Bank",
    "OPEC", "BRICS", "G7", "G20", "EPA", "IRS", "FAA", "FDA",
    "OpenAI", "Google", "Apple", "Meta", "Microsoft", "Amazon", "Tesla",
    "Nvidia", "SpaceX", "Anthropic", "Coinbase", "Binance", "Kraken",
]

TECH_FIGURES = [
    "Musk", "Elon Musk", "Bezos", "Jeff Bezos", "Zuckerberg", "Mark Zuckerberg",
    "Altman", "Sam Altman", "Pichai", "Sundar Pichai", "Cook", "Tim Cook",
    "Nadella", "Satya Nadella", "Huang", "Jensen Huang",
]

CELEBRITIES = [
    "Taylor Swift", "Drake", "Kanye", "Beyonce", "Beyoncé", "Kim Kardashian",
    "Kendrick Lamar", "Travis Kelce", "Travis Scott", "Bad Bunny",
    "Kim Jong Un",
]


def compile_patterns():
    """Build a category-keyed dict of compiled regex patterns."""
    cats = {
        "country": COUNTRIES,
        "crypto": CRYPTOS,
        "us_pol": US_POLITICIANS,
        "world_leader": WORLD_LEADERS,
        "athlete": ATHLETES,
        "club": CLUBS,
        "org": ORGS,
        "tech": TECH_FIGURES,
        "celeb": CELEBRITIES,
    }
    out = {}
    for cat, names in cats.items():
        # Sort longest first so "Cristiano Ronaldo" matches before "Ronaldo"
        sorted_names = sorted(set(names), key=len, reverse=True)
        for n in sorted_names:
            # escape regex metachars but keep word boundaries
            esc = re.escape(n)
            out.setdefault(cat, []).append((n, re.compile(rf"\b{esc}\b", re.I)))
    return out


def fetch_active_markets():
    """Pull all active+open markets from Gamma."""
    markets = []
    with httpx.Client(timeout=20, follow_redirects=True, headers=HEADERS) as c:
        for offset in range(0, 2000, 100):
            try:
                r = c.get(f"{GAMMA}/markets", params={
                    "limit": 100, "offset": offset,
                    "active": "true", "closed": "false",
                })
                if r.status_code != 200:
                    break
                batch = r.json()
                if not batch:
                    break
                markets.extend(batch)
                time.sleep(0.15)
            except Exception as e:
                print(f"  fetch err offset={offset}: {e}")
                break
    return markets


def main():
    print("Building entity → market index...")
    markets = fetch_active_markets()
    print(f"  Fetched {len(markets)} active markets")
    if not markets:
        print("  No markets, aborting")
        return

    patterns = compile_patterns()
    n_pat = sum(len(v) for v in patterns.values())
    print(f"  Compiled {n_pat} entity patterns across {len(patterns)} categories")

    entity_to_markets = defaultdict(list)   # "country:Iran" -> [cid, ...]
    market_to_entities = defaultdict(list)  # cid -> ["country:Iran", ...]

    for m in markets:
        cid = m.get("conditionId") or m.get("id", "")
        if not cid:
            continue
        question = m.get("question", "") or ""
        category = m.get("category", "")
        # Search question text for each pattern
        matches = set()
        for cat, items in patterns.items():
            for name, pat in items:
                if pat.search(question):
                    key = f"{cat}:{name}"
                    matches.add(key)
        if matches:
            for key in matches:
                entity_to_markets[key].append({
                    "cid": cid,
                    "question": question[:150],
                    "category": category,
                    "volume24hr": float(m.get("volume24hr", 0) or 0),
                    "endDate": m.get("endDate", ""),
                })
            market_to_entities[cid] = sorted(matches)

    # Stats
    n_with = len(market_to_entities)
    n_total = len(markets)
    n_keys = len(entity_to_markets)
    avg_per_market = sum(len(v) for v in market_to_entities.values()) / max(n_with, 1)

    # Top entities by market count
    top_by_count = sorted(entity_to_markets.items(), key=lambda kv: -len(kv[1]))[:30]

    out = {
        "ran_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "n_markets_total": n_total,
        "n_markets_with_entities": n_with,
        "n_entity_keys": n_keys,
        "avg_entities_per_market": round(avg_per_market, 2),
        "entity_to_markets": {k: v for k, v in entity_to_markets.items()},
        "market_to_entities": dict(market_to_entities),
        "top_entities": [
            {"key": k, "n_markets": len(v),
             "sample_questions": [m["question"][:80] for m in v[:3]]}
            for k, v in top_by_count
        ],
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    json.dump(out, open(OUT, "w"), indent=1, ensure_ascii=False)
    print(f"  Saved {OUT} ({OUT.stat().st_size/1024:.0f} KB)")
    print(f"\n  Markets matched: {n_with}/{n_total} ({n_with/n_total*100:.0f}%)")
    print(f"  Avg entities/market: {avg_per_market:.1f}")
    print(f"\n  TOP 15 entity hubs:")
    for row in out["top_entities"][:15]:
        print(f"    {row['key']:<30} {row['n_markets']:>4} markets")


if __name__ == "__main__":
    main()
