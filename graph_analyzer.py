#!/usr/bin/env python3
"""
Phase 1: Graph-based dependency analyzer for Polymarket prediction markets.
Collects data, builds a networkx graph, detects communities, and finds non-obvious patterns.

Usage: python3 graph_analyzer.py
"""

import json
import math
import os
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import httpx
import networkx as nx
import numpy as np

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
OUT_DIR = Path("bot-data")
RATE_LIMIT_SLEEP = 0.2

COUNTRIES = [
    "United States", "China", "Russia", "Ukraine", "India", "Israel", "Iran",
    "North Korea", "South Korea", "Japan", "Taiwan", "Germany", "France",
    "United Kingdom", "Brazil", "Mexico", "Canada", "Turkey", "Saudi Arabia",
    "Australia", "Italy", "Spain", "Poland", "Argentina", "Colombia",
    "South Africa", "Nigeria", "Egypt", "Pakistan", "Indonesia",
]

CRYPTO_TOKENS = [
    "Bitcoin", "BTC", "Ethereum", "ETH", "Solana", "SOL", "XRP", "Ripple",
    "Dogecoin", "DOGE", "Cardano", "ADA", "Avalanche", "AVAX", "Polygon",
    "MATIC", "Chainlink", "LINK", "Polkadot", "DOT", "Litecoin", "LTC",
    "Shiba", "SHIB", "Toncoin", "TON", "Tron", "TRX", "Pepe", "PEPE",
]

ORGANIZATIONS = [
    "Fed", "Federal Reserve", "SEC", "NATO", "UN", "United Nations", "EU",
    "European Union", "WHO", "IMF", "World Bank", "OPEC", "CIA", "FBI",
    "DOJ", "EPA", "NASA", "SpaceX", "OpenAI", "Google", "Apple", "Meta",
    "Microsoft", "Amazon", "Tesla", "Nvidia", "Congress", "Supreme Court",
    "Senate", "Pentagon",
]

PERSON_RE = re.compile(r"\b[A-Z][a-z]{2,} [A-Z][a-z]{2,}\b")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _client() -> httpx.Client:
    return httpx.Client(timeout=20, follow_redirects=True)


def fetch_markets(client: httpx.Client, total: int = 500) -> list[dict]:
    """Paginate Gamma API to fetch active markets sorted by 24h volume."""
    markets = []
    for offset in range(0, total, 100):
        time.sleep(RATE_LIMIT_SLEEP)
        try:
            r = client.get(
                f"{GAMMA_API}/markets",
                params={
                    "limit": 100,
                    "offset": offset,
                    "order": "volume24hr",
                    "ascending": "false",
                    "active": "true",
                    "closed": "false",
                },
            )
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            markets.extend(batch if isinstance(batch, list) else batch.get("data", []))
        except Exception as e:
            print(f"  [warn] markets offset={offset}: {e}")
    print(f"  Fetched {len(markets)} markets")
    return markets


def fetch_events(client: httpx.Client) -> list[dict]:
    """Fetch active events from Gamma API."""
    events = []
    for offset in range(0, 500, 100):
        time.sleep(RATE_LIMIT_SLEEP)
        try:
            r = client.get(
                f"{GAMMA_API}/events",
                params={"limit": 100, "offset": offset, "active": "true"},
            )
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            events.extend(batch if isinstance(batch, list) else batch.get("data", []))
        except Exception as e:
            print(f"  [warn] events offset={offset}: {e}")
            break
    print(f"  Fetched {len(events)} events")
    return events


def fetch_trades(client: httpx.Client, condition_id: str, limit: int = 200) -> list[dict]:
    """Fetch recent trades for a market from the data API."""
    time.sleep(RATE_LIMIT_SLEEP)
    try:
        r = client.get(
            f"{DATA_API}/trades",
            params={"market": condition_id, "limit": limit},
        )
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else data.get("data", [])
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Entity extraction
# ---------------------------------------------------------------------------
def extract_entities(text: str) -> dict[str, set[str]]:
    """Extract named entities from question text using regex."""
    entities: dict[str, set[str]] = {"person": set(), "country": set(), "crypto": set(), "org": set()}
    if not text:
        return entities
    for m in PERSON_RE.finditer(text):
        name = m.group()
        # skip common false positives
        if name.split()[0] not in ("Will", "How", "What", "When", "Which", "Does", "Can", "The", "New", "Super", "World"):
            entities["person"].add(name)
    for c in COUNTRIES:
        if c in text:
            entities["country"].add(c)
    for tok in CRYPTO_TOKENS:
        if re.search(r"\b" + re.escape(tok) + r"\b", text, re.IGNORECASE):
            entities["crypto"].add(tok.upper() if len(tok) <= 5 else tok)
    for org in ORGANIZATIONS:
        if re.search(r"\b" + re.escape(org) + r"\b", text):
            entities["org"].add(org)
    return entities


# ---------------------------------------------------------------------------
# Price analysis helpers
# ---------------------------------------------------------------------------
def price_trajectory(trades: list[dict]) -> float | None:
    """Compute linear slope of prices over time. Returns None if insufficient data."""
    if len(trades) < 5:
        return None
    pts = []
    for t in trades:
        try:
            ts = float(t.get("timestamp") or t.get("createdAt") or t.get("time", 0))
            price = float(t.get("price", 0))
            if ts > 0 and 0 < price <= 1:
                pts.append((ts, price))
        except (ValueError, TypeError):
            continue
    if len(pts) < 5:
        return None
    pts.sort(key=lambda p: p[0])
    xs = np.array([p[0] for p in pts])
    ys = np.array([p[1] for p in pts])
    xs = xs - xs[0]  # normalize
    if xs[-1] == 0:
        return None
    xs = xs / xs[-1]
    try:
        slope = float(np.polyfit(xs, ys, 1)[0])
    except Exception:
        slope = None
    return slope


def pearson(a: list[float], b: list[float]) -> float | None:
    """Pearson correlation between two price series (resampled to same length)."""
    min_len = min(len(a), len(b))
    if min_len < 5:
        return None
    # resample both to min_len
    a2 = np.interp(np.linspace(0, 1, min_len), np.linspace(0, 1, len(a)), a)
    b2 = np.interp(np.linspace(0, 1, min_len), np.linspace(0, 1, len(b)), b)
    std_a, std_b = np.std(a2), np.std(b2)
    if std_a < 1e-9 or std_b < 1e-9:
        return None
    corr = float(np.corrcoef(a2, b2)[0, 1])
    return corr if not math.isnan(corr) else None


def extract_price_series(trades: list[dict]) -> list[float]:
    """Extract sorted price series from trades."""
    pts = []
    for t in trades:
        try:
            ts = float(t.get("timestamp") or t.get("createdAt") or t.get("time", 0))
            price = float(t.get("price", 0))
            if ts > 0 and 0 < price <= 1:
                pts.append((ts, price))
        except (ValueError, TypeError):
            continue
    pts.sort(key=lambda p: p[0])
    return [p[1] for p in pts]


def parse_yes_price(market: dict) -> float | None:
    """Parse the YES price from outcomePrices field."""
    raw = market.get("outcomePrices")
    if not raw:
        return None
    try:
        if isinstance(raw, str):
            prices = json.loads(raw)
        else:
            prices = raw
        if isinstance(prices, list) and len(prices) > 0:
            return float(prices[0])
    except Exception:
        pass
    return None


def parse_date(val) -> datetime | None:
    if not val:
        return None
    try:
        if isinstance(val, str):
            return datetime.fromisoformat(val.replace("Z", "+00:00")).replace(tzinfo=None)
        return datetime.fromtimestamp(int(val))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Graph building
# ---------------------------------------------------------------------------
def build_graph(
    markets: list[dict],
    events: list[dict],
    trade_data: dict[str, list[dict]],
) -> nx.Graph:
    G = nx.Graph()

    # --- Market nodes ---
    cid_to_idx: dict[str, str] = {}
    for m in markets:
        cid = m.get("conditionId") or m.get("condition_id") or m.get("id", "")
        if not cid:
            continue
        node_id = f"m:{cid}"
        cid_to_idx[cid] = node_id
        yes_price = parse_yes_price(m)
        G.add_node(
            node_id,
            type="market",
            question=m.get("question", "")[:200],
            volume=float(m.get("volume", 0) or 0),
            volume24hr=float(m.get("volume24hr", 0) or 0),
            yes_price=yes_price if yes_price is not None else -1.0,
            category=m.get("category", m.get("tag", "")),
            slug=m.get("slug", ""),
            startDate=m.get("startDate", ""),
            endDate=m.get("endDate", m.get("end_date_iso", "")),
        )

    # --- Event nodes + belongs_to_event edges ---
    event_slug_map: dict[str, str] = {}
    for ev in events:
        slug = ev.get("slug", "")
        if not slug:
            continue
        eid = f"e:{slug}"
        event_slug_map[slug] = eid
        ev_markets = ev.get("markets", [])
        G.add_node(
            eid,
            type="event",
            title=ev.get("title", "")[:200],
            slug=slug,
            num_markets=len(ev_markets) if isinstance(ev_markets, list) else 0,
        )
        # Link markets to event
        if isinstance(ev_markets, list):
            for em in ev_markets:
                em_cid = em.get("conditionId") or em.get("condition_id") or ""
                if em_cid and em_cid in cid_to_idx:
                    G.add_edge(cid_to_idx[em_cid], eid, type="belongs_to_event")

    # Also link via eventSlug on the market data
    for m in markets:
        cid = m.get("conditionId") or m.get("condition_id") or m.get("id", "")
        es = m.get("eventSlug", "")
        if cid in cid_to_idx and es and es in event_slug_map:
            mid = cid_to_idx[cid]
            eid = event_slug_map[es]
            if not G.has_edge(mid, eid):
                G.add_edge(mid, eid, type="belongs_to_event")

    # --- Category nodes + edges ---
    cat_markets: dict[str, list[str]] = defaultdict(list)
    for m in markets:
        cid = m.get("conditionId") or m.get("condition_id") or m.get("id", "")
        cat = m.get("category", m.get("tag", ""))
        if not cat or cid not in cid_to_idx:
            continue
        cat_node = f"cat:{cat}"
        if cat_node not in G:
            G.add_node(cat_node, type="category", name=cat)
        cat_markets[cat].append(cid_to_idx[cid])

    # same_category edges (limit to avoid n^2 blowup in large categories)
    for cat, mids in cat_markets.items():
        for i in range(len(mids)):
            for j in range(i + 1, min(i + 20, len(mids))):
                G.add_edge(mids[i], mids[j], type="same_category", category=cat)

    # --- Entity extraction + entity nodes + shared_entity edges ---
    market_entities: dict[str, dict[str, set[str]]] = {}
    entity_to_markets: dict[str, list[str]] = defaultdict(list)

    for m in markets:
        cid = m.get("conditionId") or m.get("condition_id") or m.get("id", "")
        if cid not in cid_to_idx:
            continue
        mid = cid_to_idx[cid]
        ents = extract_entities(m.get("question", ""))
        market_entities[mid] = ents
        for etype, names in ents.items():
            for name in names:
                key = f"ent:{etype}:{name}"
                entity_to_markets[key].append(mid)

    for key, mids in entity_to_markets.items():
        _, etype, ename = key.split(":", 2)
        if key not in G:
            G.add_node(key, type="entity", entity_type=etype, name=ename)
        # shared_entity edges between markets
        for i in range(len(mids)):
            for j in range(i + 1, min(i + 30, len(mids))):
                if not G.has_edge(mids[i], mids[j]):
                    G.add_edge(mids[i], mids[j], type="shared_entity", entity=ename)

    # --- Trader nodes + trader_active edges ---
    trader_volume: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for cid, trades in trade_data.items():
        if cid not in cid_to_idx:
            continue
        mid = cid_to_idx[cid]
        for t in trades:
            wallet = t.get("proxyWallet") or t.get("maker") or t.get("taker") or ""
            if not wallet:
                continue
            size = float(t.get("size", 0) or t.get("amount", 0) or 0)
            trader_volume[wallet][mid] += size

    # Keep only traders active in 2+ markets or with significant volume
    for wallet, market_vols in trader_volume.items():
        if len(market_vols) < 2:
            continue
        tid = f"t:{wallet[:16]}"
        if tid not in G:
            G.add_node(tid, type="trader", wallet=wallet[:16], num_markets=len(market_vols))
        for mid, vol in market_vols.items():
            G.add_edge(tid, mid, type="trader_active", volume=vol)

    # --- Temporal overlap edges ---
    market_times: list[tuple[str, datetime, datetime]] = []
    for m in markets:
        cid = m.get("conditionId") or m.get("condition_id") or m.get("id", "")
        if cid not in cid_to_idx:
            continue
        sd = parse_date(m.get("startDate"))
        ed = parse_date(m.get("endDate", m.get("end_date_iso")))
        if sd and ed and ed > sd:
            market_times.append((cid_to_idx[cid], sd, ed))

    # Only check temporal overlap among top 200 by volume to keep it tractable
    market_times.sort(key=lambda x: G.nodes[x[0]].get("volume", 0), reverse=True)
    top_times = market_times[:200]
    for i in range(len(top_times)):
        for j in range(i + 1, len(top_times)):
            mid_a, sa, ea = top_times[i]
            mid_b, sb, eb = top_times[j]
            overlap = min(ea, eb) > max(sa, sb)
            if overlap and not G.has_edge(mid_a, mid_b):
                G.add_edge(mid_a, mid_b, type="temporal_overlap")

    # --- Price correlation edges ---
    price_series: dict[str, list[float]] = {}
    for cid, trades in trade_data.items():
        if cid not in cid_to_idx:
            continue
        series = extract_price_series(trades)
        if len(series) >= 10:
            price_series[cid_to_idx[cid]] = series

    series_keys = list(price_series.keys())
    for i in range(len(series_keys)):
        for j in range(i + 1, len(series_keys)):
            corr = pearson(price_series[series_keys[i]], price_series[series_keys[j]])
            if corr is not None and (corr > 0.5 or corr < -0.5):
                G.add_edge(
                    series_keys[i],
                    series_keys[j],
                    type="correlated_price",
                    correlation=round(corr, 3),
                )

    return G


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
def analyze_graph(G: nx.Graph) -> dict:
    results: dict = {}
    market_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "market"]
    if not market_nodes:
        print("  No market nodes found, skipping analysis.")
        return results

    # Subgraph of markets only for community detection
    market_subgraph = G.subgraph(market_nodes).copy()

    # --- Community detection ---
    try:
        communities = nx.community.louvain_communities(market_subgraph, seed=42)
        comm_list = []
        for idx, comm in enumerate(sorted(communities, key=len, reverse=True)[:30]):
            prices = [G.nodes[n].get("yes_price", -1) for n in comm if G.nodes[n].get("yes_price", -1) >= 0]
            volumes = [G.nodes[n].get("volume", 0) for n in comm]
            cats = [G.nodes[n].get("category", "") for n in comm if G.nodes[n].get("category")]
            top_cat = max(set(cats), key=cats.count) if cats else ""
            comm_list.append({
                "id": idx,
                "size": len(comm),
                "avg_price": round(np.mean(prices), 3) if prices else None,
                "total_volume": round(sum(volumes), 2),
                "dominant_category": top_cat,
                "sample_markets": [G.nodes[n].get("question", "")[:120] for n in list(comm)[:5]],
            })
        results["communities"] = comm_list
        print(f"  Found {len(communities)} communities")
    except Exception as e:
        print(f"  Community detection failed: {e}")
        results["communities"] = []
        communities = []

    # --- Centrality ---
    try:
        bc = nx.betweenness_centrality(market_subgraph)
        dc = nx.degree_centrality(market_subgraph)
        top_bc = sorted(bc.items(), key=lambda x: x[1], reverse=True)[:20]
        results["central_markets"] = [
            {
                "node": n,
                "question": G.nodes[n].get("question", "")[:150],
                "betweenness": round(score, 5),
                "degree": round(dc.get(n, 0), 5),
                "volume": G.nodes[n].get("volume", 0),
            }
            for n, score in top_bc
        ]
    except Exception as e:
        print(f"  Centrality failed: {e}")
        results["central_markets"] = []

    # --- Cross-category bridges ---
    bridges = []
    try:
        for n in market_nodes:
            neighbors = list(G.neighbors(n))
            neighbor_cats = set()
            for nb in neighbors:
                if G.nodes[nb].get("type") == "market":
                    cat = G.nodes[nb].get("category", "")
                    if cat:
                        neighbor_cats.add(cat)
            own_cat = G.nodes[n].get("category", "")
            cross_cats = neighbor_cats - {own_cat} if own_cat else neighbor_cats
            if len(cross_cats) >= 2:
                bridges.append({
                    "node": n,
                    "question": G.nodes[n].get("question", "")[:150],
                    "own_category": own_cat,
                    "bridges_to": sorted(cross_cats),
                })
        bridges.sort(key=lambda x: len(x["bridges_to"]), reverse=True)
        results["cross_category_bridges"] = bridges[:20]
    except Exception as e:
        print(f"  Bridge detection failed: {e}")
        results["cross_category_bridges"] = []

    # --- Trader concentration (HHI) ---
    trader_conc = []
    try:
        for n in market_nodes:
            trader_edges = [(u, v, d) for u, v, d in G.edges(n, data=True) if d.get("type") == "trader_active"]
            if len(trader_edges) < 2:
                continue
            vols = [d.get("volume", 0) for _, _, d in trader_edges]
            total = sum(vols)
            if total <= 0:
                continue
            shares = [v / total for v in vols]
            hhi = sum(s * s for s in shares)
            if hhi > 0.3:
                trader_conc.append({
                    "node": n,
                    "question": G.nodes[n].get("question", "")[:150],
                    "hhi": round(hhi, 3),
                    "num_traders": len(trader_edges),
                    "volume": G.nodes[n].get("volume", 0),
                })
        trader_conc.sort(key=lambda x: x["hhi"], reverse=True)
        results["trader_concentration"] = trader_conc[:30]
    except Exception as e:
        print(f"  Trader concentration failed: {e}")
        results["trader_concentration"] = []

    # --- Price anomalies (deviation from cluster average) ---
    anomalies = []
    try:
        if communities:
            for comm in communities:
                prices = {
                    n: G.nodes[n].get("yes_price", -1)
                    for n in comm
                    if G.nodes[n].get("yes_price", -1) >= 0
                }
                if len(prices) < 3:
                    continue
                avg = np.mean(list(prices.values()))
                for n, p in prices.items():
                    dev = abs(p - avg)
                    if dev > 0.10:
                        anomalies.append({
                            "node": n,
                            "question": G.nodes[n].get("question", "")[:150],
                            "price": round(p, 3),
                            "cluster_avg": round(float(avg), 3),
                            "deviation": round(dev, 3),
                        })
            anomalies.sort(key=lambda x: x["deviation"], reverse=True)
        results["price_anomalies"] = anomalies[:30]
    except Exception as e:
        print(f"  Price anomaly detection failed: {e}")
        results["price_anomalies"] = []

    # --- Entity hubs ---
    entity_hubs = []
    try:
        entity_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "entity"]
        for en in entity_nodes:
            connected = [nb for nb in G.neighbors(en) if G.nodes[nb].get("type") == "market"]
            if len(connected) >= 2:
                entity_hubs.append({
                    "entity": G.nodes[en].get("name", ""),
                    "entity_type": G.nodes[en].get("entity_type", ""),
                    "num_markets": len(connected),
                    "sample_markets": [G.nodes[n].get("question", "")[:100] for n in connected[:5]],
                })
        entity_hubs.sort(key=lambda x: x["num_markets"], reverse=True)
        results["entity_hubs"] = entity_hubs[:30]
    except Exception as e:
        print(f"  Entity hub detection failed: {e}")
        results["entity_hubs"] = []

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    print("=== Polymarket Graph Analyzer (Phase 1) ===\n")

    with _client() as client:
        # 1. Collect markets
        print("[1/4] Fetching markets...")
        markets = fetch_markets(client, total=500)
        if not markets:
            print("ERROR: No markets fetched. Exiting.")
            return

        # 2. Collect events
        print("[2/4] Fetching events...")
        events = fetch_events(client)

        # 3. Collect trades for top 100 markets by volume
        print("[3/4] Fetching trades for top 100 markets...")
        sorted_by_vol = sorted(markets, key=lambda m: float(m.get("volume24hr", 0) or 0), reverse=True)
        top_100 = sorted_by_vol[:100]
        trade_data: dict[str, list[dict]] = {}
        for i, m in enumerate(top_100):
            cid = m.get("conditionId") or m.get("condition_id") or ""
            if not cid:
                continue
            trades = fetch_trades(client, cid, limit=200)
            if trades:
                trade_data[cid] = trades
            if (i + 1) % 20 == 0:
                print(f"  ... {i + 1}/100 markets processed ({len(trade_data)} with trades)")
        print(f"  Collected trades for {len(trade_data)} markets")

    # 4. Build graph
    print("[4/4] Building graph...")
    G = build_graph(markets, events, trade_data)
    print(f"  Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    # Node type breakdown
    type_counts = defaultdict(int)
    for _, d in G.nodes(data=True):
        type_counts[d.get("type", "unknown")] += 1
    for t, c in sorted(type_counts.items()):
        print(f"    {t}: {c}")

    # Edge type breakdown
    etype_counts = defaultdict(int)
    for _, _, d in G.edges(data=True):
        etype_counts[d.get("type", "unknown")] += 1
    for t, c in sorted(etype_counts.items()):
        print(f"    edge/{t}: {c}")

    # Save graph
    graph_path = OUT_DIR / "market_graph.graphml"
    nx.write_graphml(G, str(graph_path))
    print(f"\n  Graph saved to {graph_path} ({graph_path.stat().st_size / 1024:.0f} KB)")

    # Analysis
    print("\n=== Running analysis ===")
    analysis = analyze_graph(G)

    # Save analysis
    analysis_path = OUT_DIR / "graph_analysis.json"
    with open(analysis_path, "w") as f:
        json.dump(analysis, f, indent=2, default=str)
    print(f"\n  Analysis saved to {analysis_path} ({analysis_path.stat().st_size / 1024:.0f} KB)")

    # Print summary
    elapsed = time.time() - t0
    print(f"\n=== Summary (completed in {elapsed:.1f}s) ===")
    print(f"  Communities: {len(analysis.get('communities', []))}")
    print(f"  Central markets: {len(analysis.get('central_markets', []))}")
    print(f"  Cross-category bridges: {len(analysis.get('cross_category_bridges', []))}")
    print(f"  Concentrated markets (HHI>0.3): {len(analysis.get('trader_concentration', []))}")
    print(f"  Price anomalies (>10% dev): {len(analysis.get('price_anomalies', []))}")
    print(f"  Entity hubs: {len(analysis.get('entity_hubs', []))}")

    # Top entities
    if analysis.get("entity_hubs"):
        print("\n  Top entity hubs:")
        for eh in analysis["entity_hubs"][:10]:
            print(f"    {eh['name']} ({eh['entity_type']}): {eh['num_markets']} markets")

    # Top central markets
    if analysis.get("central_markets"):
        print("\n  Most central markets:")
        for cm in analysis["central_markets"][:5]:
            print(f"    [{cm['betweenness']:.4f}] {cm['question'][:100]}")

    # Top anomalies
    if analysis.get("price_anomalies"):
        print("\n  Largest price anomalies:")
        for pa in analysis["price_anomalies"][:5]:
            print(f"    {pa['price']:.2f} vs cluster avg {pa['cluster_avg']:.2f} "
                  f"(dev {pa['deviation']:.2f}): {pa['question'][:80]}")


if __name__ == "__main__":
    main()
