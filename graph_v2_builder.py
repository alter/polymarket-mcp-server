#!/usr/bin/env python3
"""
Graph V2 Builder: Takes collected data and builds a rich graph.
Runs AFTER graph_v2_collector.py.

Nodes: market, news, trader, entity
Edges: price_correlation, shared_entity, news_linked, trader_active,
       granger_causal, temporal_overlap, same_group

Analysis: communities, centrality, Granger causality, news-price impact,
          trader clustering, anomaly detection.

Usage: python3 graph_v2_builder.py
"""
import json, re, time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import networkx as nx
import numpy as np

DATA = Path("bot-data/graph_v2")
OUT = Path("bot-data/graph_v2")


def load_data():
    markets = json.load(open(DATA / "markets.json"))
    snapshots = json.load(open(DATA / "snapshots.json"))
    news = json.load(open(DATA / "news.json"))
    matches = json.load(open(DATA / "news_matches.json"))
    traders = json.load(open(DATA / "traders.json"))
    return markets, snapshots, news, matches, traders


# ─── ENTITY EXTRACTION ──────────────────────────────────────────────────────

PEOPLE_RE = re.compile(r"\b[A-Z][a-z]{2,} [A-Z][a-z]{2,}\b")
FALSE_FIRST = {"Will","How","What","When","Which","Does","Can","The","New",
               "Super","World","South","North","West","East","United","Saudi",
               "Los","San","Las","Real","Club","Upper"}


def extract_entities(text):
    ents = set()
    if not text: return ents
    for m in PEOPLE_RE.finditer(text):
        name = m.group()
        if name.split()[0] not in FALSE_FIRST:
            ents.add(("person", name))
    for kw in ["Bitcoin","Ethereum","Solana","XRP","Trump","Fed","NATO","SEC",
               "Iran","Israel","Ukraine","Russia","China","India"]:
        if kw.lower() in text.lower():
            ents.add(("keyword", kw))
    return ents


# ─── GRANGER CAUSALITY (simplified) ─────────────────────────────────────────

def granger_test(series_a, series_b, max_lag=3):
    """
    Simplified Granger causality: does series_a predict series_b?
    Returns (f_stat, is_significant) at best lag.
    Uses OLS approach: compare restricted (only own lags) vs unrestricted model.
    """
    if len(series_a) < 10 or len(series_b) < 10:
        return 0, False

    # Align to same timestamps
    ta = {s[0]: s[1] for s in series_a}  # [ts, vwap, ...]
    tb = {s[0]: s[1] for s in series_b}
    common_ts = sorted(set(ta.keys()) & set(tb.keys()))
    if len(common_ts) < 10:
        return 0, False

    a = np.array([ta[t] for t in common_ts])
    b = np.array([tb[t] for t in common_ts])

    # Compute returns
    if np.any(a[:-1] == 0) or np.any(b[:-1] == 0):
        return 0, False
    ra = np.diff(a) / a[:-1]
    rb = np.diff(b) / b[:-1]

    n = len(ra)
    if n < max_lag + 5:
        return 0, False

    best_f = 0
    for lag in range(1, max_lag + 1):
        y = rb[lag:]
        x_own = np.column_stack([rb[lag-j-1:n-j-1] for j in range(lag)])
        x_cross = np.column_stack([ra[lag-j-1:n-j-1] for j in range(lag)])

        # Restricted model: y ~ own lags
        try:
            X_r = np.column_stack([np.ones(len(y)), x_own])
            beta_r = np.linalg.lstsq(X_r, y, rcond=None)[0]
            rss_r = np.sum((y - X_r @ beta_r) ** 2)

            # Unrestricted model: y ~ own lags + cross lags
            X_u = np.column_stack([np.ones(len(y)), x_own, x_cross])
            beta_u = np.linalg.lstsq(X_u, y, rcond=None)[0]
            rss_u = np.sum((y - X_u @ beta_u) ** 2)

            if rss_u <= 0:
                continue

            df1 = lag
            df2 = len(y) - 2 * lag - 1
            if df2 <= 0:
                continue

            f_stat = ((rss_r - rss_u) / df1) / (rss_u / df2)
            if f_stat > best_f:
                best_f = f_stat
        except:
            continue

    # F-critical at 5% for small samples ≈ 4-6
    return best_f, best_f > 4.0


# ─── GRAPH BUILDING ─────────────────────────────────────────────────────────

def build_graph(markets, snapshots, news, matches, traders):
    G = nx.Graph()
    t0 = time.time()

    # --- Market nodes ---
    cid_to_node = {}
    group_markets = defaultdict(list)
    for m in markets:
        cid = m["c"]
        nid = f"m:{cid[:16]}"
        cid_to_node[cid] = nid
        G.add_node(nid, type="market", question=m["q"][:150],
                   yes_price=m["y"], volume=m["v"], volume24hr=m["v24"],
                   group=m.get("g", ""))
        if m.get("g"):
            group_markets[m["g"]].append(nid)

    print(f"  Market nodes: {len(cid_to_node)}")

    # --- Same group edges ---
    group_edges = 0
    for group, nids in group_markets.items():
        if len(nids) < 2: continue
        for i in range(len(nids)):
            for j in range(i+1, min(i+30, len(nids))):
                G.add_edge(nids[i], nids[j], type="same_group", group=group[:30])
                group_edges += 1
    print(f"  Same-group edges: {group_edges}")

    # --- Entity nodes + shared_entity edges ---
    entity_to_markets = defaultdict(list)
    for m in markets:
        cid = m["c"]
        if cid not in cid_to_node: continue
        nid = cid_to_node[cid]
        ents = extract_entities(m["q"])
        for etype, ename in ents:
            key = f"ent:{ename}"
            entity_to_markets[key].append(nid)

    ent_edges = 0
    for key, nids in entity_to_markets.items():
        if len(nids) < 2: continue
        ename = key.split(":", 1)[1]
        if key not in G:
            G.add_node(key, type="entity", name=ename)
        for i in range(len(nids)):
            for j in range(i+1, min(i+20, len(nids))):
                if not G.has_edge(nids[i], nids[j]):
                    G.add_edge(nids[i], nids[j], type="shared_entity", entity=ename[:30])
                    ent_edges += 1
    print(f"  Entity nodes: {len([n for n,d in G.nodes(data=True) if d.get('type')=='entity'])}")
    print(f"  Shared-entity edges: {ent_edges}")

    # --- News nodes + news_linked edges ---
    news_edges = 0
    news_nodes = 0
    match_by_article = defaultdict(list)
    for match in matches:
        match_by_article[match["article_id"]].append(match)

    for art in news:
        aid = art["id"]
        art_matches = match_by_article.get(aid, [])
        if not art_matches: continue
        # Only add news that matches 2+ markets (more interesting)
        market_matches = [m for m in art_matches if m["market_cid"] in cid_to_node]
        if len(market_matches) < 1: continue

        nid = f"n:{aid}"
        G.add_node(nid, type="news", title=art.get("title", "")[:100],
                   source=art.get("source", ""), pub=art.get("pub", ""))
        news_nodes += 1

        for match in market_matches[:10]:
            mid = cid_to_node[match["market_cid"]]
            G.add_edge(nid, mid, type="news_linked", score=match["score"])
            news_edges += 1
    print(f"  News nodes: {news_nodes}, news-market edges: {news_edges}")

    # --- Trader nodes + trader_active edges ---
    trader_edges = 0
    trader_nodes = 0
    for wallet, mkts in traders.items():
        active_in = [cid for cid in mkts if cid in cid_to_node]
        if len(active_in) < 2: continue
        tid = f"t:{wallet[:12]}"
        total_vol = sum(mkts[c].get("b", 0) + mkts[c].get("s", 0) for c in active_in)
        G.add_node(tid, type="trader", wallet=wallet[:12],
                   num_markets=len(active_in), total_volume=round(total_vol))
        trader_nodes += 1
        for cid in active_in:
            mid = cid_to_node[cid]
            mv = mkts[cid]
            G.add_edge(tid, mid, type="trader_active",
                       buy_vol=mv.get("b", 0), sell_vol=mv.get("s", 0))
            trader_edges += 1
    print(f"  Trader nodes: {trader_nodes}, trader-market edges: {trader_edges}")

    # --- Price correlation edges ---
    print("  Computing price correlations...")
    series_map = {}
    for cid, series in snapshots.items():
        if cid in cid_to_node and len(series) >= 5:
            series_map[cid] = series

    # Only check correlations between markets that share entities or groups
    candidate_pairs = set()
    for key, nids in entity_to_markets.items():
        cids = [cid for cid, n in cid_to_node.items() if n in nids and cid in series_map]
        for i in range(len(cids)):
            for j in range(i+1, min(i+10, len(cids))):
                candidate_pairs.add((cids[i], cids[j]))
    for group, nids in group_markets.items():
        cids = [cid for cid, n in cid_to_node.items() if n in nids and cid in series_map]
        for i in range(len(cids)):
            for j in range(i+1, min(i+10, len(cids))):
                candidate_pairs.add((cids[i], cids[j]))

    corr_edges = 0
    granger_edges = 0
    for cid_a, cid_b in list(candidate_pairs)[:5000]:
        sa = series_map.get(cid_a, [])
        sb = series_map.get(cid_b, [])
        if len(sa) < 5 or len(sb) < 5: continue

        # Pearson on common timestamps
        ta = {s[0]: s[1] for s in sa}
        tb = {s[0]: s[1] for s in sb}
        common = sorted(set(ta.keys()) & set(tb.keys()))
        if len(common) < 5: continue

        va = np.array([ta[t] for t in common])
        vb = np.array([tb[t] for t in common])
        if np.std(va) < 1e-6 or np.std(vb) < 1e-6: continue

        corr = float(np.corrcoef(va, vb)[0, 1])
        if abs(corr) > 0.5:
            na = cid_to_node[cid_a]
            nb = cid_to_node[cid_b]
            G.add_edge(na, nb, type="price_corr", correlation=round(corr, 3))
            corr_edges += 1

        # Granger causality (both directions)
        f_ab, sig_ab = granger_test(sa, sb)
        if sig_ab:
            na = cid_to_node[cid_a]
            nb = cid_to_node[cid_b]
            G.add_edge(na, nb, type="granger_causal",
                       direction=f"{cid_a[:8]}→{cid_b[:8]}", f_stat=round(f_ab, 2))
            granger_edges += 1

        f_ba, sig_ba = granger_test(sb, sa)
        if sig_ba:
            na = cid_to_node[cid_a]
            nb = cid_to_node[cid_b]
            if not G.has_edge(na, nb) or G[na][nb].get("type") != "granger_causal":
                G.add_edge(na, nb, type="granger_causal",
                           direction=f"{cid_b[:8]}→{cid_a[:8]}", f_stat=round(f_ba, 2))
                granger_edges += 1

    print(f"  Price correlation edges: {corr_edges}")
    print(f"  Granger causal edges: {granger_edges}")

    print(f"\n  TOTAL: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges ({time.time()-t0:.1f}s)")
    return G


# ─── ANALYSIS ────────────────────────────────────────────────────────────────

def analyze(G):
    results = {}
    market_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "market"]

    # Community detection
    msub = G.subgraph(market_nodes).copy()
    try:
        communities = list(nx.community.louvain_communities(msub, seed=42))
        comm_data = []
        for idx, comm in enumerate(sorted(communities, key=len, reverse=True)[:30]):
            prices = [G.nodes[n].get("yes_price", -1) for n in comm if G.nodes[n].get("yes_price", -1) >= 0]
            groups = [G.nodes[n].get("group", "") for n in comm if G.nodes[n].get("group")]
            top_group = max(set(groups), key=groups.count) if groups else ""
            comm_data.append({
                "id": idx, "size": len(comm),
                "avg_price": round(float(np.mean(prices)), 3) if prices else None,
                "dominant_group": top_group,
                "samples": [G.nodes[n].get("question", "")[:80] for n in list(comm)[:5]],
            })
        results["communities"] = comm_data
        print(f"  Communities: {len(communities)}")
    except Exception as e:
        print(f"  Community detection failed: {e}")
        communities = []

    # Centrality
    try:
        bc = nx.betweenness_centrality(msub)
        top20 = sorted(bc.items(), key=lambda x: -x[1])[:20]
        results["central_markets"] = [{
            "node": n, "question": G.nodes[n].get("question", "")[:100],
            "centrality": round(s, 5), "volume": G.nodes[n].get("volume", 0),
        } for n, s in top20]
    except:
        results["central_markets"] = []

    # Granger causal links
    granger = [(u, v, d) for u, v, d in G.edges(data=True) if d.get("type") == "granger_causal"]
    results["granger_links"] = [{
        "from": G.nodes[u].get("question", "")[:60],
        "to": G.nodes[v].get("question", "")[:60],
        "f_stat": d.get("f_stat", 0),
        "direction": d.get("direction", ""),
    } for u, v, d in sorted(granger, key=lambda x: -x[2].get("f_stat", 0))[:30]]
    print(f"  Granger links: {len(granger)}")

    # News impact: news articles connected to markets with big price moves
    news_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "news"]
    news_impact = []
    for nn in news_nodes:
        connected_markets = [nb for nb in G.neighbors(nn)
                            if G.nodes[nb].get("type") == "market"]
        if len(connected_markets) >= 2:
            news_impact.append({
                "title": G.nodes[nn].get("title", "")[:80],
                "source": G.nodes[nn].get("source", ""),
                "markets_connected": len(connected_markets),
                "market_samples": [G.nodes[m].get("question", "")[:60] for m in connected_markets[:3]],
            })
    news_impact.sort(key=lambda x: -x["markets_connected"])
    results["news_impact"] = news_impact[:30]
    print(f"  News connecting 2+ markets: {len([n for n in news_impact if n['markets_connected'] >= 2])}")

    # Trader clusters (shared-trader bridges)
    trader_bridges = []
    for t_node in [n for n, d in G.nodes(data=True) if d.get("type") == "trader"]:
        t_markets = [nb for nb in G.neighbors(t_node) if G.nodes[nb].get("type") == "market"]
        if len(t_markets) >= 3:
            groups = set(G.nodes[m].get("group", "") for m in t_markets if G.nodes[m].get("group"))
            if len(groups) >= 2:
                trader_bridges.append({
                    "wallet": G.nodes[t_node].get("wallet", ""),
                    "markets": len(t_markets),
                    "groups": sorted(groups)[:5],
                    "total_volume": G.nodes[t_node].get("total_volume", 0),
                })
    trader_bridges.sort(key=lambda x: -x["markets"])
    results["trader_bridges"] = trader_bridges[:20]
    print(f"  Trader bridges (3+ markets, 2+ groups): {len(trader_bridges)}")

    # Price anomalies (vs community average)
    anomalies = []
    if communities:
        for comm in communities:
            prices = {n: G.nodes[n].get("yes_price", -1) for n in comm
                     if G.nodes[n].get("yes_price", -1) >= 0}
            if len(prices) < 3: continue
            avg = float(np.mean(list(prices.values())))
            for n, p in prices.items():
                dev = abs(p - avg)
                if dev > 0.15:
                    anomalies.append({
                        "question": G.nodes[n].get("question", "")[:80],
                        "price": round(p, 3), "cluster_avg": round(avg, 3),
                        "deviation": round(dev, 3),
                    })
    anomalies.sort(key=lambda x: -x["deviation"])
    results["price_anomalies"] = anomalies[:30]
    print(f"  Price anomalies (>15% dev): {len(anomalies)}")

    return results


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    print("=== Graph V2 Builder ===\n")

    print("[1] Loading data...")
    markets, snapshots, news, matches, traders = load_data()
    print(f"  Markets: {len(markets)}, Snapshots: {len(snapshots)}, "
          f"News: {len(news)}, Matches: {len(matches)}, Traders: {len(traders)}")

    print("\n[2] Building graph...")
    G = build_graph(markets, snapshots, news, matches, traders)

    # Edge type breakdown
    etype_counts = defaultdict(int)
    for _, _, d in G.edges(data=True):
        etype_counts[d.get("type", "unknown")] += 1
    print("\n  Edge types:")
    for t, c in sorted(etype_counts.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c}")

    print("\n[3] Running analysis...")
    results = analyze(G)

    # Save
    nx.write_graphml(G, str(OUT / "graph_v2.graphml"))
    with open(OUT / "analysis_v2.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    graph_kb = (OUT / "graph_v2.graphml").stat().st_size / 1024
    analysis_kb = (OUT / "analysis_v2.json").stat().st_size / 1024
    print(f"\n  Saved graph: {graph_kb:.0f} KB, analysis: {analysis_kb:.0f} KB")

    # Print key findings
    elapsed = time.time() - t0
    print(f"\n{'='*80}")
    print(f"KEY FINDINGS ({elapsed:.0f}s)")
    print(f"{'='*80}")

    if results.get("granger_links"):
        print(f"\nGranger Causal Links (A predicts B):")
        for gl in results["granger_links"][:10]:
            print(f"  F={gl['f_stat']:>5.1f}: {gl['from'][:40]} → {gl['to'][:40]}")

    if results.get("news_impact"):
        print(f"\nHigh-Impact News (connected to 2+ markets):")
        for ni in results["news_impact"][:5]:
            print(f"  [{ni['source']}] {ni['title'][:60]} → {ni['markets_connected']} markets")

    if results.get("trader_bridges"):
        print(f"\nCross-Group Traders:")
        for tb in results["trader_bridges"][:5]:
            print(f"  {tb['wallet']}: {tb['markets']} markets across {tb['groups'][:3]}")

    if results.get("price_anomalies"):
        print(f"\nPrice Anomalies:")
        for pa in results["price_anomalies"][:5]:
            print(f"  {pa['price']:.2f} vs avg {pa['cluster_avg']:.2f}: {pa['question'][:60]}")


if __name__ == "__main__":
    main()
