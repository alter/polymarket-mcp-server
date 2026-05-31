#!/usr/bin/env python3
"""
Skeleton for political leading-indicator trading model.

Pipeline:
1. Resolved markets (input: bot-data/political_resolved_clob.json — 720 markets)
2. Source dataset (input: bot-data/political_sources.json from web research)
3. Match: for each market, get sources with date < end_date that predicted outcome
4. Score sources: how early they were right, how many they got right
5. Build trading rule: when source X publishes new prediction → bet on Polymarket
6. Backtest: replay sources sequentially, simulate Polymarket entry timing

Output: bot-data/political_model_features.json
"""
import json
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict


def load_inputs():
    resolved = json.load(open('bot-data/political_resolved_clob.json'))
    print(f"Resolved markets: {resolved['n_total']}")
    sources_path = 'bot-data/political_sources.json'
    sources = None
    if os.path.exists(sources_path):
        sources = json.load(open(sources_path))
        print(f"Sources dataset: loaded")
    else:
        print(f"Sources dataset: not yet built (run web research first)")
    return resolved, sources


def score_source(source, correct, days_early):
    """Score quality of a source's prediction.

    correct: bool — did the source's prediction match the actual outcome?
    days_early: int — how many days before end_date was prediction published?

    Score = correct * (1 + log(1+days_early)) — rewards early correct calls.
    Wrong predictions get -1 (penalize wrong calls).
    """
    import math
    if correct:
        return 1.0 + math.log(1 + max(days_early, 0)) * 0.3
    return -1.0


def fuzzy_find_market(query, markets):
    """Find market matching query — strict match first, then keyword overlap."""
    qlow = query.lower()
    # Strict match
    for m in markets:
        if m['question'] == query:
            return m
    # Lowercase strict
    for m in markets:
        if m['question'].lower() == qlow:
            return m
    # Token-based: collect distinctive keywords
    qwords = set(w for w in qlow.replace(',','').replace('?','').split() if len(w) > 3)
    qwords -= {'will', 'vote', 'confirm', 'as', 'people', 'less', 'more', 'than'}
    best_m = None
    best_score = 0
    for m in markets:
        mwords = set(w for w in m['question'].lower().replace(',','').replace('?','').split() if len(w) > 3)
        overlap = len(qwords & mwords)
        if overlap >= 3 and overlap > best_score:
            best_score = overlap
            best_m = m
    return best_m


def build_features(resolved, sources):
    """For each (source, market) pair, compute features for ML model."""
    if sources is None:
        print("Cannot build features without sources data")
        return None
    market_by_id = {m['id']: m for m in resolved['markets']}
    market_by_q = {m['question']: m for m in resolved['markets']}

    source_stats = defaultdict(lambda: {
        'total_predictions': 0,
        'correct': 0,
        'days_early_sum': 0,
        'wrong': 0,
        'score_sum': 0.0,
        'markets_predicted': [],
    })

    n_matched = 0
    n_total_entries = 0
    for entry in sources.get('predictions', []):
        n_total_entries += 1
        market = market_by_q.get(entry['market_question'])
        if not market:
            market = fuzzy_find_market(entry['market_question'], resolved['markets'])
        if not market:
            continue
        n_matched += 1
        actual = market['outcome']
        for src in entry.get('sources', []):
            # Handle partial dates: "2025-12-XX" → "2025-12-01"
            d_str = src.get('date', '').strip()
            if not d_str:
                continue
            d_str_clean = d_str.replace('XX', '01').replace('XX', '01').replace('Z','+00:00')
            try:
                src_date = datetime.fromisoformat(d_str_clean)
                if src_date.tzinfo is None:
                    src_date = src_date.replace(tzinfo=timezone.utc)
                end_date_str = market['end_date'].replace('Z','+00:00') if market.get('end_date') else ''
                if not end_date_str:
                    continue
                end_date = datetime.fromisoformat(end_date_str)
                if end_date.tzinfo is None:
                    end_date = end_date.replace(tzinfo=timezone.utc)
                days_early = (end_date - src_date).days
            except Exception as e:
                continue
            if days_early < 0:
                continue
            predicted_str = src.get('prediction', '').upper()
            # Predictions like "predicted YES" / "predicted NO"
            if 'YES' in predicted_str:
                predicted = 'YES'
            elif 'NO' in predicted_str:
                predicted = 'NO'
            else:
                continue
            correct = (predicted == actual)
            url = src.get('url', '') or ''
            source_name = src.get('source_name') or 'unknown'
            if source_name == 'unknown' and url and '://' in url:
                try:
                    source_name = url.split('//')[1].split('/')[0]
                except Exception:
                    pass

            stats = source_stats[source_name]
            stats['total_predictions'] += 1
            if correct:
                stats['correct'] += 1
                stats['days_early_sum'] += days_early
            else:
                stats['wrong'] += 1
            stats['score_sum'] += score_source(src, correct, days_early)
            stats['markets_predicted'].append({
                'market': entry['market_question'],
                'date': src.get('date'),
                'predicted': predicted,
                'actual': actual,
                'correct': correct,
                'days_early': days_early,
            })

    # Rank sources
    ranked = []
    for name, stats in source_stats.items():
        n = stats['total_predictions']
        if n < 1:
            continue
        accuracy = stats['correct'] / n
        avg_days_early = stats['days_early_sum'] / max(stats['correct'], 1)
        ranked.append({
            'source': name,
            'n_predictions': n,
            'correct': stats['correct'],
            'wrong': stats['wrong'],
            'accuracy': round(accuracy, 3),
            'avg_days_early': round(avg_days_early, 1),
            'score': round(stats['score_sum'], 2),
            'markets_predicted': stats['markets_predicted'],
        })
    ranked.sort(key=lambda r: -r['score'])

    return {
        'ranked_sources': ranked,
        'n_total_sources': len(source_stats),
        'matched_entries': n_matched,
        'total_entries': n_total_entries,
    }


def main():
    resolved, sources = load_inputs()
    features = build_features(resolved, sources)
    if features:
        with open('bot-data/political_model_features.json', 'w') as f:
            json.dump(features, f, indent=1)
        print(f"\nTop 15 sources by score:")
        print(f"  {'source':<35} {'n':>4} {'correct':>7} {'acc':>6} {'avg_early':>9} {'score':>7}")
        for r in features['ranked_sources'][:15]:
            print(f"  {r['source'][:35]:<35} {r['n_predictions']:>4} "
                  f"{r['correct']:>7} {r['accuracy']:>5.0%} "
                  f"{r['avg_days_early']:>8.1f}d {r['score']:>+6.2f}")
    else:
        print("Run political web research first to populate sources, then re-run this.")


if __name__ == "__main__":
    main()
