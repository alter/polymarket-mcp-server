#!/usr/bin/env python3
"""Aggregate articles/raw/*.jsonl → deduped master index (index.json + README.md).

Dedup by normalized URL. Reports per-theme and per-type counts, full-text coverage.
Run after all collector agents finish: python3 articles/build_index.py
"""
import json, glob, os, re
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(ROOT, "raw")


def norm_url(u):
    if not u:
        return ""
    u = u.strip().split("#")[0]
    u = re.sub(r"^https?://", "", u, flags=re.I)
    u = re.sub(r"^www\.", "", u, flags=re.I)
    u = u.rstrip("/")
    # strip common tracking params
    u = re.sub(r"[?&](utm_[^=]+|ref|source)=[^&]*", "", u)
    return u.lower()


def parse_frontmatter(path):
    """Extract YAML-ish front-matter from a text/*.md file into a record."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            head = f.read(4000)
    except Exception:
        return None
    if not head.startswith("---"):
        return None
    end = head.find("\n---", 3)
    if end < 0:
        return None
    rec = {}
    for line in head[3:end].splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        rec[k.strip().lower()] = v.strip().strip('"\'')
    if not rec.get("url"):
        return None
    rec["text_saved"] = True
    rec["text_file"] = os.path.relpath(path, ROOT)
    return rec


def main():
    records = []
    files = sorted(glob.glob(os.path.join(RAW, "*.jsonl")))
    for fp in files:
        with open(fp, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except Exception:
                    continue

    # Fallback: ingest metadata from text/*.md front-matter too, so agents that
    # saved text but never wrote their raw jsonl (e.g. output-overflow) still count.
    for md in glob.glob(os.path.join(ROOT, "text", "*.md")):
        rec = parse_frontmatter(md)
        if rec:
            records.append(rec)

    # Dedup by normalized URL; prefer the record that has full text saved
    by_url = {}
    for r in records:
        k = norm_url(r.get("url", ""))
        if not k:
            continue
        if k not in by_url:
            by_url[k] = r
        else:
            # keep the richer record (text_saved wins, else longer snippet)
            old = by_url[k]
            if r.get("text_saved") and not old.get("text_saved"):
                by_url[k] = r
            elif len(r.get("snippet", "")) > len(old.get("snippet", "")):
                by_url[k] = r

    deduped = list(by_url.values())
    deduped.sort(key=lambda r: (r.get("theme", ""), r.get("type", ""), r.get("title", "")))

    by_theme = Counter(r.get("theme", "?") for r in deduped)
    by_type = Counter(r.get("type", "?") for r in deduped)
    by_lang = Counter(r.get("lang", "?") for r in deduped)
    n_text = sum(1 for r in deduped if r.get("text_saved"))
    text_files = len(glob.glob(os.path.join(ROOT, "text", "*.md")))

    index = {
        "total_raw_records": len(records),
        "total_unique": len(deduped),
        "full_text_saved": n_text,
        "text_files_on_disk": text_files,
        "by_theme": dict(by_theme.most_common()),
        "by_type": dict(by_type.most_common()),
        "by_lang": dict(by_lang.most_common()),
        "articles": deduped,
    }
    with open(os.path.join(ROOT, "index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=1)

    # README
    lines = []
    lines.append("# Polymarket / Prediction-Market Trading — Article Corpus\n")
    lines.append(f"**{len(deduped)} unique articles** "
                 f"({len(records)} raw, {len(records)-len(deduped)} dupes removed) | "
                 f"**{n_text} with full text** saved in `text/`.\n")
    lines.append("Metadata: `index.json` (full) and `raw/*.jsonl` (per theme). "
                 "Full text: `text/<theme>__<slug>.md`.\n")
    lines.append("## By theme\n")
    for t, c in by_theme.most_common():
        lines.append(f"- **{t}**: {c}")
    lines.append("\n## By type\n")
    for t, c in by_type.most_common():
        lines.append(f"- {t}: {c}")
    lines.append("\n## By language\n")
    for t, c in by_lang.most_common():
        lines.append(f"- {t}: {c}")
    lines.append("\n## Articles by theme\n")
    cur = None
    for r in deduped:
        th = r.get("theme", "?")
        if th != cur:
            lines.append(f"\n### {th}\n")
            cur = th
        mark = "📄" if r.get("text_saved") else "🔗"
        title = (r.get("title") or "(untitled)").replace("\n", " ").strip()
        lines.append(f"- {mark} [{title}]({r.get('url','')}) — {r.get('source','')} "
                     f"({r.get('type','')}, {r.get('date','n/d')})")
    with open(os.path.join(ROOT, "README.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"raw={len(records)} unique={len(deduped)} full_text={n_text} "
          f"text_files={text_files}")
    print("by_theme:", dict(by_theme.most_common()))
    print("by_type:", dict(by_type.most_common()))
    print("Wrote index.json + README.md")


if __name__ == "__main__":
    main()
