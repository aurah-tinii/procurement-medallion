#!/usr/bin/env python3
"""
Stage 1: pull OCDS notices from UK Contracts Finder and download attached documents.

Run on your laptop (Databricks Free Edition blocks most outbound internet).
Output layout (this folder is what you later upload to a Unity Catalog volume):

  data/bronze/ocds/page_0001.json ...   raw API pages, untouched
  data/bronze/docs/<sha12>_<name>       downloaded attachments
  data/bronze/manifest.csv              one row per document link found

Usage:
  pip install requests
  python fetch_ocds.py --days 14 --stages award --max-pages 5 --max-docs 60
"""
import argparse
import csv
import hashlib
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

BASE = "https://www.contractsfinder.service.gov.uk/Published/Notices/OCDS/Search"
HEADERS = {"User-Agent": "personal-learning-project/0.1"}
KEEP_EXT = {".pdf", ".docx", ".doc", ".txt"}
MANIFEST_COLS = [
    "ocid", "release_id", "release_tag", "doc_id", "doc_type", "title",
    "url", "status", "content_type", "bytes", "sha256", "local_path",
]


def fetch_pages(params, max_pages):
    """Yield raw JSON pages, following links.next when present."""
    url, p = BASE, dict(params)
    for _ in range(max_pages):
        r = requests.get(url, params=p, headers=HEADERS, timeout=60)
        r.raise_for_status()
        data = r.json()
        yield data
        nxt = (data.get("links") or {}).get("next")
        if not nxt:
            return
        url, p = nxt, None  # next link is already a full URL
        time.sleep(1)


def find_documents(node):
    """Recursively collect every dict found under a 'documents' list."""
    found = []
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "documents" and isinstance(v, list):
                found += [d for d in v if isinstance(d, dict)]
            else:
                found += find_documents(v)
    elif isinstance(node, list):
        for item in node:
            found += find_documents(item)
    return found


def safe_name(s):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)[:80] or "doc"


def download(doc, docs_dir):
    """Download one attachment. Returns dict of status fields."""
    url = doc.get("url", "")
    out = {"status": "no_url", "content_type": "", "bytes": 0, "sha256": "", "local_path": ""}
    if not url:
        return out
    try:
        r = requests.get(url, headers=HEADERS, timeout=90)
        out["content_type"] = r.headers.get("Content-Type", "")
        if r.status_code != 200:
            out["status"] = f"http_{r.status_code}"
            return out
        digest = hashlib.sha256(r.content).hexdigest()
        name = safe_name(doc.get("title") or doc.get("id") or "doc")
        ext = Path(name).suffix.lower()
        if not ext:
            ct = out["content_type"].lower()
            ext = ".pdf" if "pdf" in ct else ".docx" if "wordprocessingml" in ct else ".bin"
            name += ext
        path = docs_dir / f"{digest[:12]}_{name}"
        path.write_bytes(r.content)
        out.update(status="ok", bytes=len(r.content), sha256=digest, local_path=str(path))
    except requests.RequestException as e:
        out["status"] = f"error_{type(e).__name__}"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14, help="look back this many days")
    ap.add_argument("--stages", default="award", help="planning, tender, award, implementation")
    ap.add_argument("--size", type=int, default=100)
    ap.add_argument("--max-pages", type=int, default=5)
    ap.add_argument("--max-docs", type=int, default=60)
    ap.add_argument("--out", default="data/bronze")
    args = ap.parse_args()

    out = Path(args.out)
    (out / "ocds").mkdir(parents=True, exist_ok=True)
    (out / "docs").mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    params = {
        "publishedFrom": (now - timedelta(days=args.days)).strftime("%Y-%m-%dT00:00:00"),
        "publishedTo": now.strftime("%Y-%m-%dT23:59:59"),
        "stages": args.stages,
        "size": args.size,
    }

    rows, n_releases, downloaded = [], 0, 0
    for i, page in enumerate(fetch_pages(params, args.max_pages), start=1):
        (out / "ocds" / f"page_{i:04d}.json").write_text(json.dumps(page))
        releases = page.get("releases", [])
        n_releases += len(releases)
        for rel in releases:
            for doc in find_documents(rel):
                url = doc.get("url", "")
                row = {
                    "ocid": rel.get("ocid", ""), "release_id": rel.get("id", ""),
                    "release_tag": ",".join(rel.get("tag", [])),
                    "doc_id": doc.get("id", ""), "doc_type": doc.get("documentType", ""),
                    "title": doc.get("title", ""), "url": url,
                    "status": "skipped", "content_type": "", "bytes": 0,
                    "sha256": "", "local_path": "",
                }
                if url and downloaded < args.max_docs:
                    row.update(download(doc, out / "docs"))
                    downloaded += 1
                    time.sleep(1)
                rows.append(row)
        print(f"page {i}: {len(releases)} releases, {len(rows)} doc links so far")

    with open(out / "manifest.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        w.writerows(rows)

    ok = sum(1 for r in rows if r["status"] == "ok")
    print(f"\nreleases: {n_releases} | doc links: {len(rows)} | downloaded ok: {ok}")
    print("status breakdown:", {s: sum(1 for r in rows if r['status'] == s) for s in {r['status'] for r in rows}})


if __name__ == "__main__":
    main()
