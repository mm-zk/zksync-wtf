#!/usr/bin/env python3
"""
Merge JSON files in a directory (flat) into a single array the zksync‑wtf webpage can load.

Input file shape (example):
{
  "fetched_at": "2025-08-09T10:12:54.996311Z",
  "items": {
    "0.24.0.leaf": {
      "description": "Boojum Hash for 0.24.0.leaf version 0.24.0 in zksync-era",
      "url": "https://github.com/matter-labs/zksync-era/blob/main/prover/data/historical_data/0.24.0/commitments.json",
      "value": "0xffb19d..."
    }
  }
}

Output file shape (array):
[
  {"key":"0.24.0.leaf","value":"0xff...","description":"...","url":"https://..."},
  ...
]

Usage:
  python merge_verifier_jsons.py /path/to/subdir --out zksync_wtf_data.json

Notes:
- If two files define the same key, the record with the NEWER `fetched_at` wins.
- Files with invalid/missing `items` are skipped with a warning.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timezone


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge verifier JSONs for zksync-wtf webpage")
    p.add_argument("indir", type=Path, help="Directory containing input .json files (non-recursive)")
    p.add_argument("--out", type=Path, default=Path("zksync_wtf_data.json"), help="Output JSON file path")
    p.add_argument("--recursive", action="store_true", help="Recurse into subdirectories as well")
    return p.parse_args()


def parse_ts(s: Optional[str]) -> datetime:
    if not s:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    # Accept ...Z or with offset
    try:
        # Python 3.11+ supports Z; for older, replace Z with +00:00
        if s.endswith("Z") and "+" not in s and "-" not in s[10:]:
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return datetime.fromtimestamp(0, tz=timezone.utc)


def load_file(p: Path) -> Optional[Dict[str, Any]]:
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[warn] Skip {p.name}: {e}")
        return None


def main() -> None:
    args = parse_args()
    if not args.indir.exists() or not args.indir.is_dir():
        raise SystemExit(f"Input directory not found: {args.indir}")

    glob_pat = "**/*.json" if args.recursive else "*.json"
    files = sorted(args.indir.glob(glob_pat))
    if not files:
        print("[info] No JSON files found.")

    # Merge by key; keep newest by fetched_at
    records: Dict[str, Dict[str, Any]] = {}
    seen_from: Dict[str, datetime] = {}

    for fp in files:
        data = load_file(fp)
        if not data:
            continue
        items = data.get("items")
        if not isinstance(items, dict):
            print(f"[warn] {fp.name}: missing/invalid 'items' object; skipping")
            continue
        ts = parse_ts(data.get("fetched_at"))

        for k, v in items.items():
            if not isinstance(v, dict):
                continue
            key = str(k)
            value = v.get("value")
            if value is None:
                # must have a value to be useful on the site
                continue
            rec = {
                "key": key,
                "value": str(value),
                "description": str(v.get("description", "")),
                "url": v.get("url") or None,
            }
            # keep if newer timestamp or unseen
            prev_ts = seen_from.get(key)
            if (prev_ts is None) or (ts > prev_ts):
                records[key] = rec
                seen_from[key] = ts
            else:
                # If same timestamp, prefer one with non-empty description/url
                if ts == prev_ts:
                    cur = records[key]
                    cur_score = int(bool(cur.get("description"))) + int(bool(cur.get("url")))
                    new_score = int(bool(rec.get("description"))) + int(bool(rec.get("url")))
                    if new_score > cur_score:
                        records[key] = rec

    # Emit as a stable, nicely sorted array
    out_arr = sorted(records.values(), key=lambda r: r["key"].lower())

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as f:
        json.dump(out_arr, f, ensure_ascii=False, indent=2)

    print(f"[ok] Wrote {args.out} — {len(out_arr)} records from {len(files)} files")


if __name__ == "__main__":
    main()
