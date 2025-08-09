#!/usr/bin/env python3
"""
Read a CSV, keep only rows where Status == "Live", and emit JSON entries for:
  • chain_id (value is the number; url points to https://chainlist.org/chain/<NUMBER>)
  • MLExplorer, AltExplorer, Portal, HTTPS RPC (value is the URL; url also the same)

Each output record matches the site schema: {"key", "value", "description", "url"}.

Usage:
  python csv_to_webdata.py input.csv --out zksync_wtf_data_from_csv.json

Options:
  --status-col   Name of the status column (default: Status)
  --live-value   Value that indicates the row is live (default: Live)
  --name-cols    Comma-separated fallbacks to find human-friendly chain name (default tries common names)

Notes:
  • If a row has multiple URLs in a cell (comma/semicolon/whitespace-separated), one entry is created per URL.
  • Empty cells are skipped. Duplicate entries (same key+value+url) are de-duplicated.
  • Column matching is case-insensitive and tolerates minor variations (e.g. "Chain ID" vs "chain_id").
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CSV → zksync-wtf JSON converter")
    p.add_argument("csv", type=Path, help="Input CSV file")
    p.add_argument("--out", type=Path, default=Path("zksync_wtf_data_from_csv.json"), help="Output JSON path")
    p.add_argument("--status-col", default="Status", help="Status column name")
    p.add_argument("--live-value", default="Live", help="Row value that means 'live'")
    p.add_argument(
        "--name-cols",
        default="Name,Chain Name,Chain,Network,Title",
        help="Comma-separated candidate columns for a nice display name",
    )
    return p.parse_args()


def normalize_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", h.strip().lower()).strip()


def find_column(header_map: Dict[str, str], candidates: Iterable[str]) -> Optional[str]:
    cand_norm = [normalize_header(c) for c in candidates]
    for norm, original in header_map.items():
        if norm in cand_norm:
            return original
    return None


def split_urls(cell: str) -> List[str]:
    # Split on commas, semicolons, pipes, whitespace; collapse empties
    parts = re.split(r"[\s,;|]+", cell.strip())
    return [p for p in parts if p]


def key_exists(d: Dict[str, str], key: str) -> bool:
    return any(normalize_header(k) == normalize_header(key) for k in d.keys())


def main() -> None:
    args = parse_args()
    if not args.csv.exists():
        raise SystemExit(f"CSV not found: {args.csv}")

    # Read CSV
    with args.csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        if not rows:
            print("[info] CSV is empty; nothing to do.")
            Path(args.out).write_text("[]", encoding="utf-8")
            return

        # Build a normalized header → original header map
        header_map: Dict[str, str] = {normalize_header(h): h for h in reader.fieldnames or []}

    # Resolve key columns (case-insensitive with common aliases)
    status_col = find_column(header_map, [args.status_col]) or args.status_col

    chain_id_col = find_column(header_map, [
        "chain_id", "chain id", "chainid", "id"
    ])

    name_col = find_column(header_map, [c.strip() for c in args.name_cols.split(",")])

    columns_to_extract: List[Tuple[str, List[str]]] = [
        ("MLExplorer", ["mlexplorer", "ml explorer", "matterlabs explorer", "ml explorer url"]),
        ("AltExplorer", ["altexplorer", "alt explorer", "alternative explorer", "alt explorer url"]),
        ("Portal", ["portal", "website", "homepage", "docs", "portal url"]),
        ("HTTPS RPC", ["https rpc", "rpc", "http rpc", "https rpc url", "endpoint"]),
    ]

    # Map found normalized header → display label + original header name
    found_cols: List[Tuple[str, str]] = []  # (label, original_header)
    for label, aliases in columns_to_extract:
        col = find_column(header_map, aliases + [label])
        if col:
            found_cols.append((label, col))

    if chain_id_col is None and not found_cols:
        raise SystemExit("Could not find any of: chain_id / explorers / portal / https rpc in CSV headers")

    out: List[Dict[str, str]] = []
    dedupe: set = set()

    live_value_norm = normalize_header(args.live_value)
    status_missing_warned = False

    for row in rows:
        # Status filter (if column present). If missing, assume all rows are eligible.
        if key_exists(row, status_col):
            status_val = next((row[k] for k in row if normalize_header(k) == normalize_header(status_col)), "")
            if normalize_header(str(status_val)) != live_value_norm:
                continue
        else:
            if not status_missing_warned:
                print(f"[warn] Status column '{status_col}' not found; keeping all rows")
                status_missing_warned = True

        # Optional nice name
        chain_name = None
        if name_col and key_exists(row, name_col):
            chain_name = next((row[k] for k in row if normalize_header(k) == normalize_header(name_col)), None)
            if chain_name:
                chain_name = chain_name.strip() or None

        # chain_id entry
        if chain_id_col and key_exists(row, chain_id_col):
            raw_id = next((row[k] for k in row if normalize_header(k) == normalize_header(chain_id_col)), "").strip()
            if raw_id:
                # Keep numeric-looking piece (e.g., "1" from "1 (mainnet)")
                m = re.search(r"\d+", raw_id)
                cid = m.group(0) if m else raw_id
                url = f"https://chainlist.org/chain/{cid}"
                desc = f"Chain ID {cid}" + (f" — {chain_name}" if chain_name else "")
                entry = {
                    "key": "chain_id",
                    "value": cid,
                    "description": desc,
                    "url": url,
                }
                key_d = (entry["key"], entry["value"], entry["url"]) 
                if key_d not in dedupe:
                    out.append(entry); dedupe.add(key_d)

        # URL-ish columns
        for label, col in found_cols:
            cell = next((row[k] for k in row if normalize_header(k) == normalize_header(col)), "")
            if not cell:
                continue
            urls = split_urls(str(cell))
            for u in urls:
                if not u:
                    continue
                # If missing scheme, don't force add; keep as-is for display and link.
                desc = f"{label} for chain" + (f" {chain_name}" if chain_name else "")
                entry = {
                    "key": label,
                    "value": u,
                    "description": desc,
                    "url": u,
                }
                key_d = (entry["key"], entry["value"], entry["url"]) 
                if key_d not in dedupe:
                    out.append(entry); dedupe.add(key_d)

    # Stable sort: by key then value
    out.sort(key=lambda r: (r["key"].lower(), r["value"].lower()))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[ok] Wrote {args.out} — {len(out)} records")


if __name__ == "__main__":
    main()
