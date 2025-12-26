# run example:
# python export_grouped_by_verse.py --commentary-db "..\commentary.db" --bible-db "..\bible.db" --out "grouped_by_verse.csv" --limit 0 --max-paras-per-verse 10

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


def fetch_range_text(
    con: sqlite3.Connection,
    version: str,
    book: str,
    chapter: int,
    v1: int,
    v2: int,
) -> str:
    """
    Fetch verse text for [v1..v2] (inclusive) from bible.verses and concatenate.
    If some verses are missing, returns whatever is available.
    """
    if v2 < v1:
        v1, v2 = v2, v1
    cur = con.execute(
        """
        SELECT verse, text
        FROM bible.verses
        WHERE version=? AND book_osis=? AND chapter=? AND verse BETWEEN ? AND ?
        ORDER BY verse
        """,
        (version, book, chapter, v1, v2),
    )
    parts = []
    for verse_no, text in cur.fetchall():
        text = (text or "").strip()
        if text:
            parts.append(text)
    return " ".join(parts).strip()


def make_ref(book: str, chapter: int, v1: int, v2: int | None) -> str:
    if v2 is None or v2 == v1:
        return f"{book} {chapter}:{v1}"
    return f"{book} {chapter}:{v1}-{v2}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Export verse-centric grouped review CSV (one row per verse/range).")
    ap.add_argument("--commentary-db", required=True, help="Path to commentary.db")
    ap.add_argument("--bible-db", required=True, help="Path to bible.db")
    ap.add_argument("--out", default="grouped_by_verse.csv", help="Output CSV path")
    ap.add_argument("--limit", type=int, default=0, help="0 = no limit; otherwise limit mention rows scanned")
    ap.add_argument("--only-status", default="ok", help="Filter verse_mentions.parse_status (default: ok)")
    ap.add_argument("--max-paras-per-verse", type=int, default=10, help="Cap paragraphs stored per verse group")
    ap.add_argument("--max-para-chars", type=int, default=800, help="Trim each paragraph snippet to this many chars")
    args = ap.parse_args()

    cdb = Path(args.commentary_db).resolve()
    bdb = Path(args.bible_db).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(cdb)
    con.execute("ATTACH DATABASE ? AS bible", (str(bdb),))

    limit_sql = f"LIMIT {args.limit}" if args.limit and args.limit > 0 else ""

    # We assume these columns exist now (they do in your working pipeline):
    # verse_mentions.para_id, paragraphs.p_index, paragraphs.doc_id, documents.source/title, etc.
    q = f"""
    SELECT
      vm.book_osis,
      vm.chapter,
      vm.verse_start,
      vm.verse_end,

      d.id           AS doc_id,
      d.title        AS doc_title,
      d.source       AS doc_source,

      p.id           AS para_id,
      p.p_index      AS p_index,
      p.text         AS para_text
    FROM verse_mentions vm
    JOIN paragraphs p ON p.id = vm.para_id
    JOIN documents  d ON d.id = p.doc_id
    WHERE vm.parse_status = ?
      AND vm.book_osis IS NOT NULL
      AND vm.chapter IS NOT NULL
      AND vm.verse_start IS NOT NULL
    ORDER BY vm.book_osis, vm.chapter, vm.verse_start, COALESCE(vm.verse_end, vm.verse_start),
             d.id, p.p_index, vm.id
    {limit_sql}
    """

    rows = con.execute(q, (args.only_status,))

    # Group key includes verse_end so ranges are grouped cleanly.
    # key = (book, chapter, v1, v2_norm)
    groups: Dict[Tuple[str, int, int, int], Dict] = {}

    def get_group(book: str, ch: int, v1: int, v2: int) -> Dict:
        key = (book, ch, v1, v2)
        if key not in groups:
            groups[key] = {
                "book": book,
                "chapter": ch,
                "v1": v1,
                "v2": v2,
                "doc_sources": set(),
                "doc_titles": set(),
                "items": [],  # list of (doc_source, doc_title, para_id, p_index, para_text)
            }
        return groups[key]

    for book, ch, v1, v2, doc_id, doc_title, doc_source, para_id, p_index, para_text in rows:
        book = (book or "").strip()
        if not book:
            continue
        ch = int(ch)
        v1 = int(v1)
        v2n = int(v2) if v2 is not None else v1

        g = get_group(book, ch, v1, v2n)

        ds = (doc_source or "").strip()
        dt = (doc_title or "").strip()
        if ds:
            g["doc_sources"].add(ds)
        if dt:
            g["doc_titles"].add(dt)

        text = (para_text or "").strip().replace("\r", " ").replace("\n", " ")
        if args.max_para_chars and len(text) > args.max_para_chars:
            text = text[: args.max_para_chars].rstrip() + " …"

        # keep items capped
        if len(g["items"]) < args.max_paras_per_verse:
            g["items"].append((ds, dt, para_id, p_index, text))

    # Write CSV
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow([
            "ref",
            "book_osis", "chapter", "verse_start", "verse_end",
            "kjv_text", "cuvs_text",
            "mention_groups_count",
            "source_count",
            "sources",
            "evidence",  # compact multi-line cell
        ])

        # Stable ordering
        keys_sorted = sorted(groups.keys(), key=lambda k: (k[0], k[1], k[2], k[3]))

        for (book, ch, v1, v2n) in keys_sorted:
            g = groups[(book, ch, v1, v2n)]

            ref = make_ref(book, ch, v1, v2n)

            kjv = fetch_range_text(con, "KJV", book, ch, v1, v2n)
            cuvs = fetch_range_text(con, "CUVS", book, ch, v1, v2n)

            sources_sorted = sorted(s for s in g["doc_sources"] if s)
            evidence_lines: List[str] = []
            for (ds, dt, para_id, p_index, text) in g["items"]:
                label = dt or ds or ""
                if label:
                    evidence_lines.append(f"- {label} (para_id={para_id}, p_index={p_index}): {text}")
                else:
                    evidence_lines.append(f"- (para_id={para_id}, p_index={p_index}): {text}")

            w.writerow([
                ref,
                book, ch, v1, v2n,
                kjv, cuvs,
                1,
                len(sources_sorted),
                " | ".join(sources_sorted),
                "\n".join(evidence_lines),
            ])

    con.close()
    print(f"OK: wrote {len(groups)} verse group row(s) -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
