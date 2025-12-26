# run example:
# python export_review_queue.py --commentary-db "..\commentary.db" --bible-db "..\bible.db" --out "review_queue.csv" --limit 0

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


def table_cols(con: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in con.execute(f"pragma table_info({table});").fetchall()]


def pick_existing(cols: list[str], candidates: list[str]) -> str | None:
    s = set(cols)
    for c in candidates:
        if c in s:
            return c
    return None


def pick_id_col(cols: list[str]) -> str:
    # common PK names
    for c in ("id", "ID", "pk", "row_id"):
        if c in cols:
            return c
    # sqlite implicit rowid works if table wasn't created WITHOUT ROWID
    return "rowid"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Export a review queue by joining verse mentions with KJV+CUVS verse text and source paragraphs."
    )
    ap.add_argument("--commentary-db", required=True, help="Path to commentary.db")
    ap.add_argument("--bible-db", required=True, help="Path to bible.db")
    ap.add_argument("--out", default="review_queue.csv", help="Output CSV path")
    ap.add_argument("--limit", type=int, default=0, help="0 = no limit")
    ap.add_argument("--only-status", default="ok", help="Filter verse_mentions.parse_status (default: ok)")
    args = ap.parse_args()

    cdb = Path(args.commentary_db).resolve()
    bdb = Path(args.bible_db).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(cdb)
    con.execute("ATTACH DATABASE ? AS bible", (str(bdb),))

    # ---- Detect schema ----
    vm_cols = table_cols(con, "verse_mentions")
    p_cols = table_cols(con, "paragraphs")
    d_cols = table_cols(con, "documents")

    vm_id = pick_id_col(vm_cols)
    p_id = pick_id_col(p_cols)
    d_id = pick_id_col(d_cols)

    # verse_mentions -> paragraphs FK (this is what broke for you)
    vm_para_fk = pick_existing(
        vm_cols,
        ["para_id", "paragraph_id", "p_id", "paragraphs_id", "paragraph_rowid", "para_rowid", "p_rowid"],
    )
    if not vm_para_fk:
        # Last resort: maybe verse_mentions stores paragraph primary key under a generic name
        vm_para_fk = pick_existing(vm_cols, ["ref_id", "foreign_id", "parent_id"])

    # paragraphs -> documents FK
    p_doc_fk = pick_existing(p_cols, ["doc_id", "document_id", "d_id", "documents_id"])

    # paragraph order column (optional)
    p_order_col = pick_existing(
        p_cols,
        ["para_index", "paragraph_index", "p_index", "idx", "position", "ord", "seq", "para_no", "para_num"],
    ) or p_id

    # Required columns in verse_mentions (we know these exist from your earlier queries)
    raw_match_col = pick_existing(vm_cols, ["raw_match", "raw", "match", "raw_ref"]) or "raw_match"
    book_col = pick_existing(vm_cols, ["book_osis", "book"]) or "book_osis"
    ch_col = pick_existing(vm_cols, ["chapter", "chap"]) or "chapter"
    v1_col = pick_existing(vm_cols, ["verse_start", "verse", "vstart"]) or "verse_start"
    v2_col = pick_existing(vm_cols, ["verse_end", "vend"]) or "verse_end"
    status_col = pick_existing(vm_cols, ["parse_status", "status"]) or "parse_status"

    # paragraph text col
    p_text_col = pick_existing(p_cols, ["text", "para_text", "content"]) or "text"

    # document cols
    d_title_col = pick_existing(d_cols, ["title", "doc_title"]) or "title"
    d_source_col = pick_existing(d_cols, ["source", "url", "doc_source"]) or "source"
    d_type_col = pick_existing(d_cols, ["doc_type", "type"]) or "doc_type"
    d_extracted_col = pick_existing(d_cols, ["extracted_at", "created_at", "timestamp"]) or "extracted_at"

    # ---- Validate joins; if missing, degrade gracefully ----
    join_paras = vm_para_fk is not None and (p_text_col in p_cols)
    join_docs = join_paras and (p_doc_fk is not None)

    limit_sql = f"LIMIT {args.limit}" if args.limit and args.limit > 0 else ""

    # Build SELECT list depending on what we can join
    select_cols = [
        f"vm.{vm_id} AS mention_id",
        f"vm.{raw_match_col} AS raw_match",
        f"vm.{book_col} AS book_osis",
        f"vm.{ch_col} AS chapter",
        f"vm.{v1_col} AS verse_start",
        f"vm.{v2_col} AS verse_end",
        f"vm.{status_col} AS parse_status",
    ]

    if join_docs:
        select_cols += [
            f"d.{d_id} AS doc_id",
            f"d.{d_type_col} AS doc_type",
            f"d.{d_title_col} AS doc_title",
            f"d.{d_source_col} AS doc_source",
            f"d.{d_extracted_col} AS doc_extracted_at",
        ]
    else:
        select_cols += [
            "NULL AS doc_id",
            "NULL AS doc_type",
            "NULL AS doc_title",
            "NULL AS doc_source",
            "NULL AS doc_extracted_at",
        ]

    if join_paras:
        select_cols += [
            f"p.{p_id} AS para_id",
            f"p.{p_order_col} AS para_order",
            f"p.{p_text_col} AS para_text",
        ]
    else:
        select_cols += [
            "NULL AS para_id",
            "NULL AS para_order",
            "NULL AS para_text",
        ]

    # Bible joins (use verse_start as lookup verse)
    select_cols += [
        "kjv.text AS kjv_text",
        "cuvs.text AS cuvs_text",
    ]

    from_and_joins = ["FROM verse_mentions vm"]

    if join_paras:
        # handle possible rowid FK naming
        if vm_para_fk.endswith("rowid"):
            from_and_joins.append(f"JOIN paragraphs p ON p.rowid = vm.{vm_para_fk}")
        else:
            from_and_joins.append(f"JOIN paragraphs p ON p.{p_id} = vm.{vm_para_fk}")

    if join_docs:
        from_and_joins.append(f"JOIN documents d ON d.{d_id} = p.{p_doc_fk}")

    # Always join bible DB, even if paragraphs/docs missing
    from_and_joins.append(
        f"""LEFT JOIN bible.verses kjv
              ON kjv.version='KJV'
             AND kjv.book_osis = vm.{book_col}
             AND kjv.chapter   = vm.{ch_col}
             AND kjv.verse     = vm.{v1_col}"""
    )
    from_and_joins.append(
        f"""LEFT JOIN bible.verses cuvs
              ON cuvs.version='CUVS'
             AND cuvs.book_osis = vm.{book_col}
             AND cuvs.chapter   = vm.{ch_col}
             AND cuvs.verse     = vm.{v1_col}"""
    )

    order_by = []
    if join_docs:
        order_by.append(f"d.{d_id}")
    if join_paras:
        order_by.append(f"p.{p_order_col}")
    order_by.append(f"vm.{vm_id}")

    q = f"""
    SELECT
      {", ".join(select_cols)}
    {' '.join(from_and_joins)}
    WHERE vm.{status_col} = ?
      AND vm.{ch_col} IS NOT NULL
      AND vm.{v1_col} IS NOT NULL
    ORDER BY {", ".join(order_by)}
    {limit_sql}
    """

    # ---- Execute ----
    rows = con.execute(q, (args.only_status,))

    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow([
            "mention_id", "raw_match", "book_osis", "chapter", "verse_start", "verse_end", "parse_status",
            "doc_id", "doc_type", "doc_title", "doc_source", "doc_extracted_at",
            "para_id", "para_order", "para_text",
            "kjv_text", "cuvs_text",
        ])
        n = 0
        for r in rows:
            w.writerow(r)
            n += 1

    con.close()

    print("OK: schema detected")
    print(f"  verse_mentions id: {vm_id}")
    print(f"  verse_mentions -> paragraphs FK: {vm_para_fk}")
    print(f"  paragraphs id: {p_id}")
    print(f"  paragraphs order col: {p_order_col}")
    print(f"  paragraphs -> documents FK: {p_doc_fk}")
    print(f"  joins enabled: paragraphs={join_paras}, documents={join_docs}")
    print(f"OK: wrote {n} rows -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
