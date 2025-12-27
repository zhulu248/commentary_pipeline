# run example:
# python export_ai_commentary_draft_xlsx.py --commentary-db "..\commentary.db" --bible-db "..\bible.db" --out "commentary_draft.xlsx" --only-status ok --limit 0

# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
export_ai_commentary_draft_xlsx.py

Export ai_extractions into an Excel .xlsx file for safe manual editing in Excel.
This version auto-detects ai_extractions schema differences (e.g., missing "evidence").

Requires:
  pip install openpyxl
"""

import argparse
import sqlite3
from pathlib import Path

from openpyxl import Workbook


DESCRIPTION = """Export AI commentary draft to XLSX for manual review.\n\nExample (copy/paste):\n  python 08_export_commentary/export_ai_commentary_draft_xlsx.py --commentary-db ./commentary.db --bible-db ./bible.db --out commentary_draft.xlsx\n"""

EXAMPLES = """Examples (copy/paste ready from repo root):
  # Export all ai_extractions rows to Excel
  python 08_export_commentary/export_ai_commentary_draft_xlsx.py \
    --commentary-db ./commentary.db --bible-db ./bible.db --out commentary_draft.xlsx

  # Limit to 100 rows that have commentary before sharing for review
  python 08_export_commentary/export_ai_commentary_draft_xlsx.py \
    --commentary-db ./commentary.db --bible-db ./bible.db \
    --only-status ok --limit 100 --out ./out/commentary_draft_top100.xlsx
"""


def get_table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info({table});").fetchall()
    # rows: cid, name, type, notnull, dflt_value, pk
    return {r[1] for r in rows}


def verse_range_text(con: sqlite3.Connection, version: str, book: str, ch: int, v1: int, v2: int) -> str:
    if v2 < v1:
        v1, v2 = v2, v1
    rows = con.execute(
        """
        SELECT verse, text
        FROM verses
        WHERE version=? AND book_osis=? AND chapter=? AND verse BETWEEN ? AND ?
        ORDER BY verse
        """,
        (version, book, ch, v1, v2),
    ).fetchall()

    parts = []
    for _, txt in rows:
        t = (txt or "").strip()
        if t:
            parts.append(t)
    return " ".join(parts)


def pick_optional(cols: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return None


def main() -> int:
    ap = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLES,
    )
    ap.add_argument("--commentary-db", required=True)
    ap.add_argument("--bible-db", required=True)
    ap.add_argument("--out", default="commentary_draft.xlsx")
    ap.add_argument("--only-status", default="ok")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    cdb = Path(args.commentary_db).resolve()
    bdb = Path(args.bible_db).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con_c = sqlite3.connect(str(cdb))
    con_c.row_factory = sqlite3.Row
    con_b = sqlite3.connect(str(bdb))
    con_b.row_factory = sqlite3.Row

    try:
        cols = get_table_columns(con_c, "ai_extractions")

        # Required-ish fields (we’ll fail loudly if missing)
        required = ["model", "prompt_version", "book_osis", "chapter", "verse_start", "verse_end"]
        missing_required = [c for c in required if c not in cols]
        if missing_required:
            raise SystemExit(f"ERROR: ai_extractions missing required column(s): {missing_required}")

        has_commentary_col = pick_optional(cols, ["has_commentary"])
        summary_en_col = pick_optional(cols, ["summary_en", "summary"])
        summary_zh_col = pick_optional(cols, ["summary_zh"])
        # evidence varies a lot across versions; support a few names:
        evidence_col = pick_optional(cols, ["evidence", "evidence_text", "evidence_json", "citations", "sources"])

        # Build SELECT with safe fallbacks so row keys always exist
        select_parts = [
            "model",
            "prompt_version",
            "book_osis",
            "chapter",
            "verse_start",
            "verse_end",
            (f"{has_commentary_col} AS has_commentary" if has_commentary_col else "0 AS has_commentary"),
            (f"{summary_en_col} AS summary_en" if summary_en_col else "'' AS summary_en"),
            (f"{summary_zh_col} AS summary_zh" if summary_zh_col else "'' AS summary_zh"),
            (f"{evidence_col} AS evidence" if evidence_col else "'' AS evidence"),
        ]

        q = f"""
        SELECT {", ".join(select_parts)}
        FROM ai_extractions
        WHERE status=?
        ORDER BY book_osis, chapter, verse_start, verse_end
        """

        rows = con_c.execute(q, (args.only_status,)).fetchall()
        if args.limit and args.limit > 0:
            rows = rows[: args.limit]

        wb = Workbook()
        ws = wb.active
        ws.title = "draft"

        headers = [
            "model",
            "prompt_version",
            "ref",
            "book_osis",
            "chapter",
            "verse_start",
            "verse_end",
            "kjv_text",
            "cuvs_text",
            "has_commentary",
            "summary_en",
            "summary_zh",
            "evidence",
            # editable:
            "keep_drop",
            "my_edit_en",
            "my_edit_zh",
            "notes",
        ]
        ws.append(headers)

        for r in rows:
            book = (r["book_osis"] or "").strip()
            ch = int(r["chapter"])
            v1 = int(r["verse_start"])
            v2 = int(r["verse_end"])

            ref = f"{book} {ch}:{v1}" if v1 == v2 else f"{book} {ch}:{v1}-{v2}"
            kjv = verse_range_text(con_b, "KJV", book, ch, v1, v2)
            cuvs = verse_range_text(con_b, "CUVS", book, ch, v1, v2)

            ws.append(
                [
                    r["model"],
                    r["prompt_version"],
                    ref,
                    book,
                    ch,
                    v1,
                    v2,
                    kjv,
                    cuvs,
                    int(r["has_commentary"] or 0),
                    r["summary_en"] or "",
                    r["summary_zh"] or "",
                    r["evidence"] or "",
                    "",  # keep_drop
                    "",  # my_edit_en
                    "",  # my_edit_zh
                    "",  # notes
                ]
            )

        wb.save(out_path)
        print(f"OK: wrote {len(rows)} row(s) -> {out_path}")
        return 0

    finally:
        con_c.close()
        con_b.close()


if __name__ == "__main__":
    raise SystemExit(main())
