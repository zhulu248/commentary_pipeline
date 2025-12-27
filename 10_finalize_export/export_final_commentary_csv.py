# run example:
# python export_final_commentary_csv.py --commentary-db "..\commentary.db" --bible-db "..\bible.db" --out "final_commentary.csv" --only-status ok --kjv KJV --cuv CUVS

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
export_final_commentary_csv.py

Exports a merged "final" commentary draft:
- Starts from ai_extractions (filtered by status)
- Left-joins commentary_edits (your manual edits)
- Pulls Bible verse text from bible.db for KJV + CUVS (configurable)
- Produces a CSV suitable for review / import / publishing

Rules:
- If an edit exists and keep_drop is "drop" -> exclude that row
- If my_edit_en / my_edit_zh is non-empty -> it overrides the AI fields
- Otherwise uses summary_en/summary_zh (fallback to bullet_points_* if summary empty)
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


DROP_WORDS = {"drop", "skip", "0", "no"}
KEEP_WORDS = {"keep", "1", "yes"}


def norm(s) -> str:
    return (s or "").strip()


def pick_text(edit_text: str, ai_summary: str, ai_bullets: str) -> str:
    et = norm(edit_text)
    if et:
        return et
    s = norm(ai_summary)
    if s:
        return s
    return norm(ai_bullets)


DESCRIPTION = """Export final merged commentary to CSV.\n\nExample (copy/paste):\n  python 10_finalize_export/export_final_commentary_csv.py --commentary-db ./commentary.db --bible-db ./bible.db --out final_commentary.csv --only-status ok\n"""

EXAMPLES = """Examples (copy/paste ready from repo root):
  # Export all OK rows to final_commentary.csv in the project root
  python 10_finalize_export/export_final_commentary_csv.py \
    --commentary-db ./commentary.db --bible-db ./bible.db --out final_commentary.csv --only-status ok

  # Export everything (no status filter) to a custom folder and use different Bible versions
  python 10_finalize_export/export_final_commentary_csv.py \
    --commentary-db ./commentary.db --bible-db ./bible.db --out ./out/final_commentary_full.csv \
    --only-status "" --kjv NKJV --cuv CUVS
"""


def main() -> int:
    ap = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLES,
    )
    ap.add_argument("--commentary-db", required=True)
    ap.add_argument("--bible-db", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--only-status", default="ok", help="Filter ai_extractions.status (default: ok). Use empty to export all.")
    ap.add_argument("--kjv", default="KJV", help="Bible version code for English (default: KJV)")
    ap.add_argument("--cuv", default="CUVS", help="Bible version code for Chinese (default: CUVS)")
    args = ap.parse_args()

    commentary_db = Path(args.commentary_db).resolve()
    bible_db = Path(args.bible_db).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(commentary_db))
    con.row_factory = sqlite3.Row
    try:
        con.execute("ATTACH DATABASE ? AS bible;", (str(bible_db),))

        where_status = ""
        params = [args.kjv, args.cuv]
        if norm(args.only_status):
            where_status = "WHERE ax.status = ?"
            params.append(args.only_status)

        q = f"""
        SELECT
          ax.model,
          ax.prompt_version,
          ax.book_osis,
          ax.chapter,
          ax.verse_start,
          ax.verse_end,
          ax.has_commentary,
          ax.summary_en,
          ax.summary_zh,
          ax.bullet_points_en,
          ax.bullet_points_zh,
          ax.cited_para_ids,
          ax.status,

          ce.keep_drop AS edit_keep_drop,
          ce.my_edit_en,
          ce.my_edit_zh,
          ce.notes,

          kjv.text AS kjv_text,
          cuv.text AS cuv_text

        FROM ai_extractions ax

        LEFT JOIN commentary_edits ce
          ON ce.model = ax.model
         AND ce.prompt_version = ax.prompt_version
         AND ce.book_osis = ax.book_osis
         AND ce.chapter = ax.chapter
         AND ce.verse_start = ax.verse_start
         AND ce.verse_end = ax.verse_end

        LEFT JOIN bible.verses kjv
          ON kjv.version = ?
         AND kjv.book_osis = ax.book_osis
         AND kjv.chapter = ax.chapter
         AND kjv.verse = ax.verse_start

        LEFT JOIN bible.verses cuv
          ON cuv.version = ?
         AND cuv.book_osis = ax.book_osis
         AND cuv.chapter = ax.chapter
         AND cuv.verse = ax.verse_start

        {where_status}
        ORDER BY ax.book_osis, ax.chapter, ax.verse_start, ax.verse_end;
        """

        rows = con.execute(q, params).fetchall()

        with out_path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

            w.writerow([
                "ref",
                "model", "prompt_version",
                "book_osis", "chapter", "verse_start", "verse_end",
                "kjv_text", "cuv_text",
                "final_en", "final_zh",
                "has_commentary",
                "keep_drop",
                "notes",
                "status",
                "cited_para_ids",
            ])

            kept = 0
            dropped = 0

            for r in rows:
                kd = norm(r["edit_keep_drop"]).lower()
                if kd in DROP_WORDS:
                    dropped += 1
                    continue

                ref = f'{r["book_osis"]} {r["chapter"]}:{r["verse_start"]}' + (
                    f'-{r["verse_end"]}' if int(r["verse_end"]) != int(r["verse_start"]) else ""
                )

                final_en = pick_text(r["my_edit_en"], r["summary_en"], r["bullet_points_en"])
                final_zh = pick_text(r["my_edit_zh"], r["summary_zh"], r["bullet_points_zh"])

                keep_drop_out = norm(r["edit_keep_drop"])  # keep as user wrote (keep/drop/blank)

                w.writerow([
                    ref,
                    r["model"], r["prompt_version"],
                    r["book_osis"], r["chapter"], r["verse_start"], r["verse_end"],
                    norm(r["kjv_text"]),
                    norm(r["cuv_text"]),
                    final_en,
                    final_zh,
                    r["has_commentary"],
                    keep_drop_out,
                    norm(r["notes"]),
                    r["status"],
                    norm(r["cited_para_ids"]),
                ])
                kept += 1

        print(f"OK: wrote {kept} row(s) -> {out_path}")
        print(f"Info: dropped by keep_drop=drop: {dropped}")
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
