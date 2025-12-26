# run example:
# python import_commentary_edits.py --commentary-db "..\commentary.db" --csv "..\08_export_commentary\commentary_draft.csv" --reset

#!/usr/bin/env python3
"""
import_commentary_edits.py

Imports your manual edits from the exported commentary_draft.csv back into commentary.db.

- Does NOT overwrite ai_extractions.
- Stores edits into a separate table keyed by verse reference + model/prompt_version.
- Robust to Excel quirks: blank rows, chapter like "3.0", etc.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


DDL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS commentary_edits (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  updated_at     TEXT NOT NULL,                 -- ISO timestamp (UTC via sqlite)
  model          TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  book_osis      TEXT NOT NULL,
  chapter        INTEGER NOT NULL,
  verse_start    INTEGER NOT NULL,
  verse_end      INTEGER NOT NULL,
  keep_drop      TEXT NOT NULL,                 -- keep|drop| (blank allowed)
  my_edit_en     TEXT NOT NULL,
  my_edit_zh     TEXT NOT NULL,
  notes          TEXT NOT NULL,
  UNIQUE(model, prompt_version, book_osis, chapter, verse_start, verse_end)
);

CREATE INDEX IF NOT EXISTS idx_commentary_edits_ref
  ON commentary_edits(book_osis, chapter, verse_start, verse_end);
"""


def norm(s: str | None) -> str:
    return (s or "").strip()


def parse_int_maybe(s: str | None) -> int | None:
    """
    Accepts:
      "3", " 3 ", "3.0"
    Returns None for:
      "", None, "N/A"
    """
    t = norm(s)
    if not t:
        return None
    try:
        return int(t)
    except ValueError:
        try:
            # Excel sometimes writes integers as "3.0"
            return int(float(t))
        except Exception:
            return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Import manual edits from commentary_draft.csv into commentary.db")
    ap.add_argument("--commentary-db", required=True)
    ap.add_argument("--csv", required=True, help="Path to exported CSV (commentary_draft.csv)")
    ap.add_argument("--reset", action="store_true", help="Delete existing edits before importing")
    ap.add_argument("--verbose-bad-rows", action="store_true", help="Print details of rows skipped due to missing keys")
    args = ap.parse_args()

    db_path = Path(args.commentary_db).resolve()
    csv_path = Path(args.csv).resolve()
    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}")
        return 2

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    try:
        con.executescript(DDL)
        if args.reset:
            con.execute("DELETE FROM commentary_edits")
            con.commit()

        imported = 0
        skipped_blank_edit = 0
        skipped_missing_key = 0
        skipped_bad_key = 0

        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            required_cols = {
                "book_osis", "chapter", "verse_start", "verse_end",
                "model", "prompt_version",
                "keep_drop", "my_edit_en", "my_edit_zh", "notes",
            }
            missing = required_cols - set(reader.fieldnames or [])
            if missing:
                print("ERROR: CSV missing required columns:", ", ".join(sorted(missing)))
                return 2

            # DictReader has no stable "line number" unless we count ourselves.
            # Start at 2 because line 1 is the header.
            for row_idx, row in enumerate(reader, start=2):
                # 1) Skip completely empty rows / Excel artifacts
                if not row or all(not norm(v) for v in row.values()):
                    skipped_missing_key += 1
                    if args.verbose_bad_rows:
                        print(f"SKIP line {row_idx}: completely empty row")
                    continue

                # 2) Only import if user actually edited something
                keep_drop = norm(row.get("keep_drop"))
                my_en = norm(row.get("my_edit_en"))
                my_zh = norm(row.get("my_edit_zh"))
                notes = norm(row.get("notes"))

                if not (keep_drop or my_en or my_zh or notes):
                    skipped_blank_edit += 1
                    continue

                # 3) Parse the verse key fields (robustly)
                model = norm(row.get("model"))
                pv = norm(row.get("prompt_version"))
                book = norm(row.get("book_osis"))
                chapter = parse_int_maybe(row.get("chapter"))
                v1 = parse_int_maybe(row.get("verse_start"))
                v2 = parse_int_maybe(row.get("verse_end"))

                if not (model and pv and book):
                    skipped_missing_key += 1
                    if args.verbose_bad_rows:
                        print(f"SKIP line {row_idx}: missing model/prompt_version/book_osis")
                    continue

                if chapter is None or v1 is None or v2 is None:
                    skipped_missing_key += 1
                    if args.verbose_bad_rows:
                        print(
                            f"SKIP line {row_idx}: missing numeric key(s): "
                            f"chapter={row.get('chapter')!r}, verse_start={row.get('verse_start')!r}, verse_end={row.get('verse_end')!r}"
                        )
                    continue

                if chapter <= 0 or v1 <= 0 or v2 <= 0:
                    skipped_bad_key += 1
                    if args.verbose_bad_rows:
                        print(f"SKIP line {row_idx}: non-positive key(s): chapter={chapter}, v1={v1}, v2={v2}")
                    continue

                con.execute(
                    """
                    INSERT INTO commentary_edits(
                      updated_at, model, prompt_version,
                      book_osis, chapter, verse_start, verse_end,
                      keep_drop, my_edit_en, my_edit_zh, notes
                    ) VALUES (datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(model, prompt_version, book_osis, chapter, verse_start, verse_end)
                    DO UPDATE SET
                      updated_at=excluded.updated_at,
                      keep_drop=excluded.keep_drop,
                      my_edit_en=excluded.my_edit_en,
                      my_edit_zh=excluded.my_edit_zh,
                      notes=excluded.notes
                    """,
                    (model, pv, book, chapter, v1, v2, keep_drop, my_en, my_zh, notes),
                )
                imported += 1

        con.commit()
        print(
            "OK:"
            f" imported edits: {imported},"
            f" skipped blank-edit rows: {skipped_blank_edit},"
            f" skipped missing-key rows: {skipped_missing_key},"
            f" skipped bad-key rows: {skipped_bad_key}"
        )
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
