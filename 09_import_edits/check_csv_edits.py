#!/usr/bin/env python3
"""
Quickly scan a commentary edits CSV to see whether any rows were edited.

Outputs the total edited row count and previews the first few edited entries.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


DESCRIPTION = """Check whether a commentary edits CSV has any edited rows.\n\nExample (copy/paste):\n  python 09_import_edits/check_csv_edits.py ./08_export_commentary/commentary_draft.csv\n"""

EXAMPLES = """Examples (copy/paste ready from repo root):
  # Check the default draft CSV exported earlier
  python 09_import_edits/check_csv_edits.py ./08_export_commentary/commentary_draft.csv

  # Check a specific file and preview the first 5 edited rows
  python 09_import_edits/check_csv_edits.py ./my_edits.csv --max-preview 5
"""


EDIT_FIELDS = ["keep_drop", "my_edit_en", "my_edit_zh", "notes"]


def iter_hits(csv_path: Path) -> Iterable[tuple[int, list[str]]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            values = [(row.get(col) or "").strip() for col in EDIT_FIELDS]
            if any(values):
                preview = values[:]
                preview[1] = preview[1][:80]
                preview[2] = preview[2][:80]
                preview[3] = preview[3][:80]
                ref = (row.get("ref") or "").strip()
                book = (row.get("book_osis") or "").strip()
                chapter = (row.get("chapter") or "").strip()
                yield (i, [ref, book, chapter] + preview)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLES,
    )
    ap.add_argument("csv", help="Path to a commentary_draft CSV with review edits.")
    ap.add_argument("--max-preview", type=int, default=20, help="Number of edited rows to print (default: 20)")
    args = ap.parse_args()

    csv_path = Path(args.csv).expanduser().resolve()
    if not csv_path.exists():
        raise SystemExit(f"File not found: {csv_path}")

    hits = list(iter_hits(csv_path))
    print("edited_rows_found =", len(hits))

    for row_num, parts in hits[: args.max_preview]:
        ref, book, chapter, keep_drop, edit_en, edit_zh, notes = parts
        print(
            f"line={row_num} ref={ref or book + ' ' + chapter}: keep_drop={keep_drop or '-'} | "
            f"en={edit_en or '-'} | zh={edit_zh or '-'} | notes={notes or '-'}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
