# run example:
# python init_ai_tables.py "..\commentary.db"

#!/usr/bin/env python3
"""
init_ai_tables.py

Creates tables for storing AI-extracted commentary per verse, with paragraph citations.

Writes into your existing commentary.db (does not touch bible.db).
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


DDL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS ai_extractions (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at      TEXT NOT NULL,                    -- ISO timestamp
  model           TEXT NOT NULL,
  prompt_version  TEXT NOT NULL,
  book_osis       TEXT NOT NULL,
  chapter         INTEGER NOT NULL,
  verse_start     INTEGER NOT NULL,
  verse_end       INTEGER NOT NULL,
  has_commentary  INTEGER NOT NULL,                 -- 0/1
  summary_en      TEXT NOT NULL,
  summary_zh      TEXT NOT NULL,                    -- can be empty
  bullet_points_en TEXT NOT NULL,                   -- JSON string
  bullet_points_zh TEXT NOT NULL,                   -- JSON string
  cited_para_ids  TEXT NOT NULL,                    -- JSON string of ints
  raw_json        TEXT NOT NULL,                    -- full model output JSON
  status          TEXT NOT NULL,                    -- ok|skipped_no_verse|refused|error
  error           TEXT,
  UNIQUE(model, prompt_version, book_osis, chapter, verse_start, verse_end)
);

CREATE INDEX IF NOT EXISTS idx_ai_extractions_ref
  ON ai_extractions(book_osis, chapter, verse_start, verse_end);

CREATE TABLE IF NOT EXISTS ai_extraction_citations (
  extraction_id   INTEGER NOT NULL,
  para_id         INTEGER NOT NULL,
  reason          TEXT NOT NULL,
  PRIMARY KEY (extraction_id, para_id),
  FOREIGN KEY(extraction_id) REFERENCES ai_extractions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ai_citations_para
  ON ai_extraction_citations(para_id);
"""


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python init_ai_tables.py <path-to-commentary.db>")
        return 2

    db_path = Path(sys.argv[1]).resolve()
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(DDL)
        con.commit()
    finally:
        con.close()

    print(f"OK: initialized AI tables in {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
