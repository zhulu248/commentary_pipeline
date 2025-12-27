# run example:
# python init_commentary_db.py "..\commentary.db"

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

SCHEMA_SQL = r"""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS documents (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_type      TEXT NOT NULL,              -- book|article
  title         TEXT,
  author        TEXT,
  source        TEXT,
  extracted_at  TEXT,
  engine        TEXT,
  local_path    TEXT NOT NULL,              -- where the CPF file is on disk
  sha256        TEXT NOT NULL UNIQUE         -- for dedupe
);

CREATE TABLE IF NOT EXISTS paragraphs (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id    INTEGER NOT NULL,
  p_index   INTEGER NOT NULL,               -- 1-based (P000001 => 1)
  text      TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES documents(id) ON DELETE CASCADE,
  UNIQUE(doc_id, p_index)
);

CREATE INDEX IF NOT EXISTS idx_paragraphs_doc ON paragraphs(doc_id);

-- We'll add these later:
-- verse_mentions (regex-found scripture refs)
-- extracted_notes (LLM outputs)
"""

DESCRIPTION = """Initialize commentary.db schema.\n\nExample:\n  python 03_db/init_commentary_db.py ./commentary.db\n"""

EXAMPLES = """Examples (run from repo root):
  # Create commentary.db alongside the scripts
  python 03_db/init_commentary_db.py ./commentary.db

  # Create it in a sibling folder (the folder will be created if missing)
  python 03_db/init_commentary_db.py ../data/commentary.db
"""


def main() -> int:
    ap = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLES,
    )
    ap.add_argument("db", help="Path to SQLite DB (will be created if missing).")
    args = ap.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()

    print(f"OK: initialized {db_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
