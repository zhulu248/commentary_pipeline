# run example:
# python init_bible_db.py "..\bible.db"

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS bible_versions (
  version TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  source TEXT,
  license TEXT
);

CREATE TABLE IF NOT EXISTS verses (
  version TEXT NOT NULL,
  book_osis TEXT NOT NULL,
  chapter INTEGER NOT NULL,
  verse INTEGER NOT NULL,
  text TEXT NOT NULL,
  PRIMARY KEY (version, book_osis, chapter, verse)
);

CREATE INDEX IF NOT EXISTS idx_verses_lookup
  ON verses(version, book_osis, chapter, verse);
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db", help="Path to bible.db")
    args = ap.parse_args()

    db_path = Path(args.db).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(db_path)
    try:
        con.executescript(DDL)
        con.commit()
    finally:
        con.close()

    print(f"OK: initialized {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
