# run example:
# python import_kjv_db_to_bible_db.py --kjv-db "..\..\kjv.db" --bible-db "..\bible.db" --version KJV --name "King James Version" --reset-version

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

# Map common KJV book names -> OSIS
BOOKNAME_TO_OSIS = {
    "Genesis": "Gen", "Exodus": "Exod", "Leviticus": "Lev", "Numbers": "Num", "Deuteronomy": "Deut",
    "Joshua": "Josh", "Judges": "Judg", "Ruth": "Ruth", "1 Samuel": "1Sam", "2 Samuel": "2Sam",
    "1 Kings": "1Kgs", "2 Kings": "2Kgs", "1 Chronicles": "1Chr", "2 Chronicles": "2Chr",
    "Ezra": "Ezra", "Nehemiah": "Neh", "Esther": "Esth", "Job": "Job", "Psalms": "Ps",
    "Proverbs": "Prov", "Ecclesiastes": "Eccl", "Song of Solomon": "Song", "Song of Songs": "Song",
    "Isaiah": "Isa", "Jeremiah": "Jer", "Lamentations": "Lam", "Ezekiel": "Ezek", "Daniel": "Dan",
    "Hosea": "Hos", "Joel": "Joel", "Amos": "Amos", "Obadiah": "Obad", "Jonah": "Jonah",
    "Micah": "Mic", "Nahum": "Nah", "Habakkuk": "Hab", "Zephaniah": "Zeph", "Haggai": "Hag",
    "Zechariah": "Zech", "Malachi": "Mal",
    "Matthew": "Matt", "Mark": "Mark", "Luke": "Luke", "John": "John", "Acts": "Acts",
    "Romans": "Rom", "1 Corinthians": "1Cor", "2 Corinthians": "2Cor", "Galatians": "Gal",
    "Ephesians": "Eph", "Philippians": "Phil", "Colossians": "Col", "1 Thessalonians": "1Thess",
    "2 Thessalonians": "2Thess", "1 Timothy": "1Tim", "2 Timothy": "2Tim", "Titus": "Titus",
    "Philemon": "Phlm", "Hebrews": "Heb", "James": "Jas", "1 Peter": "1Pet", "2 Peter": "2Pet",
    "1 John": "1John", "2 John": "2John", "3 John": "3John", "Jude": "Jude", "Revelation": "Rev",
}


def main() -> int:
    ap = argparse.ArgumentParser(description="Import KJV_verses from kjv.db into bible.db (verses table).")
    ap.add_argument("--kjv-db", required=True, help="Path to kjv.db (source)")
    ap.add_argument("--bible-db", required=True, help="Path to bible.db (target)")
    ap.add_argument("--version", default="KJV")
    ap.add_argument("--name", default="King James Version")
    ap.add_argument("--source", default="local kjv.db (KJV_verses)")
    ap.add_argument("--license", default="Public Domain")
    ap.add_argument("--reset-version", action="store_true")
    args = ap.parse_args()

    kjv_db = Path(args.kjv_db).resolve()
    bible_db = Path(args.bible_db).resolve()

    src = sqlite3.connect(kjv_db)
    dst = sqlite3.connect(bible_db)
    dst.execute("PRAGMA journal_mode=WAL;")

    dst.execute(
        "INSERT OR REPLACE INTO bible_versions(version, name, source, license) VALUES (?,?,?,?)",
        (args.version, args.name, args.source, args.license),
    )
    dst.commit()

    if args.reset_version:
        dst.execute("DELETE FROM verses WHERE version=?", (args.version,))
        dst.commit()

    # Figure out schema: either KJV_verses has book_id referencing KJV_books, or it already has book name.
    tables = {r[0] for r in src.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

    if "KJV_verses" not in tables:
        raise SystemExit(f"ERROR: {kjv_db} does not contain table KJV_verses")

    if "KJV_books" in tables:
        # Join to get book name
        q = """
        SELECT b.name, v.chapter, v.verse, v.text
        FROM KJV_verses v
        JOIN KJV_books b ON b.id = v.book_id
        ORDER BY v.book_id, v.chapter, v.verse
        """
        rows = src.execute(q)
        def to_osis(book_name: str) -> str:
            return BOOKNAME_TO_OSIS.get(book_name.strip(), book_name.strip())
    else:
        # Assume KJV_verses has book column already
        q = """
        SELECT v.book, v.chapter, v.verse, v.text
        FROM KJV_verses v
        ORDER BY v.book, v.chapter, v.verse
        """
        rows = src.execute(q)
        def to_osis(book_name: str) -> str:
            return BOOKNAME_TO_OSIS.get(book_name.strip(), book_name.strip())

    batch = []
    inserted = 0

    for book_name, chapter, verse, text in rows:
        book_osis = to_osis(book_name)
        batch.append((args.version, book_osis, int(chapter), int(verse), str(text)))

        if len(batch) >= 5000:
            dst.executemany(
                "INSERT OR REPLACE INTO verses(version, book_osis, chapter, verse, text) VALUES (?,?,?,?,?)",
                batch,
            )
            dst.commit()
            inserted += len(batch)
            batch = []

    if batch:
        dst.executemany(
            "INSERT OR REPLACE INTO verses(version, book_osis, chapter, verse, text) VALUES (?,?,?,?,?)",
            batch,
        )
        dst.commit()
        inserted += len(batch)

    src.close()
    dst.close()

    print(f"OK: imported ~{inserted} KJV verse rows into {bible_db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
