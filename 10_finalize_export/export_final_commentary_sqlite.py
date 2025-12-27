# run example:
# python export_final_commentary_sqlite.py --commentary-db "..\commentary.db" --bible-db "..\bible.db" --out-db "..\publish.db" --only-status ok --kjv KJV --cuv CUVS --reset

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path
from typing import Dict, Tuple, List

DROP_WORDS = {"drop", "skip", "0", "no"}


# OSIS -> human-ish canonical English book names (used only as fallback keys)
OSIS_TO_ENGLISH = {
    "Gen": "Genesis", "Exod": "Exodus", "Lev": "Leviticus", "Num": "Numbers", "Deut": "Deuteronomy",
    "Josh": "Joshua", "Judg": "Judges", "Ruth": "Ruth",
    "1Sam": "1 Samuel", "2Sam": "2 Samuel",
    "1Kgs": "1 Kings", "2Kgs": "2 Kings",
    "1Chr": "1 Chronicles", "2Chr": "2 Chronicles",
    "Ezra": "Ezra", "Neh": "Nehemiah", "Esth": "Esther",
    "Job": "Job", "Ps": "Psalms", "Prov": "Proverbs", "Eccl": "Ecclesiastes", "Song": "Song of Solomon",
    "Isa": "Isaiah", "Jer": "Jeremiah", "Lam": "Lamentations", "Ezek": "Ezekiel", "Dan": "Daniel",
    "Hos": "Hosea", "Joel": "Joel", "Amos": "Amos", "Obad": "Obadiah", "Jonah": "Jonah", "Mic": "Micah",
    "Nah": "Nahum", "Hab": "Habakkuk", "Zeph": "Zephaniah", "Hag": "Haggai", "Zech": "Zechariah", "Mal": "Malachi",
    "Matt": "Matthew", "Mark": "Mark", "Luke": "Luke", "John": "John",
    "Acts": "Acts", "Rom": "Romans",
    "1Cor": "1 Corinthians", "2Cor": "2 Corinthians",
    "Gal": "Galatians", "Eph": "Ephesians", "Phil": "Philippians", "Col": "Colossians",
    "1Thess": "1 Thessalonians", "2Thess": "2 Thessalonians",
    "1Tim": "1 Timothy", "2Tim": "2 Timothy",
    "Titus": "Titus", "Phlm": "Philemon",
    "Heb": "Hebrews", "Jas": "James",
    "1Pet": "1 Peter", "2Pet": "2 Peter",
    "1John": "1 John", "2John": "2 John", "3John": "3 John",
    "Jude": "Jude", "Rev": "Revelation",
}

# A few extra common variants for “numbered” books (Roman numerals, spelled-out ordinals)
OSIS_NUMBERED_ALTS = {
    "1Sam": ["I Samuel", "First Samuel", "1st Samuel", "1Sam", "1 Samuel"],
    "2Sam": ["II Samuel", "Second Samuel", "2nd Samuel", "2Sam", "2 Samuel"],
    "1Kgs": ["I Kings", "First Kings", "1st Kings", "1Kgs", "1 Kings"],
    "2Kgs": ["II Kings", "Second Kings", "2nd Kings", "2Kgs", "2 Kings"],
    "1Chr": ["I Chronicles", "First Chronicles", "1st Chronicles", "1Chr", "1 Chronicles"],
    "2Chr": ["II Chronicles", "Second Chronicles", "2nd Chronicles", "2Chr", "2 Chronicles"],
    "1Cor": ["I Corinthians", "First Corinthians", "1st Corinthians", "1Cor", "1 Corinthians"],
    "2Cor": ["II Corinthians", "Second Corinthians", "2nd Corinthians", "2Cor", "2 Corinthians"],
    "1Thess": ["I Thessalonians", "First Thessalonians", "1 Thessalonians", "1Thess"],
    "2Thess": ["II Thessalonians", "Second Thessalonians", "2 Thessalonians", "2Thess"],
    "1Tim": ["I Timothy", "First Timothy", "1 Timothy", "1Tim"],
    "2Tim": ["II Timothy", "Second Timothy", "2 Timothy", "2Tim"],
    "1Pet": ["I Peter", "First Peter", "1 Peter", "1Pet"],
    "2Pet": ["II Peter", "Second Peter", "2 Peter", "2Pet"],
    "1John": ["I John", "First John", "1 John", "1John"],
    "2John": ["II John", "Second John", "2 John", "2John"],
    "3John": ["III John", "Third John", "3 John", "3John"],
}


def norm(s) -> str:
    return (s or "").strip()


def normalize_book_key(s: str) -> str:
    """
    Aggressively normalize book names/codes so that:
      'II Kings' -> 'iikings'
      '2 Kings'  -> '2kings'
      '2Kgs'     -> '2kgs'
    We will try multiple candidate keys.
    """
    s = (s or "").strip().lower()
    s = re.sub(r"[\s\.\-_/]+", "", s)        # drop common separators
    s = re.sub(r"[^a-z0-9]+", "", s)         # keep alnum only
    return s


def pick_text(edit_text: str, ai_summary: str, ai_bullets: str) -> str:
    et = norm(edit_text)
    if et:
        return et
    s = norm(ai_summary)
    if s:
        return s
    return norm(ai_bullets)


DESCRIPTION = """Export merged final commentary into a SQLite table for website use.\n\nExample:\n  python 10_finalize_export/export_final_commentary_sqlite.py --commentary-db ./commentary.db --bible-db ./bible.db --out-db ./publish.db --only-status ok --reset\n"""

EXAMPLES = """Examples (copy/paste ready from repo root):
  # Export OK rows into publish.db (used by the static viewer)
  python 10_finalize_export/export_final_commentary_sqlite.py \
    --commentary-db ./commentary.db --bible-db ./bible.db --out-db ./publish.db --only-status ok --reset

  # Append all statuses to a custom SQLite file without dropping existing rows
  python 10_finalize_export/export_final_commentary_sqlite.py \
    --commentary-db ./commentary.db --bible-db ./bible.db --out-db ./out/final_commentary.db \
    --only-status "" --kjv KJV --cuv CUVS
"""


def ensure_table(con_out: sqlite3.Connection, reset: bool) -> None:
    if reset:
        con_out.execute("DROP TABLE IF EXISTS final_commentary;")

    con_out.execute(
        """
        CREATE TABLE IF NOT EXISTS final_commentary (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at     TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at     TEXT NOT NULL DEFAULT (datetime('now')),

          ref            TEXT NOT NULL,
          book_osis      TEXT NOT NULL,
          chapter        INTEGER NOT NULL,
          verse_start    INTEGER NOT NULL,
          verse_end      INTEGER NOT NULL,

          kjv_version    TEXT NOT NULL,
          cuv_version    TEXT NOT NULL,
          kjv_text       TEXT NOT NULL DEFAULT '',
          cuv_text       TEXT NOT NULL DEFAULT '',

          model          TEXT NOT NULL,
          prompt_version TEXT NOT NULL,
          status         TEXT NOT NULL,
          has_commentary INTEGER NOT NULL,

          final_en       TEXT NOT NULL DEFAULT '',
          final_zh       TEXT NOT NULL DEFAULT '',
          keep_drop      TEXT NOT NULL DEFAULT '',
          notes          TEXT NOT NULL DEFAULT '',
          cited_para_ids TEXT NOT NULL DEFAULT '',

          UNIQUE(model, prompt_version, book_osis, chapter, verse_start, verse_end)
        );
        """
    )

    con_out.execute(
        "CREATE INDEX IF NOT EXISTS idx_final_ref ON final_commentary(book_osis, chapter, verse_start, verse_end);"
    )
    con_out.execute(
        "CREATE INDEX IF NOT EXISTS idx_final_model ON final_commentary(model, prompt_version);"
    )


def build_version_book_map(con: sqlite3.Connection, version: str) -> Dict[str, str]:
    """
    Builds a map: normalized_key -> actual book_osis stored in bible.verses for that version.
    """
    m: Dict[str, str] = {}
    rows = con.execute(
        "SELECT DISTINCT book_osis FROM bible.verses WHERE version = ?;",
        (version,),
    ).fetchall()
    for (b,) in rows:
        b = norm(b)
        if not b:
            continue
        key = normalize_book_key(b)
        # First one wins; we only need one representative
        m.setdefault(key, b)
    return m


def candidate_book_strings(book_osis: str) -> List[str]:
    """
    Generate plausible candidate strings to match bible.db book_osis values.
    """
    b = norm(book_osis)
    cands = [b]

    en = OSIS_TO_ENGLISH.get(b)
    if en:
        cands.append(en)

    # Add numbered variants
    cands.extend(OSIS_NUMBERED_ALTS.get(b, []))

    # Small additional heuristics: if OSIS ends with 'Kgs' or 'Sam' etc.
    # (safe to include; only used as lookup keys)
    if b == "Ps":
        cands.extend(["Psalm", "Psalms"])
    if b == "Song":
        cands.extend(["Song of Songs", "Canticles", "Canticle of Canticles"])

    # Dedup preserving order
    seen = set()
    out = []
    for x in cands:
        x = norm(x)
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def resolve_book_for_version(book_map: Dict[str, str], requested_book_osis: str) -> str | None:
    """
    Try to resolve requested OSIS book code to actual bible.db book_osis for a given version.
    """
    for cand in candidate_book_strings(requested_book_osis):
        k = normalize_book_key(cand)
        if k in book_map:
            return book_map[k]
    return None


def fetch_verse_range_text(
    con: sqlite3.Connection,
    version: str,
    book_osis: str,
    chapter: int,
    vstart: int,
    vend: int,
    cache: Dict[Tuple[str, str, int, int, int], str],
    book_map: Dict[str, str],
) -> str:
    # Resolve book name for this version
    resolved = resolve_book_for_version(book_map, book_osis) or book_osis

    key = (version, resolved, chapter, vstart, vend)
    if key in cache:
        return cache[key]

    rows = con.execute(
        """
        SELECT verse, text
        FROM bible.verses
        WHERE version = ?
          AND book_osis = ?
          AND chapter = ?
          AND verse BETWEEN ? AND ?
        ORDER BY verse ASC;
        """,
        (version, resolved, chapter, vstart, vend),
    ).fetchall()

    text = " ".join(norm(r[1]) for r in rows if norm(r[1]))
    cache[key] = text
    return text


def main() -> int:
    ap = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLES,
    )
    ap.add_argument("--commentary-db", required=True)
    ap.add_argument("--bible-db", required=True)
    ap.add_argument("--out-db", required=True)
    ap.add_argument("--only-status", default="ok", help="Filter ai_extractions.status (default: ok). Use empty to export all.")
    ap.add_argument("--kjv", default="KJV")
    ap.add_argument("--cuv", default="CUVS")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    commentary_db = Path(args.commentary_db).resolve()
    bible_db = Path(args.bible_db).resolve()
    out_db = Path(args.out_db).resolve()
    out_db.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(commentary_db))
    con.row_factory = sqlite3.Row
    try:
        con.execute("ATTACH DATABASE ? AS bible;", (str(bible_db),))

        # Build per-version book maps once
        kjv_book_map = build_version_book_map(con, args.kjv)
        cuv_book_map = build_version_book_map(con, args.cuv)

        con_out = sqlite3.connect(str(out_db))
        try:
            ensure_table(con_out, reset=args.reset)

            where_status = ""
            params = []
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
              ce.notes

            FROM ai_extractions ax
            LEFT JOIN commentary_edits ce
              ON ce.model = ax.model
             AND ce.prompt_version = ax.prompt_version
             AND ce.book_osis = ax.book_osis
             AND ce.chapter = ax.chapter
             AND ce.verse_start = ax.verse_start
             AND ce.verse_end = ax.verse_end

            {where_status}
            ORDER BY ax.book_osis, ax.chapter, ax.verse_start, ax.verse_end;
            """

            rows = con.execute(q, params).fetchall()

            kjv_cache: Dict[Tuple[str, str, int, int, int], str] = {}
            cuv_cache: Dict[Tuple[str, str, int, int, int], str] = {}

            to_insert = []
            dropped = 0

            for r in rows:
                kd = norm(r["edit_keep_drop"]).lower()
                if kd in DROP_WORDS:
                    dropped += 1
                    continue

                book = r["book_osis"]
                chapter = int(r["chapter"])
                vs = int(r["verse_start"])
                ve = int(r["verse_end"])

                ref = f"{book} {chapter}:{vs}" + (f"-{ve}" if ve != vs else "")

                kjv_text = fetch_verse_range_text(
                    con, args.kjv, book, chapter, vs, ve, kjv_cache, kjv_book_map
                )
                cuv_text = fetch_verse_range_text(
                    con, args.cuv, book, chapter, vs, ve, cuv_cache, cuv_book_map
                )

                final_en = pick_text(r["my_edit_en"], r["summary_en"], r["bullet_points_en"])
                final_zh = pick_text(r["my_edit_zh"], r["summary_zh"], r["bullet_points_zh"])

                to_insert.append(
                    (
                        ref, book, chapter, vs, ve,
                        args.kjv, args.cuv,
                        norm(kjv_text), norm(cuv_text),
                        r["model"], r["prompt_version"], r["status"], int(r["has_commentary"]),
                        final_en, final_zh,
                        norm(r["edit_keep_drop"]), norm(r["notes"]), norm(r["cited_para_ids"]),
                    )
                )

            con_out.execute("BEGIN;")
            con_out.executemany(
                """
                INSERT INTO final_commentary (
                  ref, book_osis, chapter, verse_start, verse_end,
                  kjv_version, cuv_version,
                  kjv_text, cuv_text,
                  model, prompt_version, status, has_commentary,
                  final_en, final_zh,
                  keep_drop, notes, cited_para_ids
                )
                VALUES (?,?,?,?,?, ?,?,?, ?,?,?,?, ?,?,?, ?,?,?)
                ON CONFLICT(model, prompt_version, book_osis, chapter, verse_start, verse_end)
                DO UPDATE SET
                  updated_at = datetime('now'),
                  ref = excluded.ref,
                  kjv_version = excluded.kjv_version,
                  cuv_version = excluded.cuv_version,
                  kjv_text = excluded.kjv_text,
                  cuv_text = excluded.cuv_text,
                  status = excluded.status,
                  has_commentary = excluded.has_commentary,
                  final_en = excluded.final_en,
                  final_zh = excluded.final_zh,
                  keep_drop = excluded.keep_drop,
                  notes = excluded.notes,
                  cited_para_ids = excluded.cited_para_ids;
                """,
                to_insert,
            )
            con_out.execute("COMMIT;")

            print(f"OK: wrote/updated {len(to_insert)} row(s) into {out_db}")
            print(f"Info: dropped by keep_drop=drop: {dropped}")
            return 0

        finally:
            con_out.close()

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
