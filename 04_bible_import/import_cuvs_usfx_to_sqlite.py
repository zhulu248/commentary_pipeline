# run example:
# python import_cuvs_usfx_to_sqlite.py --db "..\bible.db" --usfx ".\chi-cuv.usfx.xml" --version CUVS --name "Chinese Union Version (Simplified)" --reset-version

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path
import xml.etree.ElementTree as ET


USFM_TO_OSIS = {
    "GEN": "Gen", "EXO": "Exod", "LEV": "Lev", "NUM": "Num", "DEU": "Deut",
    "JOS": "Josh", "JDG": "Judg", "RUT": "Ruth",
    "1SA": "1Sam", "2SA": "2Sam", "1KI": "1Kgs", "2KI": "2Kgs",
    "1CH": "1Chr", "2CH": "2Chr", "EZR": "Ezra", "NEH": "Neh", "EST": "Esth",
    "JOB": "Job", "PSA": "Ps", "PRO": "Prov", "ECC": "Eccl", "SNG": "Song",
    "ISA": "Isa", "JER": "Jer", "LAM": "Lam", "EZK": "Ezek", "DAN": "Dan",
    "HOS": "Hos", "JOL": "Joel", "AMO": "Amos", "OBA": "Obad", "JON": "Jonah",
    "MIC": "Mic", "NAM": "Nah", "HAB": "Hab", "ZEP": "Zeph", "HAG": "Hag",
    "ZEC": "Zech", "MAL": "Mal",
    "MAT": "Matt", "MRK": "Mark", "LUK": "Luke", "JHN": "John", "ACT": "Acts",
    "ROM": "Rom", "1CO": "1Cor", "2CO": "2Cor", "GAL": "Gal", "EPH": "Eph",
    "PHP": "Phil", "COL": "Col", "1TH": "1Thess", "2TH": "2Thess",
    "1TI": "1Tim", "2TI": "2Tim", "TIT": "Titus", "PHM": "Phlm",
    "HEB": "Heb", "JAS": "Jas", "1PE": "1Pet", "2PE": "2Pet",
    "1JN": "1John", "2JN": "2John", "3JN": "3John", "JUD": "Jude", "REV": "Rev",
}

SKIP_TAGS = {"note", "f", "fn", "ft", "x", "xo", "xt", "ref"}

WS = re.compile(r"\s+")
DIGITS_ANYWHERE = re.compile(r"(\d+)")


def localname(tag: str) -> str:
    return tag.split("}", 1)[-1]


def norm_text(s: str) -> str:
    return WS.sub(" ", s or "").strip()


def last_int_anywhere(s: str | None) -> int | None:
    if not s:
        return None
    hits = DIGITS_ANYWHERE.findall(s)
    if not hits:
        return None
    return int(hits[-1])


def chapter_from_el(el: ET.Element) -> int | None:
    for k in ("id", "n", "number", "sid", "ref"):
        n = last_int_anywhere(el.attrib.get(k))
        if n is not None:
            return n
    return None


def verse_from_el(el: ET.Element) -> int | None:
    # ignore pure end markers if present
    if "eid" in el.attrib and not any(k in el.attrib for k in ("id", "n", "number", "sid", "ref")):
        return None
    for k in ("id", "n", "number", "sid", "ref"):
        n = last_int_anywhere(el.attrib.get(k))
        if n is not None:
            return n
    return None


def walk_in_order(el: ET.Element):
    """
    Yields elements in the true text order:
    - element start is implicit here
    - then el.text
    - then each child recursively
    - then each child.tail (after that child subtree)
    """
    yield ("elem", el)          # allows caller to react to markers like <c/> or <v/>
    if el.text:
        yield ("text", el.text)

    for ch in list(el):
        for item in walk_in_order(ch):
            yield item
        if ch.tail:
            yield ("text", ch.tail)


def flush_verse(rows, version, book_osis, ch, v, buf):
    if book_osis and ch and v:
        txt = norm_text("".join(buf))
        if txt:
            rows.append((version, book_osis, ch, v, txt))
    buf.clear()


def main() -> int:
    ap = argparse.ArgumentParser(description="Import CUVS USFX XML into bible.db (verses table).")
    ap.add_argument("--db", required=True)
    ap.add_argument("--usfx", required=True)
    ap.add_argument("--version", default="CUVS")
    ap.add_argument("--name", default="Chinese Union Version (Simplified)")
    ap.add_argument("--source", default="https://github.com/seven1m/open-bibles (chi-cuv.usfx.xml)")
    ap.add_argument("--license", default="Public Domain (per source repository listing)")
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--reset-version", action="store_true")
    args = ap.parse_args()

    db_path = Path(args.db).resolve()
    usfx_path = Path(args.usfx).resolve()
    if not usfx_path.exists():
        print(f"ERROR: USFX file not found: {usfx_path}", file=sys.stderr)
        return 2

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute(
        "INSERT OR REPLACE INTO bible_versions(version, name, source, license) VALUES (?,?,?,?)",
        (args.version, args.name, args.source, args.license),
    )
    con.commit()

    if args.reset_version:
        con.execute("DELETE FROM verses WHERE version=?", (args.version,))
        con.commit()

    # Parse whole XML (small enough), then walk in correct order
    tree = ET.parse(usfx_path)
    root = tree.getroot()

    rows: list[tuple[str, str, int, int, str]] = []
    inserted_attempts = 0

    def commit_rows():
        nonlocal inserted_attempts, rows
        if not rows:
            return
        con.executemany(
            "INSERT OR REPLACE INTO verses(version, book_osis, chapter, verse, text) VALUES (?,?,?,?,?)",
            rows,
        )
        con.commit()
        inserted_attempts += len(rows)
        rows = []

    # Find all book elements
    for book_el in root.iter():
        if localname(book_el.tag) != "book":
            continue

        bid = (book_el.attrib.get("id") or book_el.attrib.get("code") or "").strip().upper()
        if not bid:
            continue

        book_osis = USFM_TO_OSIS.get(bid)
        if not book_osis:
            # ignore front/back matter
            continue

        cur_ch: int | None = None
        cur_v: int | None = None
        buf: list[str] = []

        for kind, payload in walk_in_order(book_el):
            if kind == "elem":
                el = payload
                t = localname(el.tag)

                if t in ("c", "chapter"):
                    # chapter change flushes any pending verse
                    flush_verse(rows, args.version, book_osis, cur_ch, cur_v, buf)
                    cur_ch = chapter_from_el(el)
                    cur_v = None
                    if len(rows) >= args.batch_size:
                        commit_rows()
                    continue

                if t in ("v", "verse"):
                    flush_verse(rows, args.version, book_osis, cur_ch, cur_v, buf)
                    cur_v = verse_from_el(el)
                    if len(rows) >= args.batch_size:
                        commit_rows()
                    continue

            else:  # kind == "text"
                text = payload
                # We don't want footnotes/notes, but those are usually in SKIP_TAGS elements.
                # Since we're streaming raw text, keep it simple: just accumulate.
                if cur_ch is not None and cur_v is not None:
                    buf.append(text)

        # end of book
        flush_verse(rows, args.version, book_osis, cur_ch, cur_v, buf)
        if rows:
            commit_rows()

    # Report actual stored count (not attempted inserts)
    actual = con.execute("SELECT COUNT(*) FROM verses WHERE version=?", (args.version,)).fetchone()[0]
    con.close()

    print(f"OK: attempted inserts (including replacements): {inserted_attempts}")
    print(f"OK: actual rows stored for {args.version}: {actual}")
    print(f"DB: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
