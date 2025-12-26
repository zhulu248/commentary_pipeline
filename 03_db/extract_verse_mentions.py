# run example:
# python extract_verse_mentions.py --db "..\commentary.db" --reset

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sqlite3
from typing import Dict, List, Optional, Tuple


# -----------------------------
# 1) Book name normalization
# -----------------------------

# Canonical OSIS-like codes for storage (keeps DB consistent across languages)
# We include English + Simplified Chinese + Traditional Chinese + common abbreviations.
BOOK_ALIASES: Dict[str, str] = {}


def _add(osis: str, *aliases: str) -> None:
    for a in aliases:
        BOOK_ALIASES[a.lower()] = osis


# OT
_add("Gen", "gen", "genesis", "创", "創", "创世记", "創世記", "创世記")
_add("Exod", "ex", "exo", "exod", "exodus", "出", "出埃及记", "出埃及記")
_add("Lev", "lev", "leviticus", "利", "利未记", "利未記")
_add("Num", "num", "numbers", "民", "民数记", "民數記")
_add("Deut", "deut", "deuteronomy", "申", "申命记", "申命記")
_add("Josh", "josh", "joshua", "书", "書", "约书亚记", "約書亞記", "约书亚記", "約書亞記")
_add("Judg", "judg", "judges", "士", "士师记", "士師記")
_add("Ruth", "ruth", "得", "路得记", "路得記")
_add("1Sam", "1 sam", "1sam", "i sam", "1samuel", "1 samuel", "撒上", "撒母耳记上", "撒母耳記上")
_add("2Sam", "2 sam", "2sam", "ii sam", "2samuel", "2 samuel", "撒下", "撒母耳记下", "撒母耳記下")
_add("1Kgs", "1 kgs", "1kgs", "1 kings", "i kgs", "王上", "列王纪上", "列王紀上")
_add("2Kgs", "2 kgs", "2kgs", "2 kings", "ii kgs", "王下", "列王纪下", "列王紀下")
_add("1Chr", "1 chr", "1chr", "1 chronicles", "i chr", "代上", "历代志上", "歷代志上")
_add("2Chr", "2 chr", "2chr", "2 chronicles", "ii chr", "代下", "历代志下", "歷代志下")
_add("Ezra", "ezra", "拉", "以斯拉记", "以斯拉記")
_add("Neh", "neh", "nehemiah", "尼", "尼希米记", "尼希米記")
_add("Esth", "esth", "esther", "斯", "以斯帖记", "以斯帖記")
_add("Job", "job", "伯", "约伯记", "約伯記")
_add("Ps", "ps", "psalm", "psalms", "诗", "詩", "诗篇", "詩篇")
_add("Prov", "prov", "proverbs", "箴", "箴言")
_add("Eccl", "eccl", "ecclesiastes", "传", "傳", "传道书", "傳道書")
_add("Song", "song", "song of songs", "song of solomon", "雅", "雅歌")
_add("Isa", "isa", "isaiah", "赛", "賽", "以赛亚书", "以賽亞書")
_add("Jer", "jer", "jeremiah", "耶", "耶利米书", "耶利米書")
_add("Lam", "lam", "lamentations", "哀", "耶利米哀歌", "耶利米哀歌")
_add("Ezek", "ezek", "ezekiel", "结", "結", "以西结书", "以西結書")
_add("Dan", "dan", "daniel", "但", "但以理书", "但以理書")
_add("Hos", "hos", "hosea", "何", "何西阿书", "何西阿書")
_add("Joel", "joel", "珥", "约珥书", "約珥書")
_add("Amos", "amos", "摩", "阿摩司书", "阿摩司書")
_add("Obad", "obad", "obadiah", "俄", "俄巴底亚书", "俄巴底亞書")
_add("Jonah", "jonah", "拿", "约拿书", "約拿書")
_add("Mic", "mic", "micah", "弥", "彌", "弥迦书", "彌迦書")
_add("Nah", "nah", "nahum", "鸿", "鴻", "那鸿书", "那鴻書")
_add("Hab", "hab", "habakkuk", "哈", "哈巴谷书", "哈巴谷書")
_add("Zeph", "zeph", "zephaniah", "番", "西番雅书", "西番雅書")
_add("Hag", "hag", "haggai", "该", "該", "哈该书", "哈該書")
_add("Zech", "zech", "zechariah", "亚", "亞", "撒迦利亚书", "撒迦利亞書")
_add("Mal", "mal", "malachi", "玛", "瑪", "玛拉基书", "瑪拉基書")

# NT
_add("Matt", "matt", "mt", "matthew", "太", "马太福音", "馬太福音")
_add("Mark", "mark", "mk", "可", "马可福音", "馬可福音")
_add("Luke", "luke", "lk", "路", "路加福音")
_add("John", "john", "jn", "约", "約", "约翰福音", "約翰福音")
_add("Acts", "acts", "act", "徒", "使徒行传", "使徒行傳")
_add("Rom", "rom", "romans", "罗", "羅", "罗马书", "羅馬書")
_add("1Cor", "1 cor", "1cor", "i cor", "1 corinthians", "林前", "哥林多前书", "哥林多前書")
_add("2Cor", "2 cor", "2cor", "ii cor", "2 corinthians", "林后", "林後", "哥林多后书", "哥林多後書")
_add("Gal", "gal", "galatians", "加", "加拉太书", "加拉太書")
_add("Eph", "eph", "ephesians", "弗", "以弗所书", "以弗所書")
_add("Phil", "phil", "philippians", "腓", "腓立比书", "腓立比書")
_add("Col", "col", "colossians", "西", "歌罗西书", "歌羅西書")
_add("1Thess", "1 thess", "1thess", "i thess", "帖前", "帖撒罗尼迦前书", "帖撒羅尼迦前書")
_add("2Thess", "2 thess", "2thess", "ii thess", "帖后", "帖後", "帖撒罗尼迦后书", "帖撒羅尼迦後書")
_add("1Tim", "1 tim", "1tim", "i tim", "提前", "提摩太前书", "提摩太前書")
_add("2Tim", "2 tim", "2tim", "ii tim", "提后", "提後", "提摩太后书", "提摩太後書")
_add("Titus", "titus", "多", "提多书", "提多書")
_add("Phlm", "phlm", "philemon", "门", "門", "腓利门书", "腓利門書")
_add("Heb", "heb", "hebrews", "来", "來", "希伯来书", "希伯來書")
_add("Jas", "jas", "james", "雅各书", "雅各書")
_add("1Pet", "1 pet", "1pet", "i pet", "彼前", "彼得前书", "彼得前書")
_add("2Pet", "2 pet", "2pet", "ii pet", "彼后", "彼後", "彼得后书", "彼得後書")
_add("1John", "1 john", "1john", "i john", "约一", "約一", "约壹", "約壹", "约翰一书", "約翰一書")
_add("2John", "2 john", "2john", "ii john", "约二", "約二", "约贰", "約貳", "约翰二书", "約翰二書")
_add("3John", "3 john", "3john", "iii john", "约三", "約三", "约叁", "約參", "约翰三书", "約翰三書")
_add("Jude", "jude", "犹", "猶", "犹大书", "猶大書")
_add("Rev", "rev", "revelation", "启", "啟", "启示录", "啟示錄")


def normalize_book(token: str) -> Optional[str]:
    t = token.strip().lower()
    t = re.sub(r"\s+", " ", t)
    return BOOK_ALIASES.get(t)


# Standard chapter counts (used to reject junk like "Rom 193")
BOOK_MAX_CHAPTERS: Dict[str, int] = {
    "Gen": 50, "Exod": 40, "Lev": 27, "Num": 36, "Deut": 34,
    "Josh": 24, "Judg": 21, "Ruth": 4, "1Sam": 31, "2Sam": 24,
    "1Kgs": 22, "2Kgs": 25, "1Chr": 29, "2Chr": 36, "Ezra": 10,
    "Neh": 13, "Esth": 10, "Job": 42, "Ps": 150, "Prov": 31,
    "Eccl": 12, "Song": 8, "Isa": 66, "Jer": 52, "Lam": 5,
    "Ezek": 48, "Dan": 12, "Hos": 14, "Joel": 3, "Amos": 9,
    "Obad": 1, "Jonah": 4, "Mic": 7, "Nah": 3, "Hab": 3,
    "Zeph": 3, "Hag": 2, "Zech": 14, "Mal": 4,
    "Matt": 28, "Mark": 16, "Luke": 24, "John": 21, "Acts": 28,
    "Rom": 16, "1Cor": 16, "2Cor": 13, "Gal": 6, "Eph": 6,
    "Phil": 4, "Col": 4, "1Thess": 5, "2Thess": 3, "1Tim": 6,
    "2Tim": 4, "Titus": 3, "Phlm": 1, "Heb": 13, "Jas": 5,
    "1Pet": 5, "2Pet": 3, "1John": 5, "2John": 1, "3John": 1,
    "Jude": 1, "Rev": 22,
}


def chap_in_range(book_osis: Optional[str], chap: Optional[int]) -> bool:
    if not book_osis or chap is None:
        return False
    mx = BOOK_MAX_CHAPTERS.get(book_osis)
    if mx is None:
        # Unknown book: be conservative but not too strict
        return 1 <= chap <= 200
    return 1 <= chap <= mx


# -----------------------------
# 2) Regex patterns
# -----------------------------

# Build alternation for book tokens. Sort longest-first (helps long Chinese names win).
_BOOK_TOKENS = sorted(BOOK_ALIASES.keys(), key=len, reverse=True)
BOOK_ALT = "|".join(re.escape(b) for b in _BOOK_TOKENS)

# Allow hyphen/en-dash/em-dash/range markers
DASH = r"(?:-|\u2013|\u2014|~|–|—)"

# Pattern with chapter + verse:
#   John 3:16
#   约3:16
#   约3章16节
RE_REF = re.compile(
    rf"(?P<book>{BOOK_ALT})\s*"
    rf"(?P<chap>\d{{1,3}})\s*"
    rf"(?:[:：]|章)\s*"
    rf"(?P<v1>\d{{1,3}})"
    rf"(?:\s*{DASH}\s*(?P<v2>\d{{1,3}}))?"
    rf"(?:\s*节)?",
    re.IGNORECASE,
)

# Pattern with book + chapter only (no verse):
#   John 3
#   约3章
RE_CHAP_ONLY = re.compile(
    rf"(?P<book>{BOOK_ALT})\s*"
    rf"(?P<chap>\d{{1,3}})"
    rf"(?:\s*章)?",
    re.IGNORECASE,
)


# -----------------------------
# 3) DB schema + migration
# -----------------------------

SCHEMA = r"""
CREATE TABLE IF NOT EXISTS verse_mentions (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id       INTEGER NOT NULL,
  p_index      INTEGER NOT NULL,
  para_id      INTEGER,              -- NEW (may be NULL in old DBs until re-extract)
  raw_match    TEXT NOT NULL,
  book_osis    TEXT,
  chapter      INTEGER,
  verse_start  INTEGER,
  verse_end    INTEGER,
  parse_status TEXT NOT NULL,         -- ok|chapter_only|unparsed
  FOREIGN KEY(doc_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_mentions_doc ON verse_mentions(doc_id);
CREATE INDEX IF NOT EXISTS idx_mentions_para ON verse_mentions(para_id);
CREATE INDEX IF NOT EXISTS idx_mentions_book_ch ON verse_mentions(book_osis, chapter);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)

    # Auto-migrate older DBs that don't have para_id yet.
    cols = [r[1] for r in conn.execute("PRAGMA table_info(verse_mentions);").fetchall()]
    if "para_id" not in cols:
        conn.execute("ALTER TABLE verse_mentions ADD COLUMN para_id INTEGER;")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mentions_para ON verse_mentions(para_id);")
        conn.commit()


# -----------------------------
# 4) Extraction
# -----------------------------

MentionTuple = Tuple[str, Optional[str], Optional[int], Optional[int], Optional[int], str, Tuple[int, int]]
# (raw_match, book_osis, chap, v1, v2, status, span)


def extract_mentions(text: str, keep_chapter_only: bool = True) -> List[MentionTuple]:
    """
    Returns list of:
      (raw_match, book_osis, chapter, v1, v2, parse_status, (start,end))
    """
    out: List[MentionTuple] = []

    ref_spans: List[Tuple[int, int]] = []

    # 1) chapter:verse matches
    for m in RE_REF.finditer(text):
        raw = m.group(0)
        book = normalize_book(m.group("book"))
        chap = int(m.group("chap")) if m.group("chap") else None
        v1 = int(m.group("v1")) if m.group("v1") else None
        v2 = int(m.group("v2")) if m.group("v2") else None
        if v2 is None:
            v2 = v1

        # Reject obvious junk (e.g., "Rom 193:..." due to page numbers)
        if not chap_in_range(book, chap):
            out.append((raw, None, None, None, None, "unparsed", m.span()))
            continue

        out.append((raw, book, chap, v1, v2, "ok", m.span()))
        ref_spans.append(m.span())

    if not keep_chapter_only:
        return out

    def overlaps_any(span: Tuple[int, int], spans: List[Tuple[int, int]]) -> bool:
        a0, a1 = span
        for b0, b1 in spans:
            if a0 < b1 and b0 < a1:
                return True
        return False

    # 2) chapter-only matches (skip ones overlapping an already-captured chapter:verse)
    for m in RE_CHAP_ONLY.finditer(text):
        raw = m.group(0)
        if ":" in raw or "：" in raw:
            continue
        if overlaps_any(m.span(), ref_spans):
            continue

        book = normalize_book(m.group("book"))
        chap = int(m.group("chap")) if m.group("chap") else None

        if chap_in_range(book, chap):
            out.append((raw, book, chap, None, None, "chapter_only", m.span()))
        else:
            # Ignore junk chapter-only refs entirely (most are page numbers).
            continue

    return out


# -----------------------------
# 5) Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract Bible reference mentions (English + Chinese) from paragraphs into verse_mentions."
    )
    ap.add_argument("--db", required=True, help="Path to commentary.db")
    ap.add_argument("--reset", action="store_true", help="Delete existing verse_mentions before re-extracting")
    ap.add_argument("--limit", type=int, default=0, help="Limit paragraphs processed (0=all), for quick tests")
    ap.add_argument("--no-chapter-only", action="store_true", help="Do not store chapter-only references (e.g., John 3)")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys=ON;")
    ensure_schema(conn)

    if args.reset:
        conn.execute("DELETE FROM verse_mentions;")
        conn.commit()

    q = "SELECT id, doc_id, p_index, text FROM paragraphs ORDER BY doc_id, p_index"
    if args.limit and args.limit > 0:
        q += f" LIMIT {int(args.limit)}"

    cur = conn.execute(q)

    to_insert: List[Tuple[int, int, int, str, Optional[str], Optional[int], Optional[int], Optional[int], str]] = []
    total_paras = 0
    total_mentions = 0

    keep_chapter_only = not args.no_chapter_only

    for para_id, doc_id, p_index, text in cur:
        total_paras += 1
        hits = extract_mentions(text, keep_chapter_only=keep_chapter_only)
        for raw, book, chap, v1, v2, status, _span in hits:
            to_insert.append((doc_id, p_index, para_id, raw, book, chap, v1, v2, status))
            total_mentions += 1

    conn.executemany(
        """
        INSERT INTO verse_mentions(
          doc_id, p_index, para_id, raw_match,
          book_osis, chapter, verse_start, verse_end,
          parse_status
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        """,
        to_insert,
    )
    conn.commit()

    print(f"OK: scanned paragraphs: {total_paras}")
    print(f"OK: verse mentions inserted: {total_mentions}")
    print("Tip: query examples:")
    print(r'  sqlite3 "..\commentary.db" "select book_osis, count(*) from verse_mentions where parse_status=''ok'' group by book_osis order by count(*) desc limit 20;"')
    print(r'  sqlite3 "..\commentary.db" "select raw_match, book_osis, chapter, verse_start, verse_end, parse_status from verse_mentions limit 30;"')

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
