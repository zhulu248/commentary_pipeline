# run example:
# python export_ai_commentary_draft.py --commentary-db "..\commentary.db" --bible-db "..\bible.db" --out "commentary_draft.csv" --only-status ok --only-has-commentary 0 --limit 0

#!/usr/bin/env python3
"""
export_ai_commentary_draft.py

Export AI extraction results from commentary.db into a CSV for manual review/editing.
Includes KJV + CUVS verse text from bible.db, plus citations/sources if available.

Outputs a spreadsheet-friendly CSV (UTF-8 with BOM) so Excel shows Chinese correctly.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class VerseKey:
    book_osis: str
    chapter: int
    verse_start: int
    verse_end: int


def excel_safe(s: str) -> str:
    return (s or "").replace("\r\n", " \\n ").replace("\n", " \\n ").replace("\r", " ")





def make_ref(vk: VerseKey) -> str:
    if vk.verse_start == vk.verse_end:
        return f"{vk.book_osis} {vk.chapter}:{vk.verse_start}"
    return f"{vk.book_osis} {vk.chapter}:{vk.verse_start}-{vk.verse_end}"


def fetch_range_text(
    bible_con: sqlite3.Connection,
    version: str,
    vk: VerseKey,
) -> str:
    rows = bible_con.execute(
        """
        SELECT verse, text
        FROM verses
        WHERE version=? AND book_osis=? AND chapter=? AND verse BETWEEN ? AND ?
        ORDER BY verse
        """,
        (version, vk.book_osis, vk.chapter, vk.verse_start, vk.verse_end),
    ).fetchall()

    if not rows:
        return ""

    if vk.verse_start == vk.verse_end:
        return (rows[0][1] or "").strip()

    # For ranges, label verses to reduce confusion during review.
    parts = []
    for v, t in rows:
        t = (t or "").strip()
        if t:
            parts.append(f"{v}. {t}")
    return "\n".join(parts).strip()


def safe_json_loads(s: str) -> Any:
    try:
        return json.loads(s) if s else None
    except Exception:
        return None


def iter_extractions(
    con: sqlite3.Connection,
    only_status: str,
    only_has_commentary: int | None,
    limit: int,
) -> Iterable[sqlite3.Row]:
    where = ["1=1"]
    params: list[Any] = []

    if only_status:
        where.append("status=?")
        params.append(only_status)

    if only_has_commentary is not None:
        where.append("has_commentary=?")
        params.append(int(only_has_commentary))

    lim = f"LIMIT {limit}" if limit and limit > 0 else ""

    q = f"""
    SELECT
      id, created_at, model, prompt_version,
      book_osis, chapter, verse_start, verse_end,
      has_commentary,
      summary_en, summary_zh,
      bullet_points_en, bullet_points_zh,
      cited_para_ids,
      status, error
    FROM ai_extractions
    WHERE {" AND ".join(where)}
    ORDER BY book_osis, chapter, verse_start, verse_end, id
    {lim}
    """
    cur = con.execute(q, tuple(params))
    for r in cur:
        yield r


def fetch_citations_block(con: sqlite3.Connection, extraction_id: int, max_lines: int) -> tuple[str, str]:
    """
    Returns (sources_joined, evidence_block).
    - sources_joined: "Title — URL | Title — URL | ..."
    - evidence_block: multi-line text with para_id + reason + snippet
    """
    rows = con.execute(
        """
        SELECT
          c.para_id,
          c.reason,
          p.p_index,
          d.title,
          d.author,
          d.source,
          p.text
        FROM ai_extraction_citations c
        JOIN paragraphs p ON p.id = c.para_id
        JOIN documents  d ON d.id = p.doc_id
        WHERE c.extraction_id=?
        ORDER BY d.id, p.p_index
        """,
        (extraction_id,),
    ).fetchall()

    if not rows:
        return ("", "")

    # Unique sources list
    sources = []
    seen = set()
    for (_, _, _, title, author, source, _) in rows:
        title = (title or "").strip()
        author = (author or "").strip()
        source = (source or "").strip()
        label = title if title else "(untitled)"
        if author:
            label = f"{label} — {author}"
        key = (label, source)
        if key not in seen:
            seen.add(key)
            if source:
                sources.append(f"{label} — {source}")
            else:
                sources.append(f"{label}")

    # Evidence block
    evidence_lines = []
    for i, (para_id, reason, p_index, title, author, source, text) in enumerate(rows, start=1):
        if i > max_lines:
            break
        title = (title or "").strip()
        author = (author or "").strip()
        source = (source or "").strip()
        reason = (reason or "").strip()

        snippet = (text or "").strip().replace("\r", " ").replace("\n", " ")
        if len(snippet) > 500:
            snippet = snippet[:500].rstrip() + " …"

        header = title if title else "(untitled)"
        if author:
            header += f" — {author}"
        if source:
            header += f" | {source}"

        evidence_lines.append(
            f"- para_id={para_id}, p_index={p_index}: {reason}\n  {header}\n  {snippet}"
        )

    return (" | ".join(sources), "\n".join(evidence_lines))


DESCRIPTION = """Export AI commentary draft to CSV for manual review.\n\nExample (copy/paste):\n  python 08_export_commentary/export_ai_commentary_draft.py --commentary-db ./commentary.db --bible-db ./bible.db --out commentary_draft.csv\n"""

EXAMPLES = """Examples (copy/paste ready from repo root):
  # Export all OK rows to CSV in the project root
  python 08_export_commentary/export_ai_commentary_draft.py \
    --commentary-db ./commentary.db --bible-db ./bible.db --out commentary_draft.csv

  # Export only rows that lack commentary to a custom folder
  python 08_export_commentary/export_ai_commentary_draft.py \
    --commentary-db ./commentary.db --bible-db ./bible.db \
    --only-status ok --only-has-commentary 0 --limit 0 --out ./out/commentary_needs_review.csv
"""


def main() -> int:
    ap = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLES,
    )
    ap.add_argument("--commentary-db", required=True, help="Path to commentary.db")
    ap.add_argument("--bible-db", required=True, help="Path to bible.db")
    ap.add_argument("--out", default="commentary_draft.csv", help="Output CSV path")
    ap.add_argument("--only-status", default="ok", help="Filter ai_extractions.status (default: ok)")
    ap.add_argument(
        "--only-has-commentary",
        type=int,
        default=None,
        help="Set to 1 to export only has_commentary=1; set to 0 for only has_commentary=0; omit for both.",
    )
    ap.add_argument("--limit", type=int, default=0, help="0 = no limit")
    ap.add_argument("--max-citation-lines", type=int, default=12, help="Max evidence lines per row")
    args = ap.parse_args()

    cdb = Path(args.commentary_db).resolve()
    bdb = Path(args.bible_db).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(cdb))
    con.row_factory = sqlite3.Row
    bible = sqlite3.connect(str(bdb))
    bible.row_factory = sqlite3.Row

    # Export
    n = 0
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow([
            "ref",
            "book_osis", "chapter", "verse_start", "verse_end",
            "kjv_text", "cuvs_text",
            "has_commentary",
            "summary_en", "summary_zh",
            "bullet_points_en", "bullet_points_zh",
            "sources",
            "evidence",
            "model", "prompt_version", "created_at",
            "status", "error",
            # your manual workflow columns:
            "keep_drop",
            "my_edit_en",
            "my_edit_zh",
            "notes",
        ])

        for r in iter_extractions(con, args.only_status, args.only_has_commentary, args.limit):
            vk = VerseKey(
                book_osis=str(r["book_osis"]),
                chapter=int(r["chapter"]),
                verse_start=int(r["verse_start"]),
                verse_end=int(r["verse_end"]),
            )

            ref = make_ref(vk)
            kjv = fetch_range_text(bible, "KJV", vk)
            cuvs = fetch_range_text(bible, "CUVS", vk)

            bullets_en = safe_json_loads(r["bullet_points_en"]) or []
            bullets_zh = safe_json_loads(r["bullet_points_zh"]) or []

            sources, evidence = fetch_citations_block(con, int(r["id"]), args.max_citation_lines)

            w.writerow([
                ref,
                vk.book_osis, vk.chapter, vk.verse_start, vk.verse_end,
                kjv, cuvs,
                int(r["has_commentary"]),
                (r["summary_en"] or ""),
                (r["summary_zh"] or ""),
                json.dumps(bullets_en, ensure_ascii=False),
                json.dumps(bullets_zh, ensure_ascii=False),
                sources,
                evidence,
                r["model"], r["prompt_version"], r["created_at"],
                r["status"], (r["error"] or ""),
                "",  # keep_drop
                "",  # my_edit_en
                "",  # my_edit_zh
                "",  # notes
            ])
            n += 1

    con.close()
    bible.close()

    print(f"OK: wrote {n} row(s) -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
