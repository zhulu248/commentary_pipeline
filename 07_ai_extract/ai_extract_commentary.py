# run example:
# set OPENAI_API_KEY=YOUR_KEY_HERE
# python ai_extract_commentary.py --commentary-db "..\commentary.db" --bible-db "..\bible.db" --model gpt-5 --prompt-version v1 --max-paras-per-verse 10 --limit 20 --sleep 0.2 --zh

#!/usr/bin/env python3
"""
ai_extract_commentary.py

For each distinct verse reference (from verse_mentions.parse_status='ok'),
pull a small set of source paragraphs (with IDs), pull the verse text from bible.db
(KJV + CUVS), ask OpenAI to extract relevant commentary, and store results in
commentary.db tables created by init_ai_tables.py.

Requires:
  pip install openai
Env:
  OPENAI_API_KEY
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from openai import OpenAI


@dataclass(frozen=True)
class VerseKey:
    book_osis: str
    chapter: int
    verse_start: int
    verse_end: int


def iso_now() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=-5))).isoformat(timespec="seconds")


def fetch_verse_text(bible_con: sqlite3.Connection, version: str, vk: VerseKey) -> str | None:
    cur = bible_con.cursor()
    rows = cur.execute(
        """
        SELECT verse, text
        FROM verses
        WHERE version=? AND book_osis=? AND chapter=? AND verse BETWEEN ? AND ?
        ORDER BY verse
        """,
        (version, vk.book_osis, vk.chapter, vk.verse_start, vk.verse_end),
    ).fetchall()

    if not rows:
        return None

    # Join multi-verse ranges as "16 ... 17 ..." etc.
    if vk.verse_start == vk.verse_end:
        return rows[0][1].strip()

    parts = []
    for v, t in rows:
        t = (t or "").strip()
        if t:
            parts.append(f"{v}. {t}")
    return "\n".join(parts).strip() if parts else None


def list_target_verses(commentary_con: sqlite3.Connection) -> list[VerseKey]:
    rows = commentary_con.execute(
        """
        SELECT book_osis, chapter, verse_start, verse_end
        FROM verse_mentions
        WHERE parse_status='ok'
          AND book_osis IS NOT NULL
          AND chapter IS NOT NULL
          AND verse_start IS NOT NULL
          AND verse_end IS NOT NULL
        GROUP BY book_osis, chapter, verse_start, verse_end
        ORDER BY book_osis, chapter, verse_start, verse_end
        """
    ).fetchall()
    return [VerseKey(r[0], int(r[1]), int(r[2]), int(r[3])) for r in rows]


def fetch_evidence_paragraphs(commentary_con: sqlite3.Connection, vk: VerseKey, max_paras: int) -> list[dict[str, Any]]:
    # Distinct paragraphs that were tagged as mentioning this exact verse range.
    rows = commentary_con.execute(
        """
        SELECT DISTINCT
          p.id        AS para_id,
          p.p_index   AS p_index,
          d.id        AS doc_id,
          COALESCE(d.title, '')  AS doc_title,
          COALESCE(d.author, '') AS doc_author,
          COALESCE(d.source, '') AS doc_source,
          p.text      AS para_text
        FROM verse_mentions vm
        JOIN paragraphs p ON p.id = vm.para_id
        JOIN documents  d ON d.id = p.doc_id
        WHERE vm.parse_status='ok'
          AND vm.book_osis=? AND vm.chapter=? AND vm.verse_start=? AND vm.verse_end=?
        ORDER BY d.id, p.p_index
        LIMIT ?
        """,
        (vk.book_osis, vk.chapter, vk.verse_start, vk.verse_end, max_paras),
    ).fetchall()

    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "para_id": int(r[0]),
                "p_index": int(r[1]),
                "doc_id": int(r[2]),
                "doc_title": r[3],
                "doc_author": r[4],
                "doc_source": r[5],
                "text": (r[6] or "").strip(),
            }
        )
    return out


def already_done(commentary_con: sqlite3.Connection, model: str, prompt_version: str, vk: VerseKey) -> bool:
    row = commentary_con.execute(
        """
        SELECT 1 FROM ai_extractions
        WHERE model=? AND prompt_version=?
          AND book_osis=? AND chapter=? AND verse_start=? AND verse_end=?
        """,
        (model, prompt_version, vk.book_osis, vk.chapter, vk.verse_start, vk.verse_end),
    ).fetchone()
    return row is not None


def save_result(
    commentary_con: sqlite3.Connection,
    *,
    created_at: str,
    model: str,
    prompt_version: str,
    vk: VerseKey,
    result_json: dict[str, Any],
    status: str,
    error: str | None,
) -> None:
    has_commentary = 1 if bool(result_json.get("has_commentary")) else 0
    summary_en = (result_json.get("summary_en") or "").strip()
    summary_zh = (result_json.get("summary_zh") or "").strip()
    bpe = result_json.get("bullet_points_en") or []
    bpz = result_json.get("bullet_points_zh") or []
    cited = result_json.get("cited_para_ids") or []
    citations = result_json.get("citations") or []

    cur = commentary_con.cursor()
    cur.execute(
        """
        INSERT INTO ai_extractions (
          created_at, model, prompt_version,
          book_osis, chapter, verse_start, verse_end,
          has_commentary,
          summary_en, summary_zh,
          bullet_points_en, bullet_points_zh,
          cited_para_ids,
          raw_json,
          status, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            created_at, model, prompt_version,
            vk.book_osis, vk.chapter, vk.verse_start, vk.verse_end,
            has_commentary,
            summary_en, summary_zh,
            json.dumps(bpe, ensure_ascii=False),
            json.dumps(bpz, ensure_ascii=False),
            json.dumps(cited, ensure_ascii=False),
            json.dumps(result_json, ensure_ascii=False),
            status, error,
        ),
    )
    extraction_id = cur.lastrowid

    for c in citations:
        try:
            para_id = int(c["para_id"])
            reason = str(c.get("reason", "")).strip()[:500]
            if reason:
                cur.execute(
                    "INSERT OR IGNORE INTO ai_extraction_citations(extraction_id, para_id, reason) VALUES(?,?,?)",
                    (extraction_id, para_id, reason),
                )
        except Exception:
            continue

    commentary_con.commit()


def build_schema(include_zh: bool) -> dict[str, Any]:
    # Keep schema simple + strict.
    return {
        "type": "object",
        "properties": {
            "verse_ref": {"type": "string"},
            "has_commentary": {"type": "boolean"},
            "summary_en": {"type": "string"},
            "summary_zh": {"type": "string"},
            "bullet_points_en": {"type": "array", "items": {"type": "string"}},
            "bullet_points_zh": {"type": "array", "items": {"type": "string"}},
            "cited_para_ids": {"type": "array", "items": {"type": "integer"}},
            "citations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "para_id": {"type": "integer"},
                        "reason": {"type": "string"},
                    },
                    "required": ["para_id", "reason"],
                    "additionalProperties": False,
                },
            },
            "notes": {"type": "string"},
        },
        "required": [
            "verse_ref",
            "has_commentary",
            "summary_en",
            "summary_zh",
            "bullet_points_en",
            "bullet_points_zh",
            "cited_para_ids",
            "citations",
            "notes",
        ],
        "additionalProperties": False,
    }


def format_verse_ref(vk: VerseKey) -> str:
    if vk.verse_start == vk.verse_end:
        return f"{vk.book_osis} {vk.chapter}:{vk.verse_start}"
    return f"{vk.book_osis} {vk.chapter}:{vk.verse_start}-{vk.verse_end}"


DESCRIPTION = """Extract Bible-verse commentary from tagged paragraphs via OpenAI.\n\nExample:\n  OPENAI_API_KEY=sk-... python 07_ai_extract/ai_extract_commentary.py --commentary-db ./commentary.db --bible-db ./bible.db --model gpt-5 --prompt-version v1 --limit 5 --max-paras-per-verse 8 --sleep 0.5\n"""

EXAMPLES = """Examples (copy/paste ready from repo root):
  # Small batch run using the sample DBs in this repo
  OPENAI_API_KEY=sk-... python 07_ai_extract/ai_extract_commentary.py \
    --commentary-db ./commentary.db --bible-db ./bible.db \
    --model gpt-5 --prompt-version v1 --limit 5 --max-paras-per-verse 8 --sleep 0.5 --zh

  # Resume later with the same model/prompt and skip completed verses
  OPENAI_API_KEY=sk-... python 07_ai_extract/ai_extract_commentary.py \
    --commentary-db ./commentary.db --bible-db ./bible.db \
    --model gpt-4o --prompt-version v1 --resume --limit 0
"""


def main() -> int:
    ap = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLES,
    )
    ap.add_argument("--commentary-db", required=True)
    ap.add_argument("--bible-db", required=True)
    ap.add_argument("--model", default="gpt-5")
    ap.add_argument("--prompt-version", default="v1")
    ap.add_argument("--max-paras-per-verse", type=int, default=10)
    ap.add_argument("--limit", type=int, default=20, help="0 = no limit")
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--zh", action="store_true", help="Ask for Chinese output too (summary_zh / bullet_points_zh).")
    ap.add_argument("--resume", action="store_true", help="Skip verses already extracted for (model,prompt_version).")
    args = ap.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY is not set.")
        print('Tip (PowerShell): $env:OPENAI_API_KEY="sk-..."')
        print('Tip (cmd.exe): set OPENAI_API_KEY=sk-...')
        return 2

    client = OpenAI()

    comm_db = sqlite3.connect(args.commentary_db)
    comm_db.row_factory = sqlite3.Row
    bible_db = sqlite3.connect(args.bible_db)
    bible_db.row_factory = sqlite3.Row

    targets = list_target_verses(comm_db)
    if args.limit > 0:
        targets = targets[: args.limit]

    schema = build_schema(args.zh)

    done_ok = 0
    skipped_no_verse = 0
    failed = 0

    for idx, vk in enumerate(targets, start=1):
        if args.resume and already_done(comm_db, args.model, args.prompt_version, vk):
            continue

        verse_ref = format_verse_ref(vk)

        kjv = fetch_verse_text(bible_db, "KJV", vk)
        cuvs = fetch_verse_text(bible_db, "CUVS", vk)

        if not kjv and not cuvs:
            save_result(
                comm_db,
                created_at=iso_now(),
                model=args.model,
                prompt_version=args.prompt_version,
                vk=vk,
                result_json={
                    "verse_ref": verse_ref,
                    "has_commentary": False,
                    "summary_en": "",
                    "summary_zh": "",
                    "bullet_points_en": [],
                    "bullet_points_zh": [],
                    "cited_para_ids": [],
                    "citations": [],
                    "notes": "Skipped: verse text not found in bible.db (KJV/CUVS).",
                },
                status="skipped_no_verse",
                error=None,
            )
            skipped_no_verse += 1
            continue

        evidence = fetch_evidence_paragraphs(comm_db, vk, args.max_paras_per_verse)
        if not evidence:
            # No evidence paragraphs found (should be rare if verse_mentions exists).
            save_result(
                comm_db,
                created_at=iso_now(),
                model=args.model,
                prompt_version=args.prompt_version,
                vk=vk,
                result_json={
                    "verse_ref": verse_ref,
                    "has_commentary": False,
                    "summary_en": "",
                    "summary_zh": "",
                    "bullet_points_en": [],
                    "bullet_points_zh": [],
                    "cited_para_ids": [],
                    "citations": [],
                    "notes": "No evidence paragraphs found for this verse reference.",
                },
                status="ok",
                error=None,
            )
            done_ok += 1
            continue

        # Build a compact evidence block with stable IDs for citation.
        evidence_lines = []
        for e in evidence:
            tag = f"[PARA_ID {e['para_id']} | DOC {e['doc_id']} | P{e['p_index']:06d}]"
            meta = f"{e['doc_title']} {('- ' + e['doc_author']) if e['doc_author'] else ''}".strip()
            src = e["doc_source"].strip()
            evidence_lines.append(f"{tag} {meta}\nSOURCE: {src}\nTEXT: {e['text']}\n")

        instructions = (
            "You are extracting Bible-verse-focused commentary from theological writing.\n"
            "Given a target verse reference and a set of paragraph excerpts, do BOTH:\n"
            "1) Decide whether the excerpts actually comment on the target verse(s) (false positives are common).\n"
            "2) If yes, summarize the key teaching points.\n\n"
            "Hard rules:\n"
            "- Only cite paragraphs using the provided PARA_IDs.\n"
            "- If there is no real commentary on the verse, set has_commentary=false and keep summaries minimal.\n"
        )

        user_content = (
            f"TARGET VERSE: {verse_ref}\n\n"
            f"KJV:\n{kjv or ''}\n\n"
            f"CUVS (简体和合本):\n{cuvs or ''}\n\n"
            "EVIDENCE PARAGRAPHS:\n"
            + "\n".join(evidence_lines)
        )

        # Ask for Chinese output if requested; otherwise allow empty summary_zh / bullet_points_zh.
        zh_note = (
            "If possible, provide a faithful Chinese summary (Simplified Chinese) in summary_zh and bullet_points_zh.\n"
            if args.zh
            else "Set summary_zh='' and bullet_points_zh=[].\n"
        )

        try:
            resp = client.responses.create(
                model=args.model,
                input=[
                    {"role": "system", "content": instructions + zh_note},
                    {"role": "user", "content": user_content},
                ],
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "verse_commentary_extraction",
                        "strict": True,
                        "schema": schema,
                    }
                },
            )
            out_text = resp.output_text
            result = json.loads(out_text)

            save_result(
                comm_db,
                created_at=iso_now(),
                model=args.model,
                prompt_version=args.prompt_version,
                vk=vk,
                result_json=result,
                status="ok",
                error=None,
            )
            done_ok += 1
            print(f"[{idx}/{len(targets)}] OK {verse_ref}  (paras={len(evidence)})")

        except Exception as e:
            failed += 1
            save_result(
                comm_db,
                created_at=iso_now(),
                model=args.model,
                prompt_version=args.prompt_version,
                vk=vk,
                result_json={
                    "verse_ref": verse_ref,
                    "has_commentary": False,
                    "summary_en": "",
                    "summary_zh": "",
                    "bullet_points_en": [],
                    "bullet_points_zh": [],
                    "cited_para_ids": [],
                    "citations": [],
                    "notes": "",
                },
                status="error",
                error=str(e),
            )
            print(f"[{idx}/{len(targets)}] ERROR {verse_ref}: {e}")

        time.sleep(max(0.0, args.sleep))

    print(f"Done. ok={done_ok}, skipped_no_verse={skipped_no_verse}, failed={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
