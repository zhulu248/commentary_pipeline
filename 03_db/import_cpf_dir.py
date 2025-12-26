# run example:
# python import_cpf_dir.py --db "..\commentary.db" --cpf-dir "..\01_crawl_convert\cpf_vos"

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple, Optional


RE_PARA = re.compile(r"^\[P(\d{6})\]\s*(.*)$")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_cpf(text: str) -> Tuple[Dict[str, str], str, List[Tuple[int, str]]]:
    lines = text.splitlines()

    if not lines or lines[0].strip() != "---":
        raise ValueError("CPF missing starting --- header")

    meta: Dict[str, str] = {}
    i = 1
    while i < len(lines):
        line = lines[i].rstrip("\n")
        if line.strip() == "---":
            i += 1
            break
        if ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip()
        i += 1

    engine = ""
    if i < len(lines) and lines[i].lstrip().startswith("# extraction_engine:"):
        engine = lines[i].split(":", 1)[1].strip()
        i += 1

    while i < len(lines) and not lines[i].strip():
        i += 1

    paragraphs: List[Tuple[int, str]] = []
    current_idx: Optional[int] = None
    current_text: List[str] = []

    def flush():
        nonlocal current_idx, current_text
        if current_idx is not None:
            joined = " ".join(s.strip() for s in current_text if s.strip()).strip()
            if joined:
                paragraphs.append((current_idx, joined))
        current_idx = None
        current_text = []

    for j in range(i, len(lines)):
        line = lines[j]
        m = RE_PARA.match(line)
        if m:
            flush()
            current_idx = int(m.group(1))
            current_text = [m.group(2)]
        else:
            if current_idx is not None:
                current_text.append(line)

    flush()
    return meta, engine, paragraphs


def main() -> int:
    ap = argparse.ArgumentParser(description="Import .cpf.txt files into commentary.db")
    ap.add_argument("--db", required=True, help="Path to commentary.db")
    ap.add_argument("--cpf-dir", required=True, help="Folder containing .cpf.txt files")
    ap.add_argument("--glob", default="*.cpf.txt", help="Glob pattern (default: *.cpf.txt)")
    args = ap.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    cpf_dir = Path(args.cpf_dir).expanduser().resolve()
    if not cpf_dir.exists():
        raise SystemExit(f"CPF dir not found: {cpf_dir}")

    files = sorted(cpf_dir.glob(args.glob))
    if not files:
        print(f"No CPF files matched {args.glob} in {cpf_dir}")
        return 0

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON;")
    ok = 0
    skipped = 0
    failed = 0

    try:
        for f in files:
            try:
                digest = sha256_file(f)
                row = conn.execute("SELECT id FROM documents WHERE sha256=?", (digest,)).fetchone()
                if row:
                    skipped += 1
                    continue

                text = f.read_text(encoding="utf-8", errors="strict")
                meta, engine, paras = parse_cpf(text)

                doc_type = meta.get("type", "").strip() or "article"
                title = meta.get("title", "").strip()
                author = meta.get("author", "").strip()
                source = meta.get("source", "").strip()
                extracted_at = meta.get("extracted_at", "").strip()

                cur = conn.execute(
                    """
                    INSERT INTO documents(doc_type,title,author,source,extracted_at,engine,local_path,sha256)
                    VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (doc_type, title, author, source, extracted_at, engine, str(f), digest),
                )
                doc_id = cur.lastrowid

                conn.executemany(
                    "INSERT INTO paragraphs(doc_id,p_index,text) VALUES(?,?,?)",
                    [(doc_id, p_idx, p_txt) for (p_idx, p_txt) in paras],
                )

                conn.commit()
                ok += 1

            except Exception as e:
                conn.rollback()
                failed += 1
                print(f"FAILED: {f.name}\n  Reason: {e}")

    finally:
        conn.close()

    print(f"Done. Imported: {ok}, Skipped (dedupe): {skipped}, Failed: {failed}")
    print(f"DB: {db_path}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
