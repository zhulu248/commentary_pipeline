#!/usr/bin/env python3
"""
Rename *.cpf.txt files using the CPF metadata title: line.

- Keeps spaces
- Removes special characters (only letters, digits, space, dash, underscore)
- Avoids name collisions by adding " (2)", " (3)", ...
"""

from __future__ import annotations
import re
import sys
from pathlib import Path

ALLOWED = re.compile(r"[^A-Za-z0-9 _-]+")

def extract_title(cpf_path: Path) -> str:
    # Read only the first chunk; title is in the header.
    text = cpf_path.read_text(encoding="utf-8", errors="replace")
    # Find a line like: title: Something
    m = re.search(r"(?m)^\s*title:\s*(.+?)\s*$", text)
    return m.group(1).strip() if m else ""

def sanitize_filename(title: str, max_len: int = 160) -> str:
    # Replace special chars with space; keep spaces
    s = ALLOWED.sub(" ", title)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s

def unique_path(dirpath: Path, base: str, suffix: str) -> Path:
    candidate = dirpath / f"{base}{suffix}"
    if not candidate.exists():
        return candidate
    k = 2
    while True:
        candidate = dirpath / f"{base} ({k}){suffix}"
        if not candidate.exists():
            return candidate
        k += 1

def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python rename_cpf_from_title.py <cpf_output_folder>", file=sys.stderr)
        return 2

    outdir = Path(sys.argv[1]).resolve()
    if not outdir.exists() or not outdir.is_dir():
        print(f"Not a folder: {outdir}", file=sys.stderr)
        return 2

    files = sorted(outdir.glob("*.cpf.txt"))
    if not files:
        print("No *.cpf.txt files found.")
        return 0

    renamed = 0
    skipped = 0

    for f in files:
        title = extract_title(f)
        base = sanitize_filename(title)
        if not base:
            skipped += 1
            continue

        new_path = unique_path(outdir, base, ".cpf.txt")
        if new_path.name == f.name:
            skipped += 1
            continue

        f.rename(new_path)
        renamed += 1
        print(f'Renamed: "{f.name}" -> "{new_path.name}"')

    print(f"Done. Renamed: {renamed}, Skipped: {skipped}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
