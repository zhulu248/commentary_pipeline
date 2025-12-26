# run example:
# python run_batch_for_names.py --names-file names.txt
# python run_batch_for_names.py --names-file names.txt --engine pymupdf --max-pages 50 --delay 0.5
# python run_batch_for_names.py --names-file names.txt --same-domain

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote


def slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s or "unknown"


def build_search_url(name: str) -> str:
    return f"https://www.monergism.com/search?keywords={quote(name)}&format=All"


def parse_name_line(line: str) -> str | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    # Remove wrapping quotes if present
    if (line.startswith('"') and line.endswith('"')) or (line.startswith("'") and line.endswith("'")):
        line = line[1:-1].strip()

    line = re.sub(r"\s+", " ", line).strip()
    return line or None


def read_names_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Names file not found: {path}")

    names: list[str] = []
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        name = parse_name_line(raw)
        if name:
            names.append(name)
    return names


def main() -> int:
    ap = argparse.ArgumentParser(description="Run batch_convert_from_page.py for multiple names read from a file.")
    ap.add_argument("--names-file", required=True, help='Path to names file (one name per line, e.g. "John Calvin")')
    ap.add_argument("--type", default="article", choices=["book", "article"], help="Passed to batch script")
    ap.add_argument("--engine", default="pymupdf", choices=["auto", "pymupdf", "pdfplumber", "pypdf"],
                    help="--pdf-engine passed to batch script")
    ap.add_argument("--all-pages", action="store_true", default=True, help="Enable --all-pages (default: on)")
    ap.add_argument("--max-pages", type=int, default=50, help="--max-pages passed to batch script")
    ap.add_argument("--delay", type=float, default=0.5, help="--delay passed to batch script")
    ap.add_argument("--same-domain", action="store_true", help="Pass --same-domain to batch script")
    ap.add_argument("--scripts-dir", default=".", help="Folder containing batch_convert_from_page.py")
    args = ap.parse_args()

    scripts_dir = Path(args.scripts_dir).expanduser().resolve()
    batch_script = scripts_dir / "batch_convert_from_page.py"
    if not batch_script.exists():
        print(f"ERROR: Cannot find {batch_script}", file=sys.stderr)
        return 2

    names_path = Path(args.names_file).expanduser().resolve()
    try:
        names = read_names_file(names_path)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    if not names:
        print("No names found in names file.", file=sys.stderr)
        return 2

    for name in names:
        outdir = f"cpf_{slug(name)}"
        url = build_search_url(name)

        cmd = [
            sys.executable, str(batch_script),
            url,
            "--type", args.type,
            "--outdir", outdir,
            "--pdf-engine", args.engine,
            "--max-pages", str(args.max_pages),
            "--delay", str(args.delay),
        ]

        if args.all_pages:
            cmd.append("--all-pages")
        if args.same_domain:
            cmd.append("--same-domain")

        print("\n" + "=" * 70)
        print(f"NAME: {name}")
        print(f"URL : {url}")
        print(f"OUT : {outdir}")
        print("CMD :", " ".join(cmd))
        print("=" * 70)

        rc = subprocess.call(cmd)
        if rc != 0:
            print(f"WARNING: Batch run for '{name}' exited with code {rc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
