# run example:
# python convert_to_cpf.py "https://www.onthewing.org/user/Doc_Biblical%20Theology%20-%20Vos.pdf" --type article --pdf-engine pymupdf -o "vos_5.cpf.txt"




#!/usr/bin/env python3
"""
convert_to_cpf.py (simple)
Convert PDF/EPUB (local file or URL) into a commentary-friendly plain-text format (CPF).

Design goals:
- Reliable text extraction + paragraphing
- Minimal metadata
- Title is a simple placeholder (filename stem / URL basename) unless user supplies --title
- No title inference / no referer parsing / no AI

PDF extraction engines (auto):
1) PyMuPDF (fitz)
2) pdfplumber
3) pypdf

CPF output:
---
type: book|article
title: <optional placeholder or user-supplied>
author: <optional>
source: <optional>
extracted_at: <timestamp>
---
# extraction_engine: <engine>

[P000001] ...
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse, unquote

# ----------------------------
# Utilities
# ----------------------------

def now_iso_local() -> str:
    return dt.datetime.now().astimezone().replace(microsecond=0).isoformat()

def is_url(s: str) -> bool:
    return s.lower().startswith(("http://", "https://"))

def guess_ext_from_url(url: str) -> str:
    m = re.search(r"\.([a-zA-Z0-9]{2,5})(?:\?|$)", url)
    return (m.group(1).lower() if m else "")

def safe_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", errors="strict")

def default_title_placeholder(input_ref: str, local_path: Optional[Path] = None) -> str:
    """
    Simple placeholder title:
    - if local_path is known: stem
    - else if URL: basename of URL path (decoded), without extension
    - else: stem of path-like string
    """
    if local_path is not None:
        return local_path.stem

    if is_url(input_ref):
        path = urlparse(input_ref).path
        name = Path(path).name
        name = unquote(name) if name else "document"
        stem = Path(name).stem or "document"
        return stem

    p = Path(input_ref)
    return p.stem or "document"

# ----------------------------
# Downloading
# ----------------------------

def download_to_temp(url: str) -> Path:
    try:
        import requests
    except ImportError:
        raise RuntimeError("Missing dependency: requests. Install with: pip install requests")

    ext = guess_ext_from_url(url)
    suffix = f".{ext}" if ext else ".bin"

    r = requests.get(url, stream=True, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    fd, tmp_name = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    tmp_path = Path(tmp_name)

    with tmp_path.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)

    return tmp_path

# ----------------------------
# PDF extraction
# ----------------------------

def extract_pdf_pages_pymupdf(pdf_path: Path) -> List[str]:
    import fitz  # PyMuPDF
    doc = fitz.open(str(pdf_path))
    pages = []
    for i in range(len(doc)):
        page = doc.load_page(i)
        pages.append(page.get_text("text") or "")
    doc.close()
    return pages

def extract_pdf_pages_pdfplumber(pdf_path: Path) -> List[str]:
    import pdfplumber
    pages = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for p in pdf.pages:
            txt = p.extract_text(x_tolerance=2, y_tolerance=2) or ""
            pages.append(txt)
    return pages

def extract_pdf_pages_pypdf(pdf_path: Path) -> List[str]:
    from pypdf import PdfReader
    reader = PdfReader(str(pdf_path))
    pages = []
    for page in reader.pages:
        pages.append(page.extract_text() or "")
    return pages

def extract_pdf_pages(pdf_path: Path, engine: str = "auto") -> Tuple[List[str], str]:
    engine = engine.lower().strip()
    if engine not in ("auto", "pymupdf", "pdfplumber", "pypdf"):
        raise ValueError("engine must be one of: auto, pymupdf, pdfplumber, pypdf")

    tried = []

    def try_engine(name: str):
        tried.append(name)
        if name == "pymupdf":
            return extract_pdf_pages_pymupdf(pdf_path)
        if name == "pdfplumber":
            return extract_pdf_pages_pdfplumber(pdf_path)
        if name == "pypdf":
            return extract_pdf_pages_pypdf(pdf_path)
        raise AssertionError("unknown engine")

    if engine == "auto":
        for name in ("pymupdf", "pdfplumber", "pypdf"):
            try:
                pages = try_engine(name)
                if "\n".join(pages).strip():
                    return pages, name
            except Exception:
                continue
        raise RuntimeError(f"Failed to extract PDF text. Tried: {tried}")
    else:
        return try_engine(engine), engine

# ----------------------------
# EPUB extraction
# ----------------------------

def extract_epub_text(epub_path: Path) -> str:
    try:
        from ebooklib import epub
    except ImportError:
        raise RuntimeError("Missing dependency: ebooklib. Install with: pip install ebooklib")

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        raise RuntimeError("Missing dependency: beautifulsoup4. Install with: pip install beautifulsoup4")

    book = epub.read_epub(str(epub_path))
    chunks: List[str] = []

    spine_ids = [item[0] for item in book.spine if isinstance(item, tuple) and item]
    spine_set = set(spine_ids)

    items = list(book.get_items())
    doc_items = [it for it in items if it.get_type() == epub.ITEM_DOCUMENT]

    def sort_key(it):
        if it.get_id() in spine_set:
            return (0, spine_ids.index(it.get_id()))
        return (1, it.get_id() or "")

    doc_items.sort(key=sort_key)

    for it in doc_items:
        html = it.get_content()
        soup = BeautifulSoup(html, "lxml")

        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        for br in soup.find_all("br"):
            br.replace_with("\n")

        text = soup.get_text("\n")
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        if text:
            chunks.append(text)

    return "\n\n".join(chunks).strip()

# ----------------------------
# Text cleanup + paragraphing
# ----------------------------

def _top_bottom_signature(lines: List[str], n: int = 2) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    nonempty = [ln.strip() for ln in lines if ln.strip()]
    top = tuple(nonempty[:n])
    bottom = tuple(nonempty[-n:]) if len(nonempty) >= n else tuple(nonempty)
    return top, bottom

def remove_repeated_headers_footers(pages: List[str], threshold: float = 0.6, n: int = 2) -> List[str]:
    from collections import Counter

    tops = Counter()
    bottoms = Counter()

    page_lines = []
    for t in pages:
        lines = t.splitlines()
        page_lines.append(lines)
        top, bottom = _top_bottom_signature(lines, n=n)
        for ln in top:
            tops[ln] += 1
        for ln in bottom:
            bottoms[ln] += 1

    page_count = max(1, len(pages))
    top_remove = {ln for ln, c in tops.items() if c / page_count >= threshold and len(ln) <= 140}
    bottom_remove = {ln for ln, c in bottoms.items() if c / page_count >= threshold and len(ln) <= 140}

    cleaned_pages = []
    for lines in page_lines:
        new_lines = []
        for ln in lines:
            s = ln.strip()
            if not s:
                new_lines.append("")
                continue
            if s in top_remove or s in bottom_remove:
                continue
            new_lines.append(ln)
        cleaned_pages.append("\n".join(new_lines))
    return cleaned_pages

def normalize_linebreaks(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)

    # Fix hyphenation at line breaks: letter- \n letter(lowercase) => merge
    text = re.sub(r"([A-Za-z])-\n([a-z])", r"\1\2", text)

    text = re.sub(r"\n{3,}", "\n\n", text)

    # Convert single newlines within paragraphs into spaces (keep blank-line breaks)
    text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)

    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r" *\n\n *", "\n\n", text).strip()

    return text

def split_into_paragraphs(text: str, min_chars: int = 20) -> List[str]:
    paras = [p.strip() for p in text.split("\n\n")]
    cleaned = []
    for p in paras:
        p = re.sub(r"\s+", " ", p).strip()
        if len(p) >= min_chars:
            cleaned.append(p)
    return cleaned

# ----------------------------
# CPF builder
# ----------------------------

@dataclass
class CPFMeta:
    type: str
    title: str = ""
    author: str = ""
    source: str = ""
    extracted_at: str = ""

def build_cpf(meta: CPFMeta, paragraphs: List[str], engine_note: str) -> str:
    header_lines = [
        "---",
        f"type: {meta.type.strip()}",
    ]
    if meta.title:
        header_lines.append(f"title: {meta.title.strip()}")
    if meta.author:
        header_lines.append(f"author: {meta.author.strip()}")
    if meta.source:
        header_lines.append(f"source: {meta.source.strip()}")
    header_lines.append(f"extracted_at: {meta.extracted_at or now_iso_local()}")
    header_lines.append("---\n")

    header_lines.append(f"# extraction_engine: {engine_note}\n")

    body_lines = []
    for i, p in enumerate(paragraphs, start=1):
        body_lines.append(f"[P{i:06d}] {p}")

    return "\n".join(header_lines + body_lines).rstrip() + "\n"

# ----------------------------
# Main conversion logic
# ----------------------------

def convert(
    input_ref: str,
    out_path: Path,
    doc_type: str,
    title: str,
    author: str,
    source: str,
    pdf_engine: str,
    keep_temp: bool,
    min_para_chars: int,
    referer_url: str = "",  # accepted for compatibility; ignored
) -> None:
    tmp_path: Optional[Path] = None
    try:
        if is_url(input_ref):
            tmp_path = download_to_temp(input_ref)
            in_path = tmp_path
            inferred_source = input_ref
        else:
            in_path = Path(input_ref).expanduser().resolve()
            inferred_source = str(in_path)

        ext = in_path.suffix.lower().lstrip(".")
        if ext not in ("pdf", "epub"):
            raise ValueError(f"Unsupported input type: .{ext} (supported: .pdf, .epub)")

        used_engine = ""
        raw_text = ""

        if ext == "pdf":
            pages_raw, used_engine = extract_pdf_pages(in_path, engine=pdf_engine)
            pages = remove_repeated_headers_footers(pages_raw, threshold=0.6, n=2)
            raw_text = "\n\n".join(pages).strip()
        else:
            used_engine = "epub"
            raw_text = extract_epub_text(in_path)

        cleaned = normalize_linebreaks(raw_text)
        paragraphs = split_into_paragraphs(cleaned, min_chars=min_para_chars)

        # Title placeholder only (no inference)
        final_title = (title or "").strip()
        if not final_title:
            final_title = default_title_placeholder(input_ref, local_path=None if is_url(input_ref) else in_path)

        meta = CPFMeta(
            type=doc_type,
            title=final_title,
            author=(author or "").strip(),
            source=(source or inferred_source),
            extracted_at=now_iso_local(),
        )

        cpf_text = build_cpf(meta, paragraphs, engine_note=used_engine)
        safe_write_text(out_path, cpf_text)

        print(f"OK: wrote {out_path}")
        print(f"Paragraphs: {len(paragraphs)}")
        print(f"Engine: {used_engine}")
        print(f"Title (placeholder): {meta.title}")

    finally:
        if tmp_path and (not keep_temp):
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Convert PDF/EPUB (local or URL) into CPF (Commentary Plain Format)."
    )
    ap.add_argument("input", help="Path to .pdf/.epub, or a URL to a PDF/EPUB")
    ap.add_argument("-o", "--out", default="", help="Output .cpf.txt path (default: input basename + .cpf.txt)")
    ap.add_argument("--type", required=True, choices=["book", "article"], help="Declare whether input is a book or article")
    ap.add_argument("--title", default="", help="Title metadata (optional placeholder)")
    ap.add_argument("--author", default="", help="Author metadata")
    ap.add_argument("--source", default="", help="Source metadata (URL or citation string)")
    ap.add_argument("--pdf-engine", default="auto", choices=["auto", "pymupdf", "pdfplumber", "pypdf"],
                    help="PDF extraction engine (auto recommended)")
    ap.add_argument("--min-para-chars", type=int, default=20, help="Drop paragraphs shorter than this (filters noise)")
    ap.add_argument("--keep-temp", action="store_true", help="If input is a URL, keep the downloaded temp file")

    args = ap.parse_args()

    input_ref = args.input
    if args.out.strip():
        out_path = Path(args.out).expanduser().resolve()
    else:
        if is_url(input_ref):
            ext = guess_ext_from_url(input_ref) or "pdf"
            base = Path(f"downloaded.{ext}")
        else:
            base = Path(input_ref).expanduser().resolve()
        out_path = base.with_suffix("").with_suffix(".cpf.txt")

    convert(
        input_ref=input_ref,
        out_path=out_path,
        doc_type=args.type,
        title=args.title,
        author=args.author,
        source=args.source,
        pdf_engine=args.pdf_engine,
        keep_temp=args.keep_temp,
        min_para_chars=args.min_para_chars,
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
