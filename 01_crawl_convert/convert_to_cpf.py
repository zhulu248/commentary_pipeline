# run example:
# PDF URL -> CPF
# python convert_to_cpf.py "https://www.onthewing.org/user/Doc_Biblical%20Theology%20-%20Vos.pdf" --type book --mode auto --pdf-engine pymupdf -o "vos.cpf.txt"
#
# Local PDF -> CPF
# python convert_to_cpf.py "C:\Users\zhulu\OneDrive\Desktop\some.pdf" --type article --pdf-engine pymupdf -o "some.cpf.txt"
#
# Webpage -> CPF (simple)
# python convert_to_cpf.py "https://credomag.com/2012/09/the-father-of-reformed-biblical-theology-geerhardus-vos-1862-1949/" --type article --mode web -o "vos_article.cpf.txt"
#
# Webpage that blocks requests -> CPF (auto falls back if Playwright installed)
# python convert_to_cpf.py "https://www.monergism.com/second-coming-our-lord-and-millennium" --type article --mode web --fetch auto -o "monergism.cpf.txt"
#
# If needed for blocked sites:
# pip install playwright
# playwright install chromium
# python convert_to_cpf.py "https://www.monergism.com/second-coming-our-lord-and-millennium" --type article --mode web --fetch playwright -o "monergism.cpf.txt"

#!/usr/bin/env python3
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

# ============================================================
# Common utilities
# ============================================================

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

def default_title_from_url(url: str) -> str:
    path = urlparse(url).path
    name = Path(path).name
    name = unquote(name) if name else "document"
    stem = Path(name).stem or "document"
    return stem

def default_title_placeholder(input_ref: str, local_path: Optional[Path] = None) -> str:
    if local_path is not None:
        return local_path.stem
    if is_url(input_ref):
        return default_title_from_url(input_ref)
    p = Path(input_ref)
    return p.stem or "document"

def sanitize_filename(name: str, max_len: int = 140) -> str:
    name = re.sub(r"\s+", " ", (name or "").strip())
    if not name:
        name = "document"
    name = re.sub(r'[<>:"/\\\\|?*]', "", name)  # Windows-illegal chars
    name = name.rstrip(" .")
    if not name:
        name = "document"
    if len(name) > max_len:
        name = name[:max_len].rstrip(" .")
    return name

# ============================================================
# CPF builder
# ============================================================

@dataclass
class CPFMeta:
    type: str
    title: str = ""
    author: str = ""
    source: str = ""
    extracted_at: str = ""

def build_cpf(meta: CPFMeta, paragraphs: List[str], engine_note: str) -> str:
    header = ["---", f"type: {meta.type.strip()}"]
    if meta.title:
        header.append(f"title: {meta.title.strip()}")
    if meta.author:
        header.append(f"author: {meta.author.strip()}")
    if meta.source:
        header.append(f"source: {meta.source.strip()}")
    header.append(f"extracted_at: {meta.extracted_at or now_iso_local()}")
    header.append("---\n")
    header.append(f"# extraction_engine: {engine_note}\n")

    body = [f"[P{i:06d}] {p}" for i, p in enumerate(paragraphs, start=1)]
    return "\n".join(header + body).rstrip() + "\n"

# ============================================================
# Text cleanup + paragraphing (shared)
# ============================================================

def normalize_linebreaks(text: str) -> str:
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    # Fix hyphenation at line breaks: letter- \n letter(lowercase) => merge
    text = re.sub(r"([A-Za-z])-\n([a-z])", r"\1\2", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    # single newline inside paragraph -> space
    text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r" *\n\n *", "\n\n", text).strip()
    return text

def split_into_paragraphs(text: str, min_chars: int = 20) -> List[str]:
    paras = [p.strip() for p in (text or "").split("\n\n")]
    out: List[str] = []
    for p in paras:
        p = re.sub(r"\s+", " ", p).strip()
        if len(p) >= min_chars:
            out.append(p)
    return out

# ============================================================
# PDF/EPUB conversion
# ============================================================

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

def convert_pdf_or_epub(
    input_ref: str,
    out_path: Path,
    doc_type: str,
    title: str,
    author: str,
    source: str,
    pdf_engine: str,
    keep_temp: bool,
    min_para_chars: int,
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

        cpf_text = build_cpf(meta, paragraphs, engine_note=f"pdf/epub:{used_engine}")
        safe_write_text(out_path, cpf_text)

        print(f"OK: wrote {out_path}")
        print(f"Paragraphs: {len(paragraphs)}")
        print(f"Engine: pdf/epub:{used_engine}")
        print(f"Title (placeholder): {meta.title}")

    finally:
        if tmp_path and (not keep_temp):
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

# ============================================================
# Webpage conversion
# ============================================================

# IMPORTANT: Do NOT request "br" (Brotli) here.
WEB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

def _decode_response_text(r) -> str:
    content_encoding = (r.headers.get("Content-Encoding") or "").lower().strip()
    raw = r.content

    if content_encoding == "br":
        try:
            try:
                import brotlicffi as brotli
            except Exception:
                import brotli  # type: ignore
            raw = brotli.decompress(raw)
        except Exception as e:
            raise RuntimeError(
                "Server returned Brotli-compressed HTML (Content-Encoding: br) but Brotli decode failed.\n"
                "Install one of:\n"
                "  pip install brotlicffi\n"
                "  (or) pip install brotli\n"
                f"Underlying error: {e}"
            )

    enc = (r.encoding or "").strip()
    if not enc:
        try:
            enc = r.apparent_encoding or "utf-8"
        except Exception:
            enc = "utf-8"
    try:
        return raw.decode(enc, errors="replace")
    except Exception:
        return raw.decode("utf-8", errors="replace")

def fetch_url_requests(url: str, timeout: int = 60) -> Tuple[str, int]:
    try:
        import requests
    except ImportError:
        raise RuntimeError("Missing dependency: requests. Install with: pip install requests")

    s = requests.Session()
    r = s.get(url, timeout=timeout, headers=WEB_HEADERS, allow_redirects=True)
    return _decode_response_text(r), r.status_code

def fetch_url_playwright(url: str, timeout: int = 60) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError(
            "Playwright not installed. Install:\n"
            "  pip install playwright\n"
            "  playwright install chromium"
        )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=WEB_HEADERS["User-Agent"], locale="en-US")
        page = context.new_page()
        page.set_default_navigation_timeout(timeout * 1000)
        page.goto(url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=timeout * 1000)
        except Exception:
            pass
        html = page.content()
        context.close()
        browser.close()
        return html

def fetch_url(url: str, mode: str, timeout: int = 60) -> Tuple[str, str]:
    mode = (mode or "auto").lower().strip()
    if mode not in ("auto", "requests", "playwright"):
        raise ValueError("fetch mode must be one of: auto, requests, playwright")

    if mode == "requests":
        html, status = fetch_url_requests(url, timeout=timeout)
        if status >= 400:
            raise RuntimeError(f"HTTP {status} for url: {url}")
        return html, "requests"

    if mode == "playwright":
        return fetch_url_playwright(url, timeout=timeout), "playwright"

    # auto
    html, status = fetch_url_requests(url, timeout=timeout)
    if status in (401, 403, 429):
        try:
            return fetch_url_playwright(url, timeout=timeout), "playwright"
        except Exception:
            raise RuntimeError(
                f"HTTP {status} (blocked) for url: {url}. "
                f"Install Playwright and rerun with --fetch playwright.\n"
                f"  pip install playwright\n  playwright install chromium"
            )
    if status >= 400:
        raise RuntimeError(f"HTTP {status} for url: {url}")
    return html, "requests"

def extract_with_trafilatura(url: str, html: str) -> tuple[str, str, str]:
    import trafilatura
    from trafilatura.metadata import extract_metadata

    meta = extract_metadata(html, url=url)
    title = (meta.title or "").strip() if meta else ""
    author = (meta.author or "").strip() if meta else ""

    main_text = trafilatura.extract(
        html,
        url=url,
        output_format="txt",
        include_comments=False,
        include_tables=False,
        favor_precision=True,
    ) or ""

    return title, author, main_text

def extract_with_readability(html: str) -> tuple[str, str]:
    from readability import Document
    from bs4 import BeautifulSoup

    doc = Document(html)
    title = (doc.short_title() or "").strip()

    content_html = doc.summary(html_partial=True) or ""
    soup = BeautifulSoup(content_html, "lxml")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    for br in soup.find_all("br"):
        br.replace_with("\n")

    text = soup.get_text("\n")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return title, text

def convert_webpage(
    url: str,
    out_path: Path,
    doc_type: str,
    title: str,
    author: str,
    source: str,
    extract_engine: str,
    fetch_mode: str,
    timeout: int,
    min_para_chars: int,
) -> None:
    html, fetch_note = fetch_url(url, mode=fetch_mode, timeout=timeout)

    used_extract = ""
    extracted_title = ""
    extracted_author = ""
    extracted_text = ""

    extract_engine = (extract_engine or "auto").lower().strip()
    if extract_engine not in ("auto", "trafilatura", "readability"):
        raise ValueError("extract engine must be one of: auto, trafilatura, readability")

    if extract_engine in ("auto", "trafilatura"):
        try:
            t, a, txt = extract_with_trafilatura(url, html)
            if txt.strip():
                used_extract = "trafilatura"
                extracted_title, extracted_author, extracted_text = t, a, txt
        except Exception:
            pass

    if not extracted_text.strip():
        t2, txt2 = extract_with_readability(html)
        used_extract = "readability"
        extracted_title, extracted_text = t2, txt2

    cleaned = normalize_linebreaks(extracted_text)
    paragraphs = split_into_paragraphs(cleaned, min_chars=min_para_chars)

    final_title = (title or "").strip() or extracted_title.strip() or default_title_from_url(url)
    final_author = (author or "").strip() or extracted_author.strip()
    final_source = (source or "").strip() or url

    meta = CPFMeta(
        type=doc_type,
        title=final_title,
        author=final_author,
        source=final_source,
        extracted_at=now_iso_local(),
    )

    engine_note = f"web:fetch={fetch_note};extract={used_extract}"
    cpf_text = build_cpf(meta, paragraphs, engine_note=engine_note)
    safe_write_text(out_path, cpf_text)

    print(f"OK: wrote {out_path}")
    print(f"Paragraphs: {len(paragraphs)}")
    print(f"Engine: {engine_note}")
    print(f"Title: {meta.title}")

# ============================================================
# Combined CLI + auto-detection
# ============================================================

def detect_mode(input_ref: str) -> str:
    """
    Returns: 'pdf', 'epub', or 'web'
    """
    if is_url(input_ref):
        ext = guess_ext_from_url(input_ref)
        if ext in ("pdf", "epub"):
            return ext
        return "web"
    # local file
    p = Path(input_ref)
    ext = p.suffix.lower().lstrip(".")
    if ext in ("pdf", "epub"):
        return ext
    return "web"  # if user passes a local HTML file, they should use --mode web and --fetch requests won't apply

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Convert PDF/EPUB (local or URL) OR a webpage URL into CPF."
    )
    ap.add_argument("input", help="PDF/EPUB path or URL, or a webpage URL")
    ap.add_argument("--type", required=True, choices=["book", "article"], help="Declare whether it is a book or article")
    ap.add_argument("--mode", default="auto", choices=["auto", "pdf", "epub", "web"],
                    help="Override auto-detection")
    ap.add_argument("-o", "--out", default="", help="Output .cpf.txt path (overrides --outdir)")
    ap.add_argument("--outdir", default="", help="Output folder (default: current folder)")
    ap.add_argument("--title", default="", help="Optional title override (otherwise placeholder)")
    ap.add_argument("--author", default="", help="Optional author override")
    ap.add_argument("--source", default="", help="Optional source override (default: URL or local path)")
    ap.add_argument("--min-para-chars", type=int, default=20, help="Drop paragraphs shorter than this (filters noise)")

    # PDF/EPUB options
    ap.add_argument("--pdf-engine", default="auto", choices=["auto", "pymupdf", "pdfplumber", "pypdf"],
                    help="PDF extraction engine (auto recommended)")
    ap.add_argument("--keep-temp", action="store_true", help="If input is a URL to a file, keep downloaded temp file")

    # Web options
    ap.add_argument("--engine", default="auto", choices=["auto", "trafilatura", "readability"],
                    help="Web article extraction engine")
    ap.add_argument("--fetch", default="auto", choices=["auto", "requests", "playwright"],
                    help="Web fetch mode (requests/playwright/auto fallback)")
    ap.add_argument("--timeout", type=int, default=60, help="Web fetch timeout seconds")

    args = ap.parse_args()

    input_ref = args.input

    mode = args.mode
    if mode == "auto":
        mode = detect_mode(input_ref)

    # out path
    if args.out.strip():
        out_path = Path(args.out).expanduser().resolve()
    else:
        outdir = Path(args.outdir).expanduser().resolve() if args.outdir.strip() else Path.cwd()
        # Use a safe filename based on placeholder title
        placeholder = sanitize_filename(args.title.strip() or default_title_placeholder(input_ref))
        out_path = outdir / f"{placeholder}.cpf.txt"

    if mode in ("pdf", "epub"):
        convert_pdf_or_epub(
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
    elif mode == "web":
        if not is_url(input_ref):
            raise RuntimeError(
                "Web mode expects a URL (http/https). "
                "If you intended a local PDF/EPUB, set --mode pdf/epub."
            )
        convert_webpage(
            url=input_ref,
            out_path=out_path,
            doc_type=args.type,
            title=args.title,
            author=args.author,
            source=args.source,
            extract_engine=args.engine,
            fetch_mode=args.fetch,
            timeout=args.timeout,
            min_para_chars=args.min_para_chars,
        )
    else:
        raise RuntimeError(f"Unknown mode: {mode}")

    return 0


if __name__ == "__main__":
    import sys
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)

