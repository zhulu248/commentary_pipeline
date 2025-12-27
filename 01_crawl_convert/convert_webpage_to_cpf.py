# run example:
# python convert_webpage_to_cpf.py "https://kerux.com/doc/0102A1.html" --type article -o "kerux_0102A1.cpf.txt" --fetch requests
# python convert_webpage_to_cpf.py "https://www.monergism.com/second-coming-our-lord-and-millennium" --type article -o "monergism_vos.cpf.txt" --fetch auto
#
# If a site blocks requests (401/403/429), use browser fetch:
# pip install playwright
# playwright install chromium
# python convert_webpage_to_cpf.py "https://www.monergism.com/second-coming-our-lord-and-millennium" --type article -o "monergism_vos.cpf.txt" --fetch playwright
#
# If a site returns Brotli (br) anyway and requests can't decode:
# pip install brotlicffi
# (or) pip install brotli

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse, unquote

# add this for testing github
# add this line from github

# ----------------------------
# Utilities
# ----------------------------

def now_iso_local() -> str:
    return dt.datetime.now().astimezone().replace(microsecond=0).isoformat()

def default_title_from_url(url: str) -> str:
    path = urlparse(url).path
    name = Path(path).name
    name = unquote(name) if name else "document"
    stem = Path(name).stem or "document"
    return stem

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

def safe_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", errors="strict")

# ----------------------------
# Text cleanup + paragraphing
# ----------------------------

def normalize_linebreaks(text: str) -> str:
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)  # single newline -> space
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

# ----------------------------
# Fetchers
# ----------------------------

# IMPORTANT: Do NOT request "br" here; some sites send Brotli and older stacks decode poorly.
DEFAULT_HEADERS = {
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
    """
    Decode response safely, including optional Brotli.
    """
    # requests normally auto-decompresses gzip/deflate.
    content_encoding = (r.headers.get("Content-Encoding") or "").lower().strip()
    raw = r.content

    # If server still sends Brotli, try to decode it if brotli is available.
    if content_encoding == "br":
        try:
            try:
                import brotlicffi as brotli  # preferred on Windows
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

    # Pick encoding
    enc = (r.encoding or "").strip()
    if not enc:
        # If server didn’t specify, use requests’ guess
        try:
            enc = r.apparent_encoding or "utf-8"
        except Exception:
            enc = "utf-8"

    try:
        return raw.decode(enc, errors="replace")
    except Exception:
        return raw.decode("utf-8", errors="replace")

def fetch_url_requests(url: str, timeout: int = 60) -> Tuple[str, int]:
    import requests
    s = requests.Session()
    r = s.get(url, timeout=timeout, headers=DEFAULT_HEADERS, allow_redirects=True)
    text = _decode_response_text(r)
    return text, r.status_code

def fetch_url_playwright(url: str, timeout: int = 60) -> str:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=DEFAULT_HEADERS["User-Agent"], locale="en-US")
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
        html = fetch_url_playwright(url, timeout=timeout)
        return html, "playwright"

    # auto
    html, status = fetch_url_requests(url, timeout=timeout)
    if status in (401, 403, 429):
        try:
            html = fetch_url_playwright(url, timeout=timeout)
            return html, "playwright"
        except Exception:
            raise RuntimeError(
                f"HTTP {status} (blocked) for url: {url}. "
                f"Install Playwright and rerun with --fetch playwright.\n"
                f"  pip install playwright\n  playwright install chromium"
            )
    if status >= 400:
        raise RuntimeError(f"HTTP {status} for url: {url}")
    return html, "requests"

# ----------------------------
# Extraction engines
# ----------------------------

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

# ----------------------------
# Main conversion
# ----------------------------

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
        raise ValueError("engine must be one of: auto, trafilatura, readability")

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

    engine_note = f"fetch={fetch_note}; extract={used_extract}"
    cpf_text = build_cpf(meta, paragraphs, engine_note=engine_note)
    safe_write_text(out_path, cpf_text)

    print(f"OK: wrote {out_path}")
    print(f"Paragraphs: {len(paragraphs)}")
    print(f"Engine: {engine_note}")
    print(f"Title: {meta.title}")

def main() -> int:
    ap = argparse.ArgumentParser(description="Convert a single webpage URL into CPF.")
    ap.add_argument("url", help="Webpage URL")
    ap.add_argument("--type", required=True, choices=["book", "article"])
    ap.add_argument("--title", default="")
    ap.add_argument("--author", default="")
    ap.add_argument("--source", default="")
    ap.add_argument("--engine", default="auto", choices=["auto", "trafilatura", "readability"])
    ap.add_argument("--fetch", default="auto", choices=["auto", "requests", "playwright"])
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--min-para-chars", type=int, default=20)
    ap.add_argument("-o", "--out", default="")
    ap.add_argument("--outdir", default="")

    args = ap.parse_args()

    if args.out.strip():
        out_path = Path(args.out).expanduser().resolve()
    else:
        outdir = Path(args.outdir).expanduser().resolve() if args.outdir.strip() else Path.cwd()
        placeholder = sanitize_filename(args.title.strip() or default_title_from_url(args.url))
        out_path = outdir / f"{placeholder}.cpf.txt"

    convert_webpage(
        url=args.url,
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
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
