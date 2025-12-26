# run example:
# python batch_convert_from_page.py "https://www.monergism.com/search?keywords=vos&format=46" --type article --outdir cpf_vos --pdf-engine pymupdf --all-pages
# python batch_convert_from_page.py "https://www.monergism.com/search?keywords=vos&format=46" --type article --outdir cpf_vos --pdf-engine auto --all-pages --same-domain
# python batch_convert_from_page.py "https://www.monergism.com/search?keywords=vos&format=46" --type article --outdir cpf_vos --pdf-engine pymupdf --all-pages --title-prefix "Vos - "

#!/usr/bin/env python3
"""
batch_convert_from_page.py (v3)

Given a webpage URL, find PDF links on that page (optionally across pagination)
and convert each PDF to CPF by invoking convert_to_cpf.py via subprocess.

Why subprocess?
- Works with the *combined* convert_to_cpf.py (PDF/EPUB/web in one CLI)
- Avoids import/API compatibility issues

Requires:
  pip install requests beautifulsoup4 lxml
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import (
    urljoin,
    urldefrag,
    urlparse,
    parse_qs,
    urlencode,
    urlunparse,
)

import requests
from bs4 import BeautifulSoup


FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    # Avoid requesting Brotli; keeps HTML decoding simpler on some sites
    "Accept-Encoding": "gzip, deflate",
}


def sanitize_filename(name: str, default: str = "document.pdf") -> str:
    name = name.strip() or default
    name = re.sub(r"[<>:\"/\\|?*\x00-\x1F]+", "_", name)  # Windows-safe
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180].rstrip()


def is_pdf_url(url: str) -> bool:
    u = url.lower()
    return u.endswith(".pdf") or ".pdf?" in u or ".pdf#" in u


def same_domain(a: str, b: str) -> bool:
    return urlparse(a).netloc.lower() == urlparse(b).netloc.lower()


def fetch_url(url: str) -> str:
    r = requests.get(url, timeout=60, headers=FETCH_HEADERS, allow_redirects=True)
    r.raise_for_status()
    return r.text


def extract_pdf_links(page_url: str, html: str, only_same_domain: bool) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    links: set[str] = set()

    # <a href="...pdf">
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(page_url, href)
        full, _ = urldefrag(full)
        if is_pdf_url(full):
            if (not only_same_domain) or same_domain(page_url, full):
                links.add(full)

    # PDFs embedded via iframe/object/embed
    for tag, attr in [("iframe", "src"), ("embed", "src"), ("object", "data")]:
        for el in soup.select(f"{tag}[{attr}]"):
            src = (el.get(attr) or "").strip()
            if not src:
                continue
            full = urljoin(page_url, src)
            full, _ = urldefrag(full)
            if is_pdf_url(full):
                if (not only_same_domain) or same_domain(page_url, full):
                    links.add(full)

    return sorted(links)


def pdf_basename_from_url(pdf_url: str) -> str:
    path = urlparse(pdf_url).path
    name = Path(path).name or "document.pdf"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return sanitize_filename(name)


def set_query_param(url: str, key: str, value: str) -> str:
    u = urlparse(url)
    q = parse_qs(u.query, keep_blank_values=True)
    q[key] = [value]
    new_query = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, ""))


def discover_last_page_index_same_path(base_url: str, html: str) -> int | None:
    """
    Looks for links on the same PATH that have ?page=..., returns max(page).
    Works well on Monergism search pagination.
    """
    soup = BeautifulSoup(html, "lxml")
    base = urlparse(base_url)
    max_idx: int | None = None

    for a in soup.select('a[href*="page="]'):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        full, _ = urldefrag(full)
        u = urlparse(full)

        # Only consider pagination links that point to the same path
        if u.scheme and (u.scheme != base.scheme):
            continue
        if u.netloc and (u.netloc.lower() != base.netloc.lower()):
            continue
        if u.path != base.path:
            continue

        q = parse_qs(u.query)
        if "page" in q:
            try:
                idx = int(q["page"][0])
                max_idx = idx if (max_idx is None or idx > max_idx) else max_idx
            except Exception:
                pass

    return max_idx


def iter_pages(start_url: str, all_pages: bool, max_pages: int, delay: float) -> list[str]:
    """
    If all_pages=False: returns [start_url].
    If all_pages=True: fetches first page, discovers last page index, then builds URLs.
    Falls back to incrementing pages until it hits an empty page or max_pages.
    """
    if not all_pages:
        return [start_url]

    urls = []
    first_html = fetch_url(start_url)
    urls.append(start_url)

    last_idx = discover_last_page_index_same_path(start_url, first_html)

    # If we can see a last page index, generate full list deterministically
    if last_idx is not None:
        # Monergism commonly uses page=1 for “page 2”, so total pages = last_idx + 1
        for idx in range(1, min(last_idx + 1, max_pages)):
            urls.append(set_query_param(start_url, "page", str(idx)))
        return urls

    # Fallback: try page=1,2,... until stop condition
    for idx in range(1, max_pages):
        time.sleep(delay)
        u = set_query_param(start_url, "page", str(idx))
        html = fetch_url(u)
        if not extract_pdf_links(u, html, only_same_domain=False):
            break
        urls.append(u)

    return urls


def run_converter(
    converter_path: Path,
    pdf_url: str,
    out_path: Path,
    doc_type: str,
    pdf_engine: str,
    min_para_chars: int,
    title: str,
) -> None:
    """
    Invoke the *combined* convert_to_cpf.py via subprocess.
    """
    cmd = [
        sys.executable,
        str(converter_path),
        pdf_url,
        "--type",
        doc_type,
        "--mode",
        "auto",
        "--pdf-engine",
        pdf_engine,
        "--min-para-chars",
        str(min_para_chars),
        "--title",
        title,
        "--source",
        pdf_url,
        "-o",
        str(out_path),
    ]
    subprocess.run(cmd, check=True)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Find PDF links on a webpage (optionally across pages) and convert to CPF."
    )
    ap.add_argument("page_url", help="Webpage URL to scan for PDF links")
    ap.add_argument("--type", required=True, choices=["book", "article"], help="Metadata only (book/article)")
    ap.add_argument("--outdir", default="cpf_out", help="Output directory for CPF files")
    ap.add_argument("--pdf-engine", default="auto", choices=["auto", "pymupdf", "pdfplumber", "pypdf"])
    ap.add_argument("--min-para-chars", type=int, default=20)
    ap.add_argument("--same-domain", action="store_true", help="Only keep PDFs on the same domain as the start page")
    ap.add_argument("--title-prefix", default="", help="Prefix added to generated placeholder titles")
    ap.add_argument("--all-pages", action="store_true", help="Auto-crawl pagination (?page=N) on the same path")
    ap.add_argument("--max-pages", type=int, default=50, help="Safety cap when using --all-pages")
    ap.add_argument("--delay", type=float, default=0.5, help="Delay between page fetches when crawling pages")
    ap.add_argument(
        "--converter",
        default="",
        help='Path to convert_to_cpf.py (default: "convert_to_cpf.py" in the same folder as this script)',
    )
    args = ap.parse_args()

    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    # Locate converter script
    if args.converter.strip():
        converter_path = Path(args.converter).expanduser().resolve()
    else:
        converter_path = Path(__file__).with_name("convert_to_cpf.py").resolve()

    if not converter_path.exists():
        print(
            f'ERROR: Cannot find converter script at: {converter_path}\n'
            f'Put convert_to_cpf.py in the same folder, or pass --converter "path\\to\\convert_to_cpf.py".',
            file=sys.stderr,
        )
        return 2

    page_urls = iter_pages(args.page_url, args.all_pages, args.max_pages, args.delay)
    print(f"Scanning {len(page_urls)} page(s)...")

    seen_pdfs: set[str] = set()
    all_pdfs: list[str] = []

    for i, u in enumerate(page_urls, start=1):
        if i > 1:
            time.sleep(args.delay)
        html = fetch_url(u)
        pdfs = extract_pdf_links(u, html, only_same_domain=args.same_domain)
        for p in pdfs:
            if p not in seen_pdfs:
                seen_pdfs.add(p)
                all_pdfs.append(p)

    if not all_pdfs:
        print("No PDF links found.")
        return 0

    print(f"Found {len(all_pdfs)} unique PDF link(s). Converting...")

    ok = 0
    failed = 0

    for i, pdf_url in enumerate(all_pdfs, start=1):
        base = pdf_basename_from_url(pdf_url)
        out_path = outdir / (Path(base).with_suffix("").name + ".cpf.txt")
        title = f"{args.title_prefix}{Path(base).with_suffix('').name}".strip() or Path(base).with_suffix("").name

        try:
            run_converter(
                converter_path=converter_path,
                pdf_url=pdf_url,
                out_path=out_path,
                doc_type=args.type,
                pdf_engine=args.pdf_engine,
                min_para_chars=args.min_para_chars,
                title=title,
            )
            ok += 1
        except subprocess.CalledProcessError as e:
            failed += 1
            print(f"[{i}/{len(all_pdfs)}] FAILED: {pdf_url}\n  Reason: converter exit code {e.returncode}", file=sys.stderr)
        except Exception as e:
            failed += 1
            print(f"[{i}/{len(all_pdfs)}] FAILED: {pdf_url}\n  Reason: {e}", file=sys.stderr)

    print(f"Done. Success: {ok}, Failed: {failed}. Output folder: {outdir}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
