# run example:
#   # from commentary_pipeline root:
#   python 11_static_viewer\build_static_viewer.py --db publish.db --outdir viewer --commentary-db commentary.db
#   # open:
#   #   viewer\index.html

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

DESCRIPTION = """Build a static HTML viewer from publish.db (final_commentary table).

Optionally enrich each row with provenance (source) info derived from commentary.db:
- which cited paragraphs were used
- which document (title/author/type) those paragraphs came from
- source URL (PDF link) and local CPF path (if present)
- nearest section/chapter-ish heading (best-effort heuristic)

Adds an 'Evidence (raw)' column that contains the FULL cited paragraphs
(the same paragraphs selected for the OpenAI extraction step, typically up to 10).

NEW: Adds a 'Kind' column and a Keep-only table (tab):
- kind = prefilter_drop | has_commentary | no_commentary
- Keep-only tab shows only rows with non-empty commentary text
"""

EXAMPLES = """Examples (repo root):
  python 11_static_viewer/build_static_viewer.py --db ./publish.db --outdir ./viewer
  python 11_static_viewer/build_static_viewer.py --db ./publish.db --outdir ./viewer --commentary-db ./commentary.db
  python 11_static_viewer/build_static_viewer.py --db ./publish.db --outdir ./viewer --commentary-lang zh
"""


# ----------------------------
# small helpers
# ----------------------------

def table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row)


def get_tables(con: sqlite3.Connection) -> list[str]:
    rows = con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    return [r[0] for r in rows]


def get_columns(con: sqlite3.Connection, table: str) -> list[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def pick_first_existing(cols: list[str], candidates: list[str]) -> str:
    for c in candidates:
        if c in cols:
            return c
    return ""


def safe_json_loads(s: str, default: Any) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return default


# ----------------------------
# provenance extraction (commentary.db)
# ----------------------------

@dataclass
class ParaProv:
    para_id: int
    text: str
    source_text: str
    source_url: str
    doc_count: int
    evidence_text: str
    evidence_count: int


def compute_provenance_map(commentary_db: Path) -> dict[int, dict[str, Any]]:
    """
    Map final_commentary.id -> provenance payload
    (Source summary + Evidence paragraphs).
    """
    con = sqlite3.connect(str(commentary_db))
    con.row_factory = sqlite3.Row

    tables = set(get_tables(con))
    if "paragraphs" not in tables:
        con.close()
        return {}

    # paragraphs schema varies; we will use what's available
    pcols = get_columns(con, "paragraphs")
    p_id = pick_first_existing(pcols, ["id", "para_id"])
    p_text = pick_first_existing(pcols, ["text", "para_text", "content"])
    p_source_path = pick_first_existing(pcols, ["source_path", "source_file", "cpf_path", "path"])
    if not p_id or not p_text:
        con.close()
        return {}

    # optional docs table
    has_docs = "docs" in tables
    dcols = get_columns(con, "docs") if has_docs else []
    d_id = pick_first_existing(dcols, ["id", "doc_id"])
    d_title = pick_first_existing(dcols, ["title", "doc_title", "name"])
    d_author = pick_first_existing(dcols, ["author", "doc_author"])
    d_type = pick_first_existing(dcols, ["doc_type", "type"])
    d_url = pick_first_existing(dcols, ["source_url", "url", "pdf_url", "link"])
    d_path = pick_first_existing(dcols, ["source_path", "path", "cpf_path"])

    # optional paragraph->doc mapping
    para_doc_table = "paragraph_doc_map" if "paragraph_doc_map" in tables else ""
    pdcols = get_columns(con, para_doc_table) if para_doc_table else []
    pd_para = pick_first_existing(pdcols, ["para_id", "paragraph_id"])
    pd_doc = pick_first_existing(pdcols, ["doc_id", "source_id"])

    # Heuristic: in final_commentary, cited_para_ids is a JSON array of paragraph ids.
    # We build map keyed by final_commentary.id later, but here we only provide utilities.

    def doc_info_for_para_ids(para_ids: list[int]) -> tuple[str, str, int]:
        """
        Return (source_text, source_url, doc_count) best-effort.
        """
        if not para_ids:
            return ("", "", 0)

        # If we can join via paragraph_doc_map -> docs, do so
        if para_doc_table and has_docs and pd_para and pd_doc and d_id:
            q = f"""
            SELECT d.{d_title} AS title, d.{d_author} AS author, d.{d_type} AS dtype,
                   d.{d_url} AS url, d.{d_path} AS path
            FROM {para_doc_table} m
            JOIN docs d ON d.{d_id} = m.{pd_doc}
            WHERE m.{pd_para} IN ({",".join(["?"] * len(para_ids))})
            """
            rows = con.execute(q, para_ids).fetchall()
            if rows:
                seen = []
                url = ""
                for r in rows:
                    title = (r["title"] or "").strip() if "title" in r.keys() else ""
                    author = (r["author"] or "").strip() if "author" in r.keys() else ""
                    dtype = (r["dtype"] or "").strip() if "dtype" in r.keys() else ""
                    path = (r["path"] or "").strip() if "path" in r.keys() else ""
                    u = (r["url"] or "").strip() if "url" in r.keys() else ""
                    if u and not url:
                        url = u
                    label = " | ".join([x for x in [author, title, dtype] if x])
                    if path:
                        label = (label + f" | {path}") if label else path
                    if label and label not in seen:
                        seen.append(label)
                return ("\n".join(seen), url, len(seen))

        # Fallback: try source_path on paragraphs
        if p_source_path:
            q = f"""
            SELECT DISTINCT {p_source_path} AS sp
            FROM paragraphs
            WHERE {p_id} IN ({",".join(["?"] * len(para_ids))})
            """
            rows = con.execute(q, para_ids).fetchall()
            paths = []
            for r in rows:
                sp = (r["sp"] or "").strip()
                if sp and sp not in paths:
                    paths.append(sp)
            return ("\n".join(paths), "", len(paths))

        return ("", "", 0)

    def paragraphs_text_for_ids(para_ids: list[int]) -> tuple[str, int]:
        if not para_ids:
            return ("", 0)
        q = f"""
        SELECT {p_id} AS pid, {p_text} AS txt
        FROM paragraphs
        WHERE {p_id} IN ({",".join(["?"] * len(para_ids))})
        ORDER BY {p_id} ASC
        """
        rows = con.execute(q, para_ids).fetchall()
        parts = []
        for r in rows:
            pid = r["pid"]
            txt = (r["txt"] or "").strip()
            if txt:
                parts.append(f"[{pid}] {txt}")
        return ("\n\n".join(parts), len(parts))

    # Now we need final_commentary rows to know which paragraph ids to look up,
    # but this function is called before we open publish.db, so we just return
    # a callable-ish map builder. We'll build per-row below by querying inside
    # build_prov_for_publish_rows().
    con.close()
    return {}


def build_prov_for_publish_rows(commentary_db: Path, publish_rows: list[dict]) -> dict[int, dict[str, Any]]:
    """
    Returns map: final_commentary.id -> {
      evidence_text, evidence_count, source_text, source_url, doc_count
    }
    """
    con = sqlite3.connect(str(commentary_db))
    con.row_factory = sqlite3.Row
    tables = set(get_tables(con))
    if "paragraphs" not in tables:
        con.close()
        return {}

    pcols = get_columns(con, "paragraphs")
    p_id = pick_first_existing(pcols, ["id", "para_id"])
    p_text = pick_first_existing(pcols, ["text", "para_text", "content"])
    p_source_path = pick_first_existing(pcols, ["source_path", "source_file", "cpf_path", "path"])
    if not p_id or not p_text:
        con.close()
        return {}

    has_docs = "docs" in tables
    dcols = get_columns(con, "docs") if has_docs else []
    d_id = pick_first_existing(dcols, ["id", "doc_id"])
    d_title = pick_first_existing(dcols, ["title", "doc_title", "name"])
    d_author = pick_first_existing(dcols, ["author", "doc_author"])
    d_type = pick_first_existing(dcols, ["doc_type", "type"])
    d_url = pick_first_existing(dcols, ["source_url", "url", "pdf_url", "link"])
    d_path = pick_first_existing(dcols, ["source_path", "path", "cpf_path"])

    para_doc_table = "paragraph_doc_map" if "paragraph_doc_map" in tables else ""
    pdcols = get_columns(con, para_doc_table) if para_doc_table else []
    pd_para = pick_first_existing(pdcols, ["para_id", "paragraph_id"])
    pd_doc = pick_first_existing(pdcols, ["doc_id", "source_id"])

    def doc_info_for_para_ids(para_ids: list[int]) -> tuple[str, str, int]:
        if not para_ids:
            return ("", "", 0)

        if para_doc_table and has_docs and pd_para and pd_doc and d_id:
            q = f"""
            SELECT d.{d_title} AS title, d.{d_author} AS author, d.{d_type} AS dtype,
                   d.{d_url} AS url, d.{d_path} AS path
            FROM {para_doc_table} m
            JOIN docs d ON d.{d_id} = m.{pd_doc}
            WHERE m.{pd_para} IN ({",".join(["?"] * len(para_ids))})
            """
            rows = con.execute(q, para_ids).fetchall()
            if rows:
                seen = []
                url = ""
                for r in rows:
                    title = (r["title"] or "").strip() if "title" in r.keys() else ""
                    author = (r["author"] or "").strip() if "author" in r.keys() else ""
                    dtype = (r["dtype"] or "").strip() if "dtype" in r.keys() else ""
                    path = (r["path"] or "").strip() if "path" in r.keys() else ""
                    u = (r["url"] or "").strip() if "url" in r.keys() else ""
                    if u and not url:
                        url = u
                    label = " | ".join([x for x in [author, title, dtype] if x])
                    if path:
                        label = (label + f" | {path}") if label else path
                    if label and label not in seen:
                        seen.append(label)
                return ("\n".join(seen), url, len(seen))

        if p_source_path:
            q = f"""
            SELECT DISTINCT {p_source_path} AS sp
            FROM paragraphs
            WHERE {p_id} IN ({",".join(["?"] * len(para_ids))})
            """
            rows = con.execute(q, para_ids).fetchall()
            paths = []
            for r in rows:
                sp = (r["sp"] or "").strip()
                if sp and sp not in paths:
                    paths.append(sp)
            return ("\n".join(paths), "", len(paths))

        return ("", "", 0)

    def paragraphs_text_for_ids(para_ids: list[int]) -> tuple[str, int]:
        if not para_ids:
            return ("", 0)
        q = f"""
        SELECT {p_id} AS pid, {p_text} AS txt
        FROM paragraphs
        WHERE {p_id} IN ({",".join(["?"] * len(para_ids))})
        ORDER BY {p_id} ASC
        """
        rows = con.execute(q, para_ids).fetchall()
        parts = []
        for r in rows:
            pid = r["pid"]
            txt = (r["txt"] or "").strip()
            if txt:
                parts.append(f"[{pid}] {txt}")
        return ("\n\n".join(parts), len(parts))

    prov_map: dict[int, dict[str, Any]] = {}
    for r in publish_rows:
        rid = r.get("id")
        if rid is None:
            continue
        try:
            rid_int = int(rid)
        except Exception:
            continue

        para_ids = safe_json_loads(r.get("cited_para_ids") or "[]", [])
        if not isinstance(para_ids, list):
            para_ids = []
        para_ids = [int(x) for x in para_ids if str(x).isdigit()]

        evidence_text, evidence_count = paragraphs_text_for_ids(para_ids)
        source_text, source_url, doc_count = doc_info_for_para_ids(para_ids)

        prov_map[rid_int] = {
            "evidence_text": evidence_text,
            "evidence_count": evidence_count,
            "source_text": source_text,
            "source_url": source_url,
            "doc_count": doc_count,
        }

    con.close()
    return prov_map


# ----------------------------
# load publish rows
# ----------------------------

def load_final_commentary_rows(publish_db: Path) -> list[dict]:
    con = sqlite3.connect(str(publish_db))
    con.row_factory = sqlite3.Row

    tables = get_tables(con)
    if "final_commentary" not in tables:
        con.close()
        raise RuntimeError(f"Could not find final_commentary table in {publish_db}. Tables: {tables}")

    cols = get_columns(con, "final_commentary")

    id_col = pick_first_existing(cols, ["id"])
    ref_col = pick_first_existing(cols, ["ref", "verse_ref"])
    book_col = pick_first_existing(cols, ["book_osis"])
    ch_col = pick_first_existing(cols, ["chapter"])
    vs_col = pick_first_existing(cols, ["verse_start"])
    ve_col = pick_first_existing(cols, ["verse_end"])
    kjv_col = pick_first_existing(cols, ["kjv_text", "kjv"])
    cuv_col = pick_first_existing(cols, ["cuv_text", "cuv_s"])
    keep_col = pick_first_existing(cols, ["keep_drop", "decision"])
    notes_col = pick_first_existing(cols, ["notes"])

    my_en = pick_first_existing(cols, ["my_edit_en"])
    my_zh = pick_first_existing(cols, ["my_edit_zh"])
    sum_en = pick_first_existing(cols, ["final_en", "summary_en"])
    sum_zh = pick_first_existing(cols, ["final_zh", "summary_zh"])
    cited_col = pick_first_existing(cols, ["cited_para_ids"])

    select_cols = []
    for c in [
        id_col, ref_col, book_col, ch_col, vs_col, ve_col,
        kjv_col, cuv_col,
        keep_col, notes_col,
        my_en, my_zh, sum_en, sum_zh,
        cited_col,
    ]:
        if c and c not in select_cols:
            select_cols.append(c)

    if not select_cols:
        con.close()
        raise RuntimeError("final_commentary table has no readable columns (unexpected).")

    q = f"SELECT {', '.join(select_cols)} FROM final_commentary ORDER BY id ASC;"
    rows = con.execute(q).fetchall()

    out: list[dict] = []
    for r in rows:
        d = {select_cols[i]: r[i] for i in range(len(select_cols))}
        out.append(
            {
                "id": d.get(id_col),
                "ref": d.get(ref_col) or "",
                "book_osis": d.get(book_col) or "",
                "chapter": d.get(ch_col),
                "verse_start": d.get(vs_col),
                "verse_end": d.get(ve_col),
                "kjv_text": d.get(kjv_col) or "",
                "cuv_text": d.get(cuv_col) or "",
                "keep_drop": d.get(keep_col) or "",
                "notes": d.get(notes_col) or "",
                "my_edit_en": d.get(my_en) or "",
                "my_edit_zh": d.get(my_zh) or "",
                "summary_en": d.get(sum_en) or "",
                "summary_zh": d.get(sum_zh) or "",
                "cited_para_ids": d.get(cited_col) or "",
            }
        )

    con.close()
    return out


def choose_display_commentary(row: dict, *, lang: str = "en") -> str:
    lang = (lang or "en").strip().lower()
    if lang not in {"en", "zh"}:
        lang = "en"

    if lang == "en":
        keys = ["my_edit_en", "summary_en", "my_edit_zh", "summary_zh"]
    else:
        keys = ["my_edit_zh", "summary_zh", "my_edit_en", "summary_en"]

    for k in keys:
        v = (row.get(k) or "").strip()
        if v:
            return v
    return ""


# ----------------------------
# HTML builder
# ----------------------------

def build_index_html(
    rows: list[dict],
    prov_map: dict[int, dict[str, Any]],
    generated_at: str,
    *,
    commentary_lang: str = "en",
) -> str:
    books = sorted({(r.get("book_osis") or "").strip() for r in rows if (r.get("book_osis") or "").strip()})
    book_options = "\n".join([f'<option value="{html.escape(b)}">{html.escape(b)}</option>' for b in books])

    data_payload = []
    for r in rows:
        rid = r.get("id")
        prov = prov_map.get(int(rid)) if rid is not None and str(rid).isdigit() else None
        source_text = (prov or {}).get("source_text", "")
        source_url = (prov or {}).get("source_url", "")
        doc_count = (prov or {}).get("doc_count", 0)
        evidence_text = (prov or {}).get("evidence_text", "")
        evidence_count = (prov or {}).get("evidence_count", 0)

        commentary = choose_display_commentary(r, lang=commentary_lang)
        notes = (r.get("notes") or "")
        kind = (
            "prefilter_drop"
            if "prefilter drop" in notes.lower()
            else ("has_commentary" if commentary.strip() else "no_commentary")
        )

        data_payload.append(
            {
                "id": r.get("id"),
                "ref": r.get("ref") or "",
                "book_osis": r.get("book_osis") or "",
                "kjv_text": r.get("kjv_text") or "",
                "cuv_text": r.get("cuv_text") or "",
                "keep_drop": r.get("keep_drop") or "",
                "notes": notes,
                "commentary": commentary,
                "has_commentary": bool(commentary.strip()),
                "kind": kind,
                "evidence_text": evidence_text or "",
                "evidence_count": int(evidence_count) if evidence_count else 0,
                "source_text": source_text or "",
                "source_url": source_url or "",
                "source_docs": int(doc_count) if doc_count else 0,
            }
        )

    data_json = json.dumps(data_payload, ensure_ascii=False)

    # NOTE: This is a Python f-string. Any literal `{` / `}` in JS must be doubled as `{{` / `}}`.
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Commentary Draft Viewer</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 16px; }}
    .top {{ display:flex; gap:12px; align-items:center; flex-wrap:wrap; }}
    .top label {{ font-size: 14px; color:#222; }}
    input[type="text"], select {{ font-size: 14px; padding:6px 8px; border:1px solid #bbb; border-radius:8px; }}
    .stat {{ margin-left:auto; font-size: 13px; color:#444; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
    th {{ background: #fafafa; text-align: left; font-size: 13px; }}
    td {{ font-size: 13px; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }}
    .pill {{ display:inline-block; padding:2px 8px; border:1px solid #bbb; border-radius:999px; font-size:12px; color:#333; background:#fff; }}
    .small {{ font-size:12px; color:#555; margin-top:4px; }}
    details summary {{ cursor:pointer; user-select:none; }}
    details > div {{ margin-top: 6px; white-space: pre-wrap; line-height: 1.35; }}
    .row {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; }}
    .tabs {{ display:flex; gap:8px; margin: 10px 0 12px; }}
    .tab {{ border:1px solid #bbb; background:#f6f6f6; padding:6px 10px; border-radius:8px; cursor:pointer; font-size: 14px; }}
    .tab.active {{ background:#fff; border-color:#666; font-weight:600; }}
    .panel.hidden {{ display:none; }}
  </style>
</head>
<body>
  <div class="top">
    <label>Search:
      <input id="q" type="text" placeholder="type to filter..." size="28" />
    </label>

    <label>Book:
      <select id="book">
        <option value="">(all)</option>
        {book_options}
      </select>
    </label>

    <label>Decision:
      <select id="keep">
        <option value="">(all)</option>
        <option value="keep">keep</option>
        <option value="drop">drop</option>
      </select>
    </label>

    <div class="stat" id="stat"></div>
  </div>

  <div class="small">Generated: {html.escape(generated_at)}</div>

  <div class="tabs">
    <button id="tab_all" class="tab active" type="button">Working table (all)</button>
    <button id="tab_keep" class="tab" type="button">Keep-only (AI found commentary)</button>
  </div>

  <div id="panel_all" class="panel">
    <table>
      <thead>
        <tr>
          <th style="width:110px;">Ref</th>
          <th style="width:90px;">KJV</th>
          <th style="width:90px;">CUV (Simplified)</th>
          <th style="width:120px;">Decision</th>
          <th style="width:110px;">Kind</th>
          <th style="width:130px;">Source</th>
          <th style="width:160px;">Evidence (raw)</th>
          <th>Commentary ({html.escape(commentary_lang.upper())})</th>
        </tr>
      </thead>
      <tbody id="tb_all"></tbody>
    </table>
  </div>

  <div id="panel_keep" class="panel hidden">
    <table>
      <thead>
        <tr>
          <th style="width:110px;">Ref</th>
          <th style="width:90px;">KJV</th>
          <th style="width:90px;">CUV (Simplified)</th>
          <th style="width:120px;">Decision</th>
          <th style="width:110px;">Kind</th>
          <th style="width:130px;">Source</th>
          <th style="width:160px;">Evidence (raw)</th>
          <th>Commentary ({html.escape(commentary_lang.upper())})</th>
        </tr>
      </thead>
      <tbody id="tb_keep"></tbody>
    </table>
  </div>

<script>
const DATA = {data_json};
const STORAGE_KEY = "commentary_pipeline_keepdrop_v1";

// ---------- utils ----------
function esc(s) {{
  return (s ?? "").toString()
    .replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")
    .replaceAll('"',"&quot;").replaceAll("'","&#39;");
}}

function norm(s) {{
  return (s ?? "").toString().toLowerCase();
}}

function loadDecisions() {{
  try {{
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return {{}};
    const obj = JSON.parse(raw);
    return obj && typeof obj === "object" ? obj : {{}};
  }} catch {{
    return {{}};
  }}
}}

function saveDecisions(obj) {{
  try {{
    localStorage.setItem(STORAGE_KEY, JSON.stringify(obj));
  }} catch {{}}
}}

function rowKey(r) {{
  return `${{r.book_osis}}|${{r.ref}}|${{r.id}}`;
}}

const DECISIONS = loadDecisions();

function getDecision(r) {{
  const key = rowKey(r);
  const v = DECISIONS[key] || (r.keep_drop || "");
  return (v === "keep" || v === "drop") ? v : "";
}}

function setDecisionByKey(key, val) {{
  const v = (val || "").toString().trim().toLowerCase();
  if (v === "keep" || v === "drop") {{
    DECISIONS[key] = v;
  }} else {{
    delete DECISIONS[key];
  }}
  saveDecisions(DECISIONS);
}}

function detailsHtml(text, label="show") {{
  const t = (text || "").trim();
  if (!t) return "";
  return `<details><summary>${{esc(label)}}</summary><div>${{esc(t)}}</div></details>`;
}}

// ---------- render ----------
let CURRENT_TAB = "all";

function rowToHtml(r) {{
  const key = rowKey(r);
  const decision = getDecision(r);

  const pill = decision ? `<span class="pill">${{esc(decision)}}</span>` : "";
  const kind = (r.kind || "").trim();
  const kindPill = kind ? `<span class="pill">${{esc(kind)}}</span>` : "";

  const notes = (r.notes || "").trim();
  const notesHtml = notes ? `<div class="small">notes: ${{esc(notes)}}</div>` : "";

  const commHtml = detailsHtml(r.commentary);
  const kjvHtml = detailsHtml(r.kjv_text);
  const cuvHtml = detailsHtml(r.cuv_text);

  const evLabel = (r.evidence_count && r.evidence_count > 0) ? `show (${{r.evidence_count}})` : "show";
  const evidenceHtml = detailsHtml(r.evidence_text, evLabel);

  const srcLabel = (r.source_docs && r.source_docs > 0) ? `show (${{r.source_docs}})` : "show";
  const srcHtml = detailsHtml(r.source_text, srcLabel);

  const srcUrl = (r.source_url || "").trim();
  const srcLink = srcUrl ? `<a href="${{esc(srcUrl)}}" target="_blank" rel="noopener">open</a>` : "";

  const sel =
    `<select data-key="${{esc(key)}}">
       <option value="" ${{decision===""?"selected":""}}>—</option>
       <option value="keep" ${{decision==="keep"?"selected":""}}>keep</option>
       <option value="drop" ${{decision==="drop"?"selected":""}}>drop</option>
     </select>`;

  return `
    <tr>
      <td>
        <div class="mono">${{esc(r.ref)}}</div>
      </td>
      <td>${{kjvHtml}}</td>
      <td>${{cuvHtml}}</td>
      <td>
        <div class="row">${{sel}} ${{pill}}</div>
      </td>
      <td>
        <div class="row">${{kindPill}}</div>
        ${{notesHtml}}
      </td>
      <td>
        <div class="row">${{srcHtml}} ${{srcLink}}</div>
      </td>
      <td>${{evidenceHtml}}</td>
      <td>${{commHtml}}</td>
    </tr>
  `.trim();
}}

function filterRows(extraFn) {{
  const q = norm(document.getElementById("q").value);
  const book = document.getElementById("book").value;
  const keep = document.getElementById("keep").value;

  return DATA.filter(r => {{
    if (book && (r.book_osis || "") !== book) return false;

    const decision = getDecision(r);
    if (keep && decision !== keep) return false;

    if (extraFn && !extraFn(r)) return false;

    if (!q) return true;
    const blob =
      norm(r.ref) + "\\n" +
      norm(r.kjv_text) + "\\n" +
      norm(r.cuv_text) + "\\n" +
      norm(r.commentary) + "\\n" +
      norm(r.evidence_text) + "\\n" +
      norm(r.source_text) + "\\n" +
      norm(r.source_url) + "\\n" +
      norm(r.notes) + "\\n" +
      norm(r.kind) + "\\n" +
      decision;
    return blob.includes(q);
  }});
}}

function renderTable(tbodyId, extraFn) {{
  const rows = filterRows(extraFn);
  const tb = document.getElementById(tbodyId);
  tb.innerHTML = rows.map(rowToHtml).join("");

  tb.querySelectorAll("select[data-key]").forEach(sel => {{
    sel.addEventListener("change", () => {{
      const key = sel.dataset.key || "";
      setDecisionByKey(key, sel.value);
      renderAllAndKeep();
    }});
  }});

  return rows.length;
}}

function renderAllAndKeep() {{
  const nAll = renderTable("tb_all", null);
  const nKeep = renderTable("tb_keep", (r) => !!r.has_commentary);

  const total = DATA.length;
  document.getElementById("stat").textContent =
    `all: ${{nAll}} / ${{total}}   |   keep-only: ${{nKeep}} / ${{total}}`;
}}

function showTab(name) {{
  CURRENT_TAB = name;

  document.getElementById("tab_all").classList.toggle("active", name === "all");
  document.getElementById("tab_keep").classList.toggle("active", name === "keep");

  document.getElementById("panel_all").classList.toggle("hidden", name !== "all");
  document.getElementById("panel_keep").classList.toggle("hidden", name !== "keep");
}}

["q","book","keep"].forEach(id => {{
  document.getElementById(id).addEventListener("input", renderAllAndKeep);
  document.getElementById(id).addEventListener("change", renderAllAndKeep);
}});

document.getElementById("tab_all").addEventListener("click", () => showTab("all"));
document.getElementById("tab_keep").addEventListener("click", () => showTab("keep"));
showTab("all");
renderAllAndKeep();
</script>

</body>
</html>
"""


# ----------------------------
# main
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLES,
    )
    ap.add_argument("--db", required=True, help="Path to publish.db")
    ap.add_argument("--outdir", default="viewer", help="Output folder (will be created)")
    ap.add_argument("--commentary-lang", default="en", choices=["en", "zh"], help="Which commentary language to display")
    ap.add_argument(
        "--commentary-db",
        default="",
        help="Optional path to commentary.db (adds Source + Evidence columns). "
             "If omitted, auto-uses ./commentary.db if it exists.",
    )
    args = ap.parse_args()

    publish_db = Path(args.db).resolve()
    if not publish_db.exists():
        raise RuntimeError(f"publish.db not found: {publish_db}")

    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    publish_rows = load_final_commentary_rows(publish_db)

    commentary_db: Path | None = None
    if args.commentary_db:
        commentary_db = Path(args.commentary_db).resolve()
    else:
        auto = (Path.cwd() / "commentary.db").resolve()
        if auto.exists():
            commentary_db = auto

    prov_map: dict[int, dict[str, Any]] = {}
    if commentary_db and commentary_db.exists():
        prov_map = build_prov_for_publish_rows(commentary_db, publish_rows)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html_text = build_index_html(publish_rows, prov_map, generated_at, commentary_lang=args.commentary_lang)

    out_path = outdir / "index.html"
    out_path.write_text(html_text, encoding="utf-8")
    print(f"OK: wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
