# run example:
#   # from commentary_pipeline root:
#   python 11_static_viewer\build_static_viewer.py --db "publish.db" --outdir "viewer"

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import sqlite3
from pathlib import Path
from datetime import datetime


DESCRIPTION = """Build a static HTML viewer from publish.db (final_commentary table).\n\nExample (copy/paste):\n  python 11_static_viewer/build_static_viewer.py --db ./publish.db --outdir ./viewer\n"""

EXAMPLES = """Examples (copy/paste ready from repo root):
  # Rebuild the viewer folder from the publish.db produced by export_final_commentary_sqlite
  python 11_static_viewer/build_static_viewer.py --db ./publish.db --outdir ./viewer

  # Write the viewer to a custom output directory
  python 11_static_viewer/build_static_viewer.py --db ./publish.db --outdir ./dist/viewer
"""


def get_columns(con: sqlite3.Connection, table: str) -> list[str]:
    rows = con.execute(f"PRAGMA table_info({table});").fetchall()
    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    return [r[1] for r in rows]


def pick_first(cols: list[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return None


def fetch_rows(con: sqlite3.Connection) -> list[dict]:
    cols = get_columns(con, "final_commentary")

    # Common columns (schema may evolve; we adapt)
    id_col = pick_first(cols, ["id"]) or None
    ref_col = pick_first(cols, ["ref"]) or None
    keep_col = pick_first(cols, ["keep_drop"])  # optional
    notes_col = pick_first(cols, ["notes"])     # optional

    kjv_col = pick_first(cols, ["kjv_text", "kjv"])
    cuv_col = pick_first(cols, ["cuv_text", "cuv"])

    # AI / edits (may or may not exist depending on your exporter version)
    my_en = pick_first(cols, ["my_edit_en"])
    my_zh = pick_first(cols, ["my_edit_zh"])

    # publish.db uses final_en/final_zh
    sum_en = pick_first(cols, ["final_en", "summary_en"])
    sum_zh = pick_first(cols, ["final_zh", "summary_zh"])

    # Verse identity
    book_col = pick_first(cols, ["book_osis"])
    ch_col = pick_first(cols, ["chapter"])
    vs_col = pick_first(cols, ["verse_start"])
    ve_col = pick_first(cols, ["verse_end"])

    # Build SELECT list (only columns that exist)
    select_cols = []
    for c in [id_col, ref_col, book_col, ch_col, vs_col, ve_col, kjv_col, cuv_col, keep_col, notes_col, my_en, my_zh, sum_en, sum_zh]:
        if c and c not in select_cols:
            select_cols.append(c)

    if not select_cols:
        raise RuntimeError("final_commentary table has no readable columns (unexpected).")

    q = f"SELECT {', '.join(select_cols)} FROM final_commentary ORDER BY id ASC;"
    rows = con.execute(q).fetchall()

    out: list[dict] = []
    for r in rows:
        d = {select_cols[i]: r[i] for i in range(len(select_cols))}
        # Normalize keys the viewer expects
        out.append(
            {
                "id": d.get(id_col) if id_col else None,
                "ref": d.get(ref_col) if ref_col else "",
                "book_osis": d.get(book_col) if book_col else "",
                "chapter": d.get(ch_col) if ch_col else None,
                "verse_start": d.get(vs_col) if vs_col else None,
                "verse_end": d.get(ve_col) if ve_col else None,
                "kjv_text": d.get(kjv_col) if kjv_col else "",
                "cuv_text": d.get(cuv_col) if cuv_col else "",
                "keep_drop": d.get(keep_col) if keep_col else "",
                "notes": d.get(notes_col) if notes_col else "",
                "my_edit_en": d.get(my_en) if my_en else "",
                "my_edit_zh": d.get(my_zh) if my_zh else "",
                "summary_en": d.get(sum_en) if sum_en else "",
                "summary_zh": d.get(sum_zh) if sum_zh else "",
            }
        )
    return out


def choose_display_commentary(row: dict) -> str:
    # Priority: your edits > AI zh > AI en
    for k in ["my_edit_zh", "my_edit_en", "summary_zh", "summary_en"]:
        v = (row.get(k) or "").strip()
        if v:
            return v
    return ""


def build_index_html(rows: list[dict], generated_at: str) -> str:
    # Build book list for dropdown (if available)
    books = sorted({(r.get("book_osis") or "").strip() for r in rows if (r.get("book_osis") or "").strip()})

    # Data is embedded as JSON so you can open index.html directly
    data_json = json.dumps(
        [
            {
                "id": r.get("id"),
                "ref": r.get("ref") or "",
                "book_osis": r.get("book_osis") or "",
                "kjv_text": r.get("kjv_text") or "",
                "cuv_text": r.get("cuv_text") or "",
                "keep_drop": r.get("keep_drop") or "",
                "notes": r.get("notes") or "",
                "commentary": choose_display_commentary(r),
            }
            for r in rows
        ],
        ensure_ascii=False,
    )

    book_options = "\n".join([f'<option value="{html.escape(b)}">{html.escape(b)}</option>' for b in books])

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Commentary Draft Viewer</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 16px; }}
    .bar {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-bottom: 12px; }}
    input, select {{ padding: 8px; font-size: 14px; }}
    .meta {{ color: #666; font-size: 12px; margin-bottom: 12px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: #f6f6f6; }}
    .ref {{ white-space: nowrap; font-weight: 600; }}
    .small {{ font-size: 12px; color: #666; }}
    details > summary {{ cursor: pointer; }}
    .pill {{ display:inline-block; padding:2px 8px; border:1px solid #ddd; border-radius: 999px; font-size: 12px; }}
  </style>
</head>
<body>
  <h2>Commentary Draft Viewer</h2>
  <div class="meta">
    Generated at: {html.escape(generated_at)} · Rows: {len(rows)}
  </div>

  <div class="bar">
    <input id="q" type="search" placeholder="Search ref / KJV / CUV / commentary / notes..." size="40" />
    <select id="book">
      <option value="">All books</option>
      {book_options}
    </select>
    <select id="keep">
      <option value="">keep/drop (all)</option>
      <option value="keep">keep</option>
      <option value="drop">drop</option>
    </select>
    <span class="small" id="stat"></span>
  </div>

  <table>
    <thead>
      <tr>
        <th style="width:110px;">Ref</th>
        <th style="width:34%;">KJV</th>
        <th style="width:34%;">CUV (Simplified)</th>
        <th>Commentary</th>
      </tr>
    </thead>
    <tbody id="tb"></tbody>
  </table>

<script>
const DATA = {data_json};

function esc(s) {{
  return (s ?? "").toString()
    .replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")
    .replaceAll('"',"&quot;").replaceAll("'","&#39;");
}}

function norm(s) {{
  return (s ?? "").toString().toLowerCase();
}}

function render() {{
  const q = norm(document.getElementById("q").value);
  const book = document.getElementById("book").value;
  const keep = document.getElementById("keep").value;

  const rows = DATA.filter(r => {{
    if (book && (r.book_osis || "") !== book) return false;
    if (keep && norm(r.keep_drop) !== keep) return false;
    if (!q) return true;
    const blob = norm(r.ref) + "\\n" + norm(r.kjv_text) + "\\n" + norm(r.cuv_text) + "\\n" + norm(r.commentary) + "\\n" + norm(r.notes);
    return blob.includes(q);
  }});

  const tb = document.getElementById("tb");
  tb.innerHTML = rows.map(r => {{
    const kd = (r.keep_drop || "").trim();
    const pill = kd ? `<span class="pill">${{esc(kd)}}</span>` : "";
    const notes = (r.notes || "").trim();
    const notesHtml = notes ? `<div class="small">notes: ${{esc(notes)}}</div>` : "";

    const comm = (r.commentary || "").trim();
    const commHtml = comm
      ? `<details><summary>show</summary><div style="white-space:pre-wrap; margin-top:6px;">${{esc(comm)}}</div></details>`
      : `<span class="small">(empty)</span>`;

    return `
      <tr>
        <td class="ref">${{esc(r.ref)}}<div style="margin-top:6px;">${{pill}}</div></td>
        <td style="white-space:pre-wrap;">${{esc(r.kjv_text)}}</td>
        <td style="white-space:pre-wrap;">${{esc(r.cuv_text)}}</td>
        <td>${{commHtml}}${{notesHtml}}</td>
      </tr>
    `;
  }}).join("");

  document.getElementById("stat").textContent = `showing ${{rows.length}} / ${{DATA.length}}`;
}}

["q","book","keep"].forEach(id => {{
  document.getElementById(id).addEventListener("input", render);
  document.getElementById(id).addEventListener("change", render);
}});

render();
</script>

</body>
</html>
"""


def main() -> int:
    ap = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLES,
    )
    ap.add_argument("--db", required=True, help="Path to publish.db")
    ap.add_argument("--outdir", default="viewer", help="Output folder (will be created)")
    args = ap.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        raise SystemExit(f"ERROR: DB not found: {db_path}")

    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(db_path))
    try:
        rows = fetch_rows(con)
    finally:
        con.close()

    generated_at = datetime.now().isoformat(timespec="seconds")
    html_text = build_index_html(rows, generated_at)

    index_path = outdir / "index.html"
    index_path.write_text(html_text, encoding="utf-8")

    print(f"OK: wrote {index_path}")
    print(f"Open it in your browser:")
    print(f"  {index_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
