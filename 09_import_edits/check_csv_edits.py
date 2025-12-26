# run example:
# python check_csv_edits.py "..\08_export_commentary\commentary_draft.csv"

import csv
import sys

csv_path = sys.argv[1]

with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
    r = csv.DictReader(f)
    hits = []
    for i, row in enumerate(r, start=2):
        kd = (row.get("keep_drop") or "").strip()
        en = (row.get("my_edit_en") or "").strip()
        zh = (row.get("my_edit_zh") or "").strip()
        nt = (row.get("notes") or "").strip()
        if kd or en or zh or nt:
            hits.append(
                (
                    i,
                    (row.get("ref") or "").strip(),
                    (row.get("book_osis") or "").strip(),
                    (row.get("chapter") or "").strip(),
                    kd,
                    en[:40],
                    zh[:40],
                    nt[:40],
                )
            )

print("edited_rows_found =", len(hits))
for x in hits[:20]:
    print(x)
