# -*- coding: utf-8 -*-
import csv, glob, os
files = [f for f in glob.glob("data/change_20240101_20251201/*.csv")
         if not os.path.basename(f).startswith(("_", "000000"))]
print("scan", len(files))
removed = 0
errs = []
for f in files:
    try:
        with open(f, encoding="utf-8-sig", newline="") as fh:
            rows = list(csv.reader(fh))
        keep = [r for r in rows if not (r and r[0].startswith("2025-12-01") and r[0] > "2025-12-01 00:00:00")]
        if len(keep) != len(rows):
            removed += len(rows) - len(keep)
            with open(f, "w", encoding="utf-8", newline="") as fh:
                csv.writer(fh).writerows(keep)
    except Exception as e:
        errs.append((os.path.basename(f), str(e)[:60]))
print("removed", removed, "errs", len(errs), errs[:3])
