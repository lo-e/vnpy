import csv, os, datetime, glob

DIR = r"C:\Users\lo-e\Quant\vnpy\data\change_20251201_20260816"
NEW_COL = "ts_ms_readable_cst"
CST = datetime.timezone(datetime.timedelta(hours=8))  # 北京时间 UTC+8

files = sorted(glob.glob(os.path.join(DIR, "ef_*.csv")))
n_done = 0
n_skip = 0
for f in files:
    with open(f, newline='', encoding='utf-8') as fh:
        rows = list(csv.reader(fh))
    if not rows:
        continue
    # 去掉尾部空行
    while rows and (len(rows[-1]) == 0 or (len(rows[-1]) == 1 and rows[-1][0].strip() == '')):
        rows.pop()
    header = rows[0]
    if NEW_COL in header:
        n_skip += 1
        continue
    ti = header.index('ts_ms') if 'ts_ms' in header else 0
    header = header + [NEW_COL]
    out = [header]
    for row in rows[1:]:
        if not row or (len(row) == 1 and row[0].strip() == ''):
            out.append(row)
            continue
        ts = int(row[ti])
        dt = datetime.datetime.fromtimestamp(ts / 1000, tz=CST)
        s = dt.strftime('%Y-%m-%d %H:%M:%S+08:00')
        out.append(row + [s])
    with open(f, 'w', newline='', encoding='utf-8') as fh:
        csv.writer(fh).writerows(out)
    n_done += 1

print(f"done: 新增列 {n_done} 个文件, 跳过(已有) {n_skip} 个")
