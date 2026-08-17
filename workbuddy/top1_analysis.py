"""每整10分钟时间点 全合约 24h 涨跌幅 Top1 分析 (涨幅榜 + 跌幅榜)。

数据源: workbuddy/data/change_20260701_20260811/*.csv (527 币已生成)
时间点: 2026-07-01 00:00 -> 2026-08-11 23:30 (北京时间, 6046 点)
规则: 每点取 changePct 最大(涨幅榜) / 最小(跌幅榜) 且 status=ok 的合约;
      缺失/error 点按币跳过, 不影响该点其他币排名。
输出: workbuddy/data/change_20260701_20260811/top1_rise.csv  (涨幅榜)
      workbuddy/data/change_20260701_20260811/top1_fall.csv  (跌幅榜)
  列: pointTimeLocal, topSymbol, topChangePct, validCount(参与币数), skipCount(跳过币数)
"""
import os
import csv
import glob
from datetime import datetime, timezone, timedelta

SRC_DIR = os.environ.get("SRC_DIR", os.path.join("workbuddy", "data", "change_20260701_20260811"))
START_DT = os.environ.get("START_DT", "2026-07-01 00:00")
END_DT = os.environ.get("END_DT", "2026-08-11 23:30")
LOCAL_TZ = timezone(timedelta(hours=8))

# 标准时间点集合
pts = set()
t = datetime.strptime(START_DT, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ)
t_end = datetime.strptime(END_DT, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ)
while t <= t_end:
    pts.add(t.strftime("%Y-%m-%d %H:%M:%S"))
    t += timedelta(minutes=10)
print(f"标准时间点: {len(pts)} 个 ({START_DT} -> {END_DT})")

files = sorted(glob.glob(os.path.join(SRC_DIR, "*.csv")))
files = [f for f in files if "top1" not in os.path.basename(f)]
print(f"读取 {len(files)} 个合约 CSV")

# 边读边聚合: pt -> [max_chg, max_sym, min_chg, min_sym, count]
top = {}
for f in files:
    sym = os.path.splitext(os.path.basename(f))[0]
    with open(f, encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["status"] != "ok":
                continue
            pt = row["pointTimeLocal"]
            if pt not in pts:
                continue
            chg = float(row["changePct"])
            e = top.get(pt)
            if e is None:
                top[pt] = [chg, sym, chg, sym, 1]
            else:
                e[4] += 1
                if chg > e[0]:
                    e[0], e[1] = chg, sym
                if chg < e[2]:
                    e[2], e[3] = chg, sym


def write_out(path, pick):
    rows = []
    for pt in sorted(pts):
        e = top.get(pt)
        if e is None:
            continue
        rows.append([pt, e[pick + 1], round(e[pick], 6), e[4], len(files) - e[4]])
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["pointTimeLocal", "topSymbol", "topChangePct", "validCount", "skipCount"])
        w.writerows(rows)
    print(f"输出 {len(rows)} 行 -> {path}")
    return rows


rise = write_out(os.path.join(SRC_DIR, "000000_top1_rise.csv"), 0)
fall = write_out(os.path.join(SRC_DIR, "000000_top1_fall.csv"), 2)

print("\n涨幅榜 首3:", [r[:3] for r in rise[:3]])
print("跌幅榜 首3:", [r[:3] for r in fall[:3]])
print("跌幅榜 末3:", [r[:3] for r in fall[-3:]])
