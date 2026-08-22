# -*- coding: utf-8 -*-
"""验证"库里数据左边界 = 历史拉取窗口左边界"：
对 LB45 日志中 fetch 过的币 vs 未 fetch 的币，对比：
  - 信号文件中的首次上榜时间 first_sig
  - 库里该币最早 K 时间
期望：未 fetch 的币 库里最早 ≈ first_sig - 30d（历史 LB30 拉的窗口左边界）
      fetch 过的币 库里最早 ≈ first_sig - 45d（LB45 补拉后已到 45d）
"""
import csv
import re
import pymongo
import datetime as dt

c = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
db = c["Workbuddy_5Min_Db"]
D = "data/change_20260101_20260812"

# 1. 从 LB45 日志提取 fetch 过的币
log = open("workbuddy/_exp5_lb45.log", encoding="utf-8", errors="ignore").read()
fetched = set(re.findall(r"\[fetch\] (\w+) 第1次", log))
print("LB45 日志中 fetch 过的币数:", len(fetched))

# 2. 信号文件: 每币首次上榜时间 (pointTimeLocal)
first_sig = {}
with open(D + "/000000_top1_rise.csv", encoding="utf-8-sig") as f:
    for r in csv.DictReader(f):
        s = r["topSymbol"].lower()
        t = r.get("pointTimeLocal") or r.get("pointTime")
        if s not in first_sig or t < first_sig[s]:
            first_sig[s] = t

# 3. 抽 fetch 过的币 3 个 + 未 fetch 的币 3 个(按库里最早时间分布抽查)
fetch_syms = sorted(fetched)[:3]
all_syms = [x.replace(".BINANCE", "").lower() for x in db.list_collection_names() if x.endswith(".BINANCE")]
not_fetched = [s for s in all_syms if s not in fetched and s in first_sig]
not_fetched = sorted(not_fetched)[:5]  # 抽前 5 个未 fetch 的币

print("\n=== fetch 过的币(应: 库里最早 ≈ 首上榜-45d) ===")
for s in fetch_syms:
    col = db[s.upper() + ".BINANCE"]
    k0 = col.find_one(sort=[("openTimeLocal", 1)])
    if not k0 or s not in first_sig:
        print(f"  {s}: 无数据或无信号"); continue
    t_first = dt.datetime.strptime(first_sig[s], "%Y-%m-%d %H:%M:%S")
    t_db = dt.datetime.strptime(k0["openTimeLocal"], "%Y-%m-%d %H:%M:%S")
    delta = (t_first - t_db).total_seconds() / 3600 / 24
    print(f"  {s:14s} 首上榜={first_sig[s]}  库最早={k0['openTimeLocal']}  首上榜-库最早={delta:.1f}天")

print("\n=== 未 fetch 的币(应: 库里最早 ≈ 首上榜-30d, 证明只拉到30天窗口) ===")
for s in not_fetched:
    col = db[s.upper() + ".BINANCE"]
    k0 = col.find_one(sort=[("openTimeLocal", 1)])
    if not k0 or s not in first_sig:
        print(f"  {s}: 无数据或无信号"); continue
    t_first = dt.datetime.strptime(first_sig[s], "%Y-%m-%d %H:%M:%S")
    t_db = dt.datetime.strptime(k0["openTimeLocal"], "%Y-%m-%d %H:%M:%S")
    delta = (t_first - t_db).total_seconds() / 3600 / 24
    print(f"  {s:14s} 首上榜={first_sig[s]}  库最早={k0['openTimeLocal']}  首上榜-库最早={delta:.1f}天")
