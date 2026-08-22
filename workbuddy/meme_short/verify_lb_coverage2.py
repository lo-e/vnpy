# -*- coding: utf-8 -*-
"""修正验证:
1. 用 2026-02-15 00:00 之后该币首次上榜(回测实际 first_sig) 而不是全局首次上榜
2. 对比: 库里最早 vs first_sig_2_15 - 45d / -30d(看左边界是否覆盖)
3. 连续性检查: fetch 过的币窗口内是否有中间缺口(相邻K间隔>5min 的段)
"""
import csv
import re
import pymongo
import datetime as dt

c = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
db = c["Workbuddy_5Min_Db"]
D = "data/change_20260101_20260812"
START = dt.datetime(2026, 2, 15, 0, 0, 0)

# 信号文件: 每币在 2-15 之后的首次上榜
first_after = {}
with open(D + "/000000_top1_rise.csv", encoding="utf-8-sig") as f:
    for r in csv.DictReader(f):
        s = r["topSymbol"].lower()
        t = r.get("pointTimeLocal") or r.get("pointTime")
        ts = dt.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
        if ts < START:
            continue
        if s not in first_after or t < first_after[s]:
            first_after[s] = t

log = open("workbuddy/_exp5_lb45.log", encoding="utf-8", errors="ignore").read()
fetched = set(re.findall(r"\[fetch\] (\w+) 第1次", log))
print("2-15后信号币数:", len(first_after), "| fetch过的币数:", len(fetched))

def continuity_check(sym):
    """检查库里该币数据连续性: 返回缺口段数(相邻K间隔>5min的gap数)"""
    col = db[sym.upper() + ".BINANCE"]
    bars = list(col.find({}, {"openTime": 1}).sort("openTime", 1))
    gaps = 0
    for a, b in zip(bars, bars[1:]):
        if int(b["openTime"]) - int(a["openTime"]) > 5 * 60 * 1000:
            gaps += 1
    return len(bars), gaps

print("\n=== fetch 过的币(2-15后首上榜 vs 库最早 vs 45d窗口起点 vs 连续性) ===")
for s in sorted(fetched)[:8]:
    if s not in first_after:
        continue
    t_f = dt.datetime.strptime(first_after[s], "%Y-%m-%d %H:%M:%S")
    win45 = t_f - dt.timedelta(days=45)
    col = db[s.upper() + ".BINANCE"]
    k0 = col.find_one(sort=[("openTimeLocal", 1)])
    t_db = dt.datetime.strptime(k0["openTimeLocal"], "%Y-%m-%d %H:%M:%S")
    nb, ng = continuity_check(s)
    cover = "✅覆盖" if t_db <= win45 else "❌左缺"
    print(f"  {s:12s} 首上榜={first_after[s]} 45d起点={win45.strftime('%m-%d')} 库最早={t_db.strftime('%m-%d %H:%M')} {cover} 库K={nb} 缺口段={ng}")

print("\n=== 未 fetch 的币(抽查5个, 应: 库最早≤45d起点 且 无中间缺口) ===")
all_syms = [x.replace(".BINANCE", "").lower() for x in db.list_collection_names() if x.endswith(".BINANCE")]
nf = [s for s in all_syms if s not in fetched and s in first_after]
for s in sorted(nf)[:5]:
    t_f = dt.datetime.strptime(first_after[s], "%Y-%m-%d %H:%M:%S")
    win45 = t_f - dt.timedelta(days=45)
    col = db[s.upper() + ".BINANCE"]
    k0 = col.find_one(sort=[("openTimeLocal", 1)])
    t_db = dt.datetime.strptime(k0["openTimeLocal"], "%Y-%m-%d %H:%M:%S")
    nb, ng = continuity_check(s)
    cover = "✅覆盖" if t_db <= win45 else "❌左缺"
    print(f"  {s:12s} 首上榜={first_after[s]} 45d起点={win45.strftime('%m-%d')} 库最早={t_db.strftime('%m-%d %H:%M')} {cover} 库K={nb} 缺口段={ng}")
