# -*- coding: utf-8 -*-
"""C: 新高超越幅度 | D: 30天内创30天新高次数 —— 定稿345笔分析"""
import csv
import pymongo
import datetime as dt

D = "C:/Users/lo-e/Quant/vnpy/workbuddy/meme_short/data/change_20260101_20260812"
c = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
rows = list(csv.DictReader(open(D + "/_bt_pump8h_final.csv", encoding="utf-8-sig")))
FMT = "%Y-%m-%d %H:%M:%S"


def cd_metrics(sym, ot_str):
    """返回 (24h最高high/30天最高high-1 百分比, 30天窗口内创30天滚动新高次数)"""
    col = c["Workbuddy_5Min_Db"][sym.upper() + ".BINANCE"]
    t = dt.datetime.strptime(ot_str, FMT)
    lo30 = (t - dt.timedelta(days=30)).strftime(FMT)
    lo24 = (t - dt.timedelta(hours=24)).strftime(FMT)
    hi = (t - dt.timedelta(minutes=5)).strftime(FMT)
    b30 = list(col.find({"openTimeLocal": {"$gte": lo30, "$lte": hi}}).sort("openTimeLocal", 1))
    if not b30:
        return None, None
    h30 = max(float(x["high"]) for x in b30)
    b24 = [x for x in b30 if x["openTimeLocal"] >= lo24]
    h24 = max(float(x["high"]) for x in b24)
    over = (h24 / h30 - 1) * 100
    # D: 30天窗口内滚动新高次数(每根K的high超过此前窗口内所有high)
    cnt = 0
    run_max = -1e18
    for x in b30:
        h = float(x["high"])
        if h > run_max:
            run_max = h
            cnt += 1
    return over, cnt


print("=== C: 新高超越幅度 (24h最高 / 30天最高 - 1) ===")
cb = {"0-3%": [], "3-8%": [], "8-15%": [], "15-30%": [], ">30%": []}
for r in rows:
    over, _ = cd_metrics(r["symbol"], r["openTime"])
    if over is None:
        continue
    if over < 3: cb["0-3%"].append(r)
    elif over < 8: cb["3-8%"].append(r)
    elif over < 15: cb["8-15%"].append(r)
    elif over < 30: cb["15-30%"].append(r)
    else: cb[">30%"].append(r)
print("%-8s %6s %8s %10s %12s" % ("分组", "笔数", "胜率", "均pnl%", "合计pp"))
for k, rs in cb.items():
    if not rs: continue
    wins = sum(1 for x in rs if float(x["pnlPct"]) > 0)
    avg = sum(float(x["pnlPct"]) * float(x["weight"]) for x in rs) / sum(float(x["weight"]) for x in rs)
    tot = sum(float(x["pnlPct"]) * float(x["weight"]) for x in rs)
    print("%-8s %6d %7.1f%% %9.2f %+11.1f" % (k, len(rs), wins / len(rs) * 100, avg, tot))

print()
print("=== D: 30天窗口内创30天滚动新高次数 ===")
db = {"1次": [], "2次": [], "3次": [], "4+次": []}
for r in rows:
    _, cnt = cd_metrics(r["symbol"], r["openTime"])
    if cnt is None:
        continue
    if cnt == 1: db["1次"].append(r)
    elif cnt == 2: db["2次"].append(r)
    elif cnt == 3: db["3次"].append(r)
    else: db["4+次"].append(r)
print("%-6s %6s %8s %10s %12s" % ("分组", "笔数", "胜率", "均pnl%", "合计pp"))
for k, rs in db.items():
    if not rs: continue
    wins = sum(1 for x in rs if float(x["pnlPct"]) > 0)
    avg = sum(float(x["pnlPct"]) * float(x["weight"]) for x in rs) / sum(float(x["weight"]) for x in rs)
    tot = sum(float(x["pnlPct"]) * float(x["weight"]) for x in rs)
    print("%-6s %6d %7.1f%% %9.2f %+11.1f" % (k, len(rs), wins / len(rs) * 100, avg, tot))
