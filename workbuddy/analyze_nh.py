# -*- coding: utf-8 -*-
"""梯度测试: 不同"冲高窗口"(1/3/6/12/24h) vs 锚(24h/30天) 的过滤效果(静态)"""
import csv
import pymongo
import datetime as dt

D = "C:/Users/lo-e/Quant/vnpy/workbuddy/data/change_20260101_20260812"
c = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
rows = list(csv.DictReader(open(D + "/_bt_pump8h_final.csv", encoding="utf-8-sig")))
FMT = "%Y-%m-%d %H:%M:%S"

WINS = {1: 1, 3: 3, 6: 6, 12: 12, 24: 24}
recs = []
for r in rows:
    col = c["Workbuddy_5Min_Db"][r["symbol"].upper() + ".BINANCE"]
    t = dt.datetime.strptime(r["openTime"], FMT)
    hi = (t - dt.timedelta(minutes=5)).strftime(FMT)
    lo30 = (t - dt.timedelta(days=30)).strftime(FMT)
    b30 = list(col.find({"openTimeLocal": {"$gte": lo30, "$lte": hi}}).sort("openTimeLocal", 1))
    if not b30:
        continue
    h30 = max(float(x["high"]) for x in b30)
    hmax = {1: None, 3: None, 6: None, 12: None, 24: None}
    for w in WINS:
        lo = (t - dt.timedelta(hours=w)).strftime(FMT)
        bw = [x for x in b30 if x["openTimeLocal"] >= lo]
        hmax[w] = max(float(x["high"]) for x in bw)
    recs.append((r, hmax, h30))

# 组合: (窗口h, 锚, 锚名)
combos = [(6, 720, "6h冲30天"), (12, 720, "12h冲30天"), (3, 720, "3h冲30天"),
          (1, 720, "1h冲30天"), (1, 24, "1h冲24h"), (3, 24, "3h冲24h"), (6, 24, "6h冲24h")]
print("%-10s %6s %8s %10s %12s  剔除明细" % ("组合", "保留", "剔除", "剔胜率", "剔合计pp"))
for w, anch, name in combos:
    keep, drop = [], []
    for r, hmax, h30 in recs:
        anch_max = h30 if anch == 720 else hmax[24]
        if hmax[w] >= anch_max:
            keep.append(r)
        else:
            drop.append(r)
    d_wins = sum(1 for x in drop if float(x["pnlPct"]) > 0)
    d_tot = sum(float(x["pnlPct"]) * float(x["weight"]) for x in drop)
    d_wr = d_wins / len(drop) * 100 if drop else 0
    print("%-10s %6d %6d %7.1f%% %+11.1f   " % (name, len(keep), len(drop), d_wr, d_tot), end="")
    if drop:
        print("; ".join("%s%+.0f" % (x["symbol"][:8], float(x["pnlPct"])) for x in drop[:6]), end="")
        if len(drop) > 6:
            print(" ...", end="")
    print()
