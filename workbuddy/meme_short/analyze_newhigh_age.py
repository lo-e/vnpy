# -*- coding: utf-8 -*-
"""分析定稿345笔: 距30天新高时间分布与表现, 验证 confirm_new_high 优化方向"""
import csv
import pymongo
import datetime as dt

D = "C:/Users/lo-e/Quant/vnpy/workbuddy/meme_short/data/change_20260101_20260812"
c = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
rows = list(csv.DictReader(open(D + "/_bt_pump8h_final.csv", encoding="utf-8-sig")))
FMT = "%Y-%m-%d %H:%M:%S"


def dist_30d(sym, ot_str):
    """开仓前30天窗口最高high的K, 返回 (距开仓小时数, 高点涨幅%相对开仓价)"""
    col = c["Workbuddy_5Min_Db"][sym.upper() + ".BINANCE"]
    t = dt.datetime.strptime(ot_str, FMT)
    lo = (t - dt.timedelta(days=30)).strftime(FMT)
    hi = (t - dt.timedelta(minutes=5)).strftime(FMT)
    bars = list(col.find({"openTimeLocal": {"$gte": lo, "$lte": hi}}).sort("openTimeLocal", 1))
    if not bars:
        return None, None
    bmax = max(bars, key=lambda x: float(x["high"]))
    hms = dt.datetime.strptime(bmax["openTimeLocal"], FMT)
    hours = (t - hms).total_seconds() / 3600
    return hours, bmax["openTimeLocal"]


print("总笔数:", len(rows))
# 距30天新高时间分组
buckets = {"<6h": [], "6-12h": [], "12-24h": [], "24-72h": [], "72-168h": [], ">168h": []}
n_none = 0
for r in rows:
    h, _ = dist_30d(r["symbol"], r["openTime"])
    if h is None:
        n_none += 1
        continue
    if h < 6: buckets["<6h"].append(r)
    elif h < 12: buckets["6-12h"].append(r)
    elif h < 24: buckets["12-24h"].append(r)
    elif h < 72: buckets["24-72h"].append(r)
    elif h < 168: buckets["72-168h"].append(r)
    else: buckets[">168h"].append(r)

print("数据不足30天(新上市):", n_none)
print("\n按[距30天新高时间]分组:")
print("%-10s %6s %8s %10s %12s" % ("分组", "笔数", "胜率", "均pnl%", "合计pp"))
for k, rs in buckets.items():
    if not rs: continue
    wins = sum(1 for x in rs if float(x["pnlPct"]) > 0)
    avg = sum(float(x["pnlPct"]) * float(x["weight"]) for x in rs) / sum(float(x["weight"]) for x in rs)
    tot = sum(float(x["pnlPct"]) * float(x["weight"]) for x in rs)
    print("%-10s %6d %7.1f%% %9.2f %+11.1f" % (k, len(rs), wins / len(rs) * 100, avg, tot))

# 对应地: 当前24h窗口AGE(已用AGE12)的30天新高距离, 看哪些单的30天新高很远但AGE12放行了
print("\n距30天新高 >72h 的单(AGE12可能漏掉的'旧新高'接刀):")
for r in rows:
    h, t0 = dist_30d(r["symbol"], r["openTime"])
    if h is not None and h > 72:
        print("  %-14s %s  距30天新高%6.1fh (新高@%s)  pnl=%+7.2f%% %s" % (r["symbol"], r["openTime"], h, t0, float(r["pnlPct"]), r["reason"]))
