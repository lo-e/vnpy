# -*- coding: utf-8 -*-
"""统计每笔单开仓时距 24h 最高价的时间(小时), 评估"接刀单"规模与表现"""
import csv
import pymongo
import datetime as dt

D = "C:/Users/lo-e/Quant/vnpy/workbuddy/meme_short/data/change_20260101_20260812"
c = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
rows = list(csv.DictReader(open(D + "/_bt_pump8h_final.csv", encoding="utf-8-sig")))
print("总笔数:", len(rows))

FMT = "%Y-%m-%d %H:%M:%S"


def dist_high(sym, ot_str):
    """开仓时刻前24h窗口内最高high的openTime, 返回 (距开仓小时数, 高点涨幅%, 开仓价相对高点%)"""
    col = c["Workbuddy_5Min_Db"][sym.upper() + ".BINANCE"]
    t = dt.datetime.strptime(ot_str, FMT)
    lo = (t - dt.timedelta(hours=24)).strftime(FMT)
    hi = (t - dt.timedelta(minutes=5)).strftime(FMT)
    bars = list(col.find({"openTimeLocal": {"$gte": lo, "$lte": hi}}).sort("openTimeLocal", 1))
    if not bars:
        return None
    bmax = max(bars, key=lambda x: float(x["high"]))
    hms = dt.datetime.strptime(bmax["openTimeLocal"], FMT)
    hours = (t - hms).total_seconds() / 3600
    # 开仓价(用CSV openPrice)相对高点
    return hours, bmax["openTimeLocal"]


# jtousdt 详情
for r in rows:
    if r["symbol"] == "jtousdt" and r["openTime"] == "2026-05-08 17:20:00":
        res = dist_high("jtousdt", "2026-05-08 17:20:00")
        print("\njtousdt 2026-05-08 17:20:00: pnl=%s reason=%s maxAdvPct=%s" % (r["pnlPct"], r["reason"], r["maxAdvPct"]))
        print("  距24h最高点: %.1f 小时 (高点@%s)" % (res[0], res[1]))

# 全样本分布
buckets = {"<6h": [], "6-12h": [], "12-18h": [], ">18h": []}
jihua = []
for r in rows:
    res = dist_high(r["symbol"], r["openTime"])
    if res is None:
        continue
    h = res[0]
    if h < 6: buckets["<6h"].append(r)
    elif h < 12: buckets["6-12h"].append(r)
    elif h < 18: buckets["12-18h"].append(r)
    else: buckets[">18h"].append(r)

print("\n按[距24h最高点时间]分组的规模与表现:")
print("%-8s %6s %8s %10s %12s" % ("分组", "笔数", "胜率", "均pnl%", "合计pp"))
for k, rs in buckets.items():
    if not rs: continue
    wins = sum(1 for x in rs if float(x["pnlPct"]) > 0)
    avg = sum(float(x["pnlPct"]) * float(x["weight"]) for x in rs) / sum(float(x["weight"]) for x in rs)
    tot = sum(float(x["pnlPct"]) * float(x["weight"]) for x in rs)
    print("%-8s %6d %7.1f%% %9.2f %+11.1f" % (k, len(rs), wins / len(rs) * 100, avg, tot))

# >=15h 的明细
print("\n距24h最高点 >= 15h 的单(疑似接刀):")
n15 = [r for r in rows if (lambda h: h is not None and h >= 15)(dist_high(r["symbol"], r["openTime"])[0] if dist_high(r["symbol"], r["openTime"]) else None)]
for r in n15:
    h = dist_high(r["symbol"], r["openTime"])[0]
    print("  %-12s %s  距高点%5.1fh  pnl=%+7.2f%% %s" % (r["symbol"], r["openTime"], h, float(r["pnlPct"]), r["reason"]))
if n15:
    wins = sum(1 for x in n15 if float(x["pnlPct"]) > 0)
    tot = sum(float(x["pnlPct"]) * float(x["weight"]) for x in n15)
    print("  => %d 笔, 胜率 %.1f%%, 合计 %+.1f pp" % (len(n15), wins / len(n15) * 100, tot))
