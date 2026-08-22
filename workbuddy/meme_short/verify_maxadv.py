# -*- coding: utf-8 -*-
"""核查旧版 maxAdvPct 的真实算法口径：对 351 笔按持仓期复算多种口径 vs 旧版值"""
import csv
import pymongo

D = "C:/Users/lo-e/Quant/vnpy/workbuddy/meme_short/data/change_20260101_20260812"
c = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)

rows = list(csv.DictReader(open(D + "/_bt_pump8h.csv", encoding="utf-8-sig")))
print("旧版笔数:", len(rows))


def calc(sym, ot, ct, entry, cp):
    """返回持仓期各口径的 max 涨幅(相对开仓价)"""
    col = c["Workbuddy_5Min_Db"][sym.upper() + ".BINANCE"]
    bars = list(col.find({"openTimeLocal": {"$gte": ot, "$lte": ct}}).sort("openTimeLocal", 1))
    if not bars:
        return None
    mxh = max((float(x["high"]) - entry) / entry * 100 for x in bars)
    mxcl = max((float(x["close"]) - entry) / entry * 100 for x in bars)
    mxoc = max(max(float(x["open"]), float(x["close"])) for x in bars)
    mxoc = (mxoc - entry) / entry * 100
    cpct = (cp - entry) / entry * 100
    return {"A_high": mxh, "B_close": mxcl, "C_max(close,cp)": max(mxcl, cpct),
            "D_max(open,close)": mxoc, "E_max(high,cp)": max(mxh, cpct), "K": len(bars)}


# 各口径匹配计数
tally = {"A_high": 0, "B_close": 0, "C_max(close,cp)": 0, "D_max(open,close)": 0, "E_max(high,cp)": 0}
sample = {}
for r in rows:
    ot, ct = r["openTime"], r["closeTime"]
    import datetime as dt
    ct5 = (dt.datetime.strptime(ct, "%Y-%m-%d %H:%M:%S") - dt.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    res = calc(r["symbol"], ot, ct5, float(r["openPrice"]), float(r["closePrice"]))
    if res is None:
        continue
    old = float(r["maxAdvPct"])
    for name, v in res.items():
        if name == "K":
            continue
        if abs(v - old) < 0.05:
            tally[name] += 1
    if r["symbol"] in ("sirenusdt", "icntusdt", "riverusdt"):
        sample[(r["symbol"], ot)] = (res, old)

print("\n各口径与旧版 maxAdvPct 匹配笔数 (|差|<0.05):")
for name, n in tally.items():
    print("  %-18s %d / %d" % (name, n, len(rows)))
print("\n抽样核对 (当前数据复算 vs 旧版):")
for (sym, ot), (res, old) in sample.items():
    print("  %s %s 旧版=%+8.2f" % (sym, ot, old))
    for name, v in res.items():
        if name != "K":
            print("      %-18s %+8.2f" % (name, v))
