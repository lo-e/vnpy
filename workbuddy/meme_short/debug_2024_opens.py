# -*- coding: utf-8 -*-
"""调试: 2024 窗口首信号为何 0 开仓 —— 逐段跑过滤链"""
import csv, bisect, pymongo, datetime
from datetime import timezone, timedelta
LOCAL = timezone(timedelta(hours=8))
STEP5 = 5 * 60 * 1000
DAY_MS = 24 * 3600 * 1000
CONFIRM_MS = 24 * 3600 * 1000
LOOKBACK_MS = 30 * 24 * 3600 * 1000
db = pymongo.MongoClient("mongodb://127.0.0.1:27017")["Workbuddy_5Min_Db"]
MAX_TOP1_PCT = 80.0
HIGH_AGE_MAX = 12
RATIO6_MIN = -1
CVD_W = 12
OFF_HIGH_MAX = 0.15

rows = list(csv.DictReader(open("data/change_20240101_20251201/000000_top1_rise.csv", encoding="utf-8-sig")))
print("=== 前 12 个信号逐段过滤链 ===\n")
shown = 0
for r in rows:
    if shown >= 12:
        break
    pt = r["pointTimeLocal"]
    sym = r["topSymbol"].upper()
    chg = float(r["topChangePct"])
    t = datetime.datetime.strptime(pt, "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL)
    t_ms = int(t.timestamp() * 1000)
    kc = db[f"{sym}.BINANCE"]
    lo = t_ms - 40 * DAY_MS
    docs = {d["openTime"]: d for d in kc.find({"openTime": {"$gte": lo, "$lte": t_ms}})}
    ots = sorted(docs.keys())
    if not ots:
        print(f"{pt} {sym} chg={chg:+.1f}% → [无K线数据]"); shown += 1; continue
    # 1) MAX_TOP1_PCT
    if chg >= MAX_TOP1_PCT:
        print(f"{pt} {sym} chg={chg:+.1f}% → [MAX_TOP1_PCT≥80 妖币]"); shown += 1; continue
    hi = bisect.bisect_right(ots, t_ms - STEP5) - 1
    if hi < 0:
        print(f"{pt} {sym} → [无已收盘K]"); shown += 1; continue
    # 2) HIGH_AGE_MAX: 距24h最高点
    lo1 = bisect.bisect_left(ots, t_ms - DAY_MS)
    imax = max(range(lo1, hi + 1), key=lambda i: docs[ots[i]]["high"])
    age = (ots[hi] - ots[imax]) / 3600000.0
    age_fail = age > HIGH_AGE_MAX
    # 3) RATIO6_MIN: r6
    px = docs.get(t_ms - STEP5)
    k6 = docs.get(t_ms - STEP5 - 6 * 3600 * 1000)
    k24 = docs.get(t_ms - STEP5 - DAY_MS)
    r6 = None; g24 = None
    r6_fail = False
    if px and k6 and k24 and k6["close"] > 0 and k24["close"] > 0:
        g6 = (px["close"] - k6["close"]) / k6["close"]
        g24 = (px["close"] - k24["close"]) / k24["close"]
        if g24 > 0:
            r6 = g6 / g24
            if RATIO6_MIN < 0 and r6 < 0:
                r6_fail = True
    # 4) confirm_new_high
    h1 = max(docs[ots[i]]["high"] for i in range(lo1, hi + 1))
    lo15 = bisect.bisect_left(ots, t_ms - LOOKBACK_MS)
    h15 = max(docs[ots[i]]["high"] for i in range(lo15, hi + 1))
    cnh = h1 >= h15
    # 5) CVD
    if hi + 1 >= 2 * CVD_W:
        rec = sum(2 * docs[ots[j]].get("takerBuyQuoteVolume", 0) - docs[ots[j]].get("quoteVolume", 0) for j in range(hi + 1 - CVD_W, hi + 1))
        ref = sum(2 * docs[ots[j]].get("takerBuyQuoteVolume", 0) - docs[ots[j]].get("quoteVolume", 0) for j in range(hi + 1 - 2 * CVD_W, hi + 1 - CVD_W))
        cvd = rec < ref
    else:
        cvd = True
    # 6) OFF_HIGH_MAX
    off = (h1 - px["close"]) / h1 * 100 if px else None
    off_fail = off is not None and off > OFF_HIGH_MAX * 100
    # 汇总
    fails = []
    if age_fail: fails.append(f"AGE={age:.1f}h>12h")
    if r6_fail: fails.append(f"r6={r6:.2f}<0")
    if not cnh: fails.append(f"新高失败 h24={h1:.4f}<h30={h15:.4f}")
    if not cvd: fails.append("CVD不成立")
    if off_fail: fails.append(f"OFF={off:.1f}%>15%")
    st = "❌ " + " | ".join(fails) if fails else "✅ 全部通过(可开仓)"
    print(f"{pt} {sym} chg={chg:+.1f}% g24={g24*100 if g24 else None:+.1f}% → {st}")
    shown += 1
