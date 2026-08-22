# -*- coding: utf-8 -*-
"""增量追加 2025-12-01 全天信号点(2026-08-21 超指出: 窗口终点应为 12-01 而非 11-30)。
- 每币: probe 上市时间 → 若 2025-12-01 前已上市 → 拉 12-01 K 线(缺则补) → 计算 12-01 全天 144 点 change → 追加到 CSV。
- 复用 binance_24h_change_all.py 的网络/代理逻辑。
"""
import os
import sys
import json
import csv
import glob
import time
import requests
import pymongo
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

OUT_DIR = "data/change_20240101_20251201"
KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
EXCHANGE = "BINANCE"
STEP5 = 5 * 60 * 1000
DAY_MS = 24 * 3600 * 1000
LOCAL_TZ = timezone(timedelta(hours=8))

db = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)["Workbuddy_5Min_Db"]

def ms_to_local(ms):
    return datetime.fromtimestamp(ms / 1000, tz=LOCAL_TZ)

def local_str(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def probe_listing(sym):
    for a in range(3):
        try:
            r = requests.get(KLINE_URL, params={"symbol": sym, "interval": "5m",
                                                "startTime": 1577836800000, "limit": 1}, timeout=30)
            if r.status_code == 200:
                j = r.json()
                return j[0][0] if j else None
        except Exception:
            pass
        time.sleep(3)
    return None

def fetch_kl(sym, start_ms, end_ms):
    out = []
    cur = start_ms
    while cur <= end_ms:
        r = requests.get(KLINE_URL, params={"symbol": sym, "interval": "5m",
                                            "startTime": cur, "endTime": end_ms, "limit": 1000}, timeout=30)
        r.raise_for_status()
        kl = r.json()
        if not kl:
            break
        out.extend(kl)
        nxt = kl[-1][0] + STEP5
        if nxt <= cur:
            break
        cur = nxt
    return out

def upsert(kc, kl):
    for k in kl:
        kc.update_one({"openTime": k[0]}, {"$set": {
            "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
            "volume": float(k[5]), "quoteVolume": float(k[7]),
            "takerBuyQuoteVolume": float(k[9])}}, upsert=True)

# 12-01 全天 144 点
pts = []
t = datetime(2025, 12, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
while t <= datetime(2025, 12, 1, 23, 50, 0, tzinfo=LOCAL_TZ):
    pts.append(t)
    t += timedelta(minutes=10)
print(f"追加点: {len(pts)} 个 (2025-12-01 00:00 ~ 23:50)")

symfiles = sorted(glob.glob(os.path.join(OUT_DIR, "*.csv")))
symfiles = [f for f in symfiles if not os.path.basename(f).startswith(("_", "000000"))]
print(f"处理 {len(symfiles)} 币 CSV")

ok_cnt = skip_cnt = 0
for f in symfiles:
    sym = os.path.splitext(os.path.basename(f))[0].upper()
    # 跳过上市晚于 12-01 的币
    listing = probe_listing(sym)
    if listing is None or listing > int(datetime(2025, 12, 1, 0, 0, 0, tzinfo=LOCAL_TZ).timestamp() * 1000):
        skip_cnt += 1
        continue
    # 幂等: 最后一行 == 23:50 才视为已完整追加; 否则追加(跳过已存在的 00:00 点)
    with open(f, "r", encoding="utf-8-sig") as fh:
        last_line = fh.readlines()[-1]
    if last_line.startswith("2025-12-01 23:50"):
        print(f"  [skip] {sym} 已完整追加", flush=True)
        skip_cnt += 1
        continue
    if last_line.startswith("2025-12-01 00:00"):
        print(f"  [dup] {sym} 已有00:00点, 从00:10起追加", flush=True)
        first_skip = 1
    else:
        first_skip = 0
    kc = db[f"{sym}.{EXCHANGE}"]
    kc.create_index("openTime", unique=True)
    # 需要的 K: ot1 ∈ [12-01 00:00-5m, 12-01 23:50-5m], ot2 ∈ [11-30 00:00-5m, 11-30 23:50-5m]
    need_lo = int(datetime(2025, 11, 30, 0, 0, 0, tzinfo=LOCAL_TZ).timestamp() * 1000) - STEP5
    need_hi = int(datetime(2025, 12, 1, 23, 50, 0, tzinfo=LOCAL_TZ).timestamp() * 1000)
    have = set(kc.distinct("openTime", {"openTime": {"$gte": need_lo, "$lte": need_hi}}))
    missing = [m for m in range(need_lo, need_hi + 1, STEP5) if m not in have]
    if missing:
        try:
            kl = fetch_kl(sym, missing[0], missing[-1])
            upsert(kc, kl)
            print(f"  [fetch] {sym} 补K {len(kl)} 根", flush=True)
        except Exception as e:
            print(f"  [fetch] {sym} 失败: {e}", flush=True)
            skip_cnt += 1
            continue
        have = set(kc.distinct("openTime", {"openTime": {"$gte": need_lo, "$lte": need_hi}}))
    docs = {d["openTime"]: d for d in kc.find({"openTime": {"$gte": need_lo, "$lte": need_hi}})}
    rows = []
    for p in pts:
        ot1 = int((p - timedelta(minutes=5)).timestamp() * 1000)
        ot2 = int((p - timedelta(hours=24, minutes=5)).timestamp() * 1000)
        k1 = docs.get(ot1)
        k2 = docs.get(ot2)
        if k1 is None or k2 is None:
            rows.append([local_str(p), None, None, None, "error", "数据缺失"])
            continue
        chg = (k1["close"] - k2["close"]) / k2["close"] * 100
        rows.append([local_str(p), k1["close"], k2["close"], round(chg, 6), "ok", ""])
    # 追加(幂等: 已有 00:00 点则从 00:10 起)
    with open(f, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        for r in rows[first_skip:]:
            w.writerow(r)
    ok_cnt += 1

print(f"完成: 追加 {ok_cnt} 币, 跳过 {skip_cnt} 币")
