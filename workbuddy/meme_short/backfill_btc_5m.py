# -*- coding: utf-8 -*-
"""
backfill_btc_5m.py — 补 BTCUSDT.BINANCE 5m 历史数据(头+尾)
==================================================================
老规矩: 先查 DB 范围, 缺的补头/补尾, 幂等 upsert。
复用回测 fetch/upsert 逻辑, 分批拉取+批量写入防止内存堆积, 带代理自愈。

端点: fapi 合约 (与 BTCUSDT.BINANCE 库同源)
代理: HTTP_PROXY/HTTPS_PROXY=127.0.0.1:10809 (requests 自动走)

用法:
    python backfill_btc_5m.py
"""
import os
import time
import requests
import pymongo
from pymongo import UpdateOne
from datetime import datetime, timezone, timedelta

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME = "Workbuddy_5Min_Db"
SYM = "BTCUSDT"
COL = SYM + ".BINANCE"
KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
STEP5 = 5 * 60 * 1000
TARGET_START_MS = int(datetime(2019, 9, 1, tzinfo=timezone.utc).timestamp() * 1000)  # 2019-09-01 UTC


def local_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def check_proxy():
    try:
        r = requests.get("https://fapi.binance.com/fapi/v1/time", timeout=8)
        return r.status_code == 200
    except Exception:
        return False


def fetch_page(sym, start_ms, end_ms, attempt=0):
    params = {"symbol": sym, "interval": "5m",
              "startTime": start_ms, "endTime": end_ms, "limit": 1000}
    try:
        r = requests.get(KLINE_URL, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        if attempt < 3:
            time.sleep(3)
            return fetch_page(sym, start_ms, end_ms, attempt + 1)
        if not check_proxy():
            print("  ⚠️ 代理不可用, 等待恢复...", flush=True)
            while not check_proxy():
                time.sleep(60)
        print(f"  [fetch] 重试3次失败: {e}", flush=True)
        raise


def upsert(kc, kl):
    docs = []
    seen = set()
    for k in kl:
        ot = int(k[0])
        if ot in seen:
            continue
        seen.add(ot)
        docs.append({
            "openTime": ot,
            "openTimeLocal": local_str(ot),
            "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
            "volume": float(k[5]), "closeTime": int(k[6]),
            "closeTimeLocal": local_str(int(k[6])),
            "quoteVolume": float(k[7]), "trades": int(k[8]),
            "takerBuyBaseVolume": float(k[9]), "takerBuyQuoteVolume": float(k[10]),
        })
    if docs:
        kc.bulk_write([UpdateOne({"openTime": d["openTime"]}, {"$set": d}, upsert=True)
                       for d in docs], ordered=False)


def backfill_range(cli, start_ms, end_ms, tag):
    kc = cli[DB_NAME][COL]
    kc.create_index([("openTime", 1)], unique=True)  # 幂等保障 (唯一索引+upsert)
    cursor = start_ms
    total = 0
    page = 0
    t0 = time.time()
    while cursor <= end_ms:
        kl = fetch_page(SYM, cursor, end_ms)
        if not kl:
            print(f"  [{tag}] 空页, 结束", flush=True)
            break
        upsert(kc, kl)
        total += len(kl)
        page += 1
        nxt = int(kl[-1][0]) + STEP5
        if nxt <= cursor:
            print(f"  [{tag}] cursor 不前进, 终止", flush=True)
            break
        cursor = nxt
        if page % 25 == 0:
            dt = (time.time() - t0) / 60
            print(f"  [{tag}] 已补 {total} 根, 进度 {local_str(cursor)} ({dt:.1f}min)", flush=True)
    print(f"  [{tag}] 完成, 共补 {total} 根", flush=True)
    return total


def main():
    cli = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    kc = cli[DB_NAME][COL]
    n = kc.count_documents({})
    first = kc.find_one(sort=[("openTime", 1)])["openTime"] if n else None
    last = kc.find_one(sort=[("openTime", -1)])["openTime"] if n else None
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    print(f"DB 现有: n={n}, first={local_str(first) if first else None}, last={local_str(last) if last else None}", flush=True)
    print(f"目标: 头从 {local_str(TARGET_START_MS)} 起; 尾到 {local_str(now_ms)}", flush=True)

    # 补头
    if first is None or TARGET_START_MS < first:
        head_end = (first - STEP5) if first else now_ms
        print(f"▶ 补头: {local_str(TARGET_START_MS)} ~ {local_str(head_end)}", flush=True)
        backfill_range(cli, TARGET_START_MS, head_end, "HEAD")
    else:
        print("✅ 头部已覆盖, 无需补头", flush=True)

    # 补尾
    if last is None or now_ms > last + STEP5:
        tail_start = (last + STEP5) if last else TARGET_START_MS
        print(f"▶ 补尾: {local_str(tail_start)} ~ {local_str(now_ms)}", flush=True)
        backfill_range(cli, tail_start, now_ms, "TAIL")
    else:
        print("✅ 尾部已最新, 无需补尾", flush=True)

    # 验证
    n2 = kc.count_documents({})
    f2 = kc.find_one(sort=[("openTime", 1)])["openTime"]
    l2 = kc.find_one(sort=[("openTime", -1)])["openTime"]
    print(f"补完 DB: n={n2}, first={local_str(f2)}, last={local_str(l2)}", flush=True)


if __name__ == "__main__":
    main()
