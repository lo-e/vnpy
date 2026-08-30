# -*- coding: utf-8 -*-
"""补 PUMPUSDT 近一个月纯 5m K 线进 MongoDB(对齐 ensure_klines 格式)。
老规矩: 只补指定窗口, 幂等 upsert(openTime 唯一), 不删已有数据。
"""
import os, time, requests, pymongo
from datetime import datetime, timedelta, timezone

PROX = "http://127.0.0.1:10809"
os.environ.setdefault("HTTP_PROXY", PROX)
os.environ.setdefault("HTTPS_PROXY", PROX)

URI = "mongodb://127.0.0.1:27017/"
DB = "Workbuddy_5Min_Db"
SYM = "PUMPUSDT"
KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
STEP5 = 5 * 60 * 1000
BJ = timezone(timedelta(hours=8))


def local_str(ms, tz=BJ):
    return datetime.fromtimestamp(ms / 1000, tz).strftime("%Y-%m-%d %H:%M:%S")


def fetch(start_ms, end_ms):
    out, cursor = [], start_ms
    while True:
        p = {"symbol": SYM, "interval": "5m", "startTime": cursor,
             "endTime": end_ms, "limit": 1000}
        r = requests.get(KLINE_URL, params=p, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        out.extend(batch)
        last = batch[-1][0]
        if last >= end_ms - STEP5:
            break
        cursor = last + STEP5
        time.sleep(0.05)
    return out


def upsert(klines):
    c = pymongo.MongoClient(URI)
    col = c[DB][f"{SYM}.BINANCE"]
    col.create_index([("openTime", 1)], unique=True)
    n = 0
    for k in klines:
        doc = {
            "openTime": k[0],
            "openTimeLocal": local_str(k[0]),
            "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
            "volume": float(k[5]),
            "closeTime": k[6],
            "closeTimeLocal": local_str(k[6]),
            "quoteVolume": float(k[7]),
            "trades": int(float(k[8])),
            "takerBuyBaseVolume": float(k[9]),
            "takerBuyQuoteVolume": float(k[10]),
        }
        col.update_one({"openTime": doc["openTime"]}, {"$set": doc}, upsert=True)
        n += 1
    c.close()
    return n


if __name__ == "__main__":
    # 窗口: 近 ~32 天(北京时间), 确保完整覆盖"近一个月"
    end_bj = datetime.now(BJ)
    start_bj = end_bj - timedelta(days=32)
    start_ms = int(start_bj.timestamp() * 1000)
    end_ms = int(end_bj.timestamp() * 1000)
    print(f"拉取 {SYM} 5m [{start_bj}] ~ [{end_bj}]", flush=True)
    kl = fetch(start_ms, end_ms)
    print(f"  拉到 {len(kl)} 根, upsert 中...", flush=True)
    n = upsert(kl)
    print(f"  完成 upsert {n} 根", flush=True)
