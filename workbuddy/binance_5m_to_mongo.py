"""BTCUSDT 永续 5m OHLC -> MongoDB。

库: Workbuddy_5Min_Db
collection: BTCUSDT.BINANCE (命名规则: <SYMBOL>.<EXCHANGE>)
幂等: openTime 唯一索引 + upsert, 重复运行不产生重复文档。
时区: openTime/closeTime 存 UTC datetime(BSON ISODate, 物理精确)；
      openTimeLocal/closeTimeLocal 存北京时间字符串(UTC+8), 直观可读。
数据质量: 忽略当前未走完的 5m 块(进行中 K 不落库); 插入前防御性清理误入库的进行中/未来块。

用法:
    python binance_5m_to_mongo.py            # 默认测试窗口: 最近 1 小时
    set WINDOW_HOURS=24 后运行               # 扩展窗口
"""
import os
import requests
import pymongo
from datetime import datetime, timezone, timedelta

SYMBOL = "BTCUSDT"
EXCHANGE = "BINANCE"
DB_NAME = "Workbuddy_5Min_Db"
COLL_NAME = f"{SYMBOL}.{EXCHANGE}"
KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"

WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "1"))
STEP_MS = 5 * 60 * 1000  # 5m = 300000ms
LOCAL_TZ = timezone(timedelta(hours=8))  # 中国标准时间 UTC+8 (无夏令时)


def ms_to_dt(ms):
    """毫秒时间戳 -> UTC datetime(带时区, pymongo 存为 ISODate, 毫秒精度)"""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def local_str(ms):
    """毫秒时间戳 -> 北京时间字符串, 如 '2026-08-11 21:25:00'"""
    return ms_to_dt(ms).astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


def main():
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    # 忽略当前进行中块: endTime 对齐到最后一个完整 5m K 的 closeTime
    end_ms = (now_ms // STEP_MS) * STEP_MS - 1
    start_ms = end_ms - WINDOW_HOURS * 3600 * 1000 + 1
    cur_open_ms = (now_ms // STEP_MS) * STEP_MS  # 当前进行中块 openTime

    params = {
        "symbol": SYMBOL,
        "interval": "5m",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,
    }
    r = requests.get(KLINE_URL, params=params, timeout=20)
    r.raise_for_status()
    kl = r.json()
    print(f"拉取到 {len(kl)} 根完整 5m K 线 "
          f"({local_str(start_ms)} -> {local_str(end_ms)} 北京时间), "
          f"已忽略当前进行中块 {local_str(cur_open_ms)[11:16]}")

    docs = []
    for k in kl:
        docs.append({
            "openTime": ms_to_dt(k[0]),
            "openTimeLocal": local_str(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "closeTime": ms_to_dt(k[6]),
            "closeTimeLocal": local_str(k[6]),
            "quoteVolume": float(k[7]),
            "trades": int(k[8]),
            "takerBuyBaseVolume": float(k[9]),
            "takerBuyQuoteVolume": float(k[10]),
        })

    cli = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
    db = cli[DB_NAME]
    col = db[COLL_NAME]
    col.create_index("openTime", unique=True)

    # 防御性清理: 删掉误入库的进行中/未来块(openTime >= 当前进行中块)
    purged = col.delete_many({"openTime": {"$gte": ms_to_dt(cur_open_ms)}}).deleted_count
    if purged:
        print(f"清理进行中/未来块 {purged} 条")

    inserted = 0
    for d in docs:
        res = col.replace_one({"openTime": d["openTime"]}, d, upsert=True)
        if res.upserted_id:
            inserted += 1

    total = col.count_documents({})
    print(f"新插入 {inserted} 条, collection 现有 {total} 条")
    print(f"库: {DB_NAME} / collection: {COLL_NAME}")

    first = col.find_one(sort=[("openTime", 1)])
    last = col.find_one(sort=[("openTime", -1)])
    if first:
        print(f"首条: {first['openTimeLocal']} (openTime={first['openTime']}) "
              f"O={first['open']} H={first['high']} L={first['low']} C={first['close']} V={first['volume']}")
    if last:
        print(f"末条(最新完整块): {last['openTimeLocal']} "
              f"O={last['open']} H={last['high']} L={last['low']} C={last['close']} V={last['volume']}")


if __name__ == "__main__":
    main()
