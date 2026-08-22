"""用户验证: 删除 BTCUSDT 全部数据, 下载今天(北京) 23:00:00 ~ 23:25:00 的 5m K 落库。
统一规范: openTime/closeTime = UTC 毫秒时间戳(int); openTimeLocal/closeTimeLocal = 北京可读字符串。
"""
import requests
import pymongo
from datetime import datetime, timezone, timedelta
from pymongo import UpdateOne

LOCAL = timezone(timedelta(hours=8))
cli = pymongo.MongoClient("mongodb://127.0.0.1:27017")
db = cli["Workbuddy_5Min_Db"]
kc = db["BTCUSDT.BINANCE"]

# 1) 删除全部
kc.drop()
kc.create_index("openTime", unique=True)
print("已删除 BTCUSDT.BINANCE 全部数据")


def bj_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=LOCAL).strftime("%Y-%m-%d %H:%M:%S")


# 2) 下载 今天 23:00:00 ~ 23:25:00 (北京) = UTC 15:00 ~ 15:25
start_ms = int(datetime(2026, 8, 12, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
end_ms = int(datetime(2026, 8, 12, 15, 25, 0, tzinfo=timezone.utc).timestamp() * 1000)
r = requests.get("https://fapi.binance.com/fapi/v1/klines",
                 params={"symbol": "BTCUSDT", "interval": "5m",
                         "startTime": start_ms, "endTime": end_ms, "limit": 1000}, timeout=30)
kl = r.json()
print(f"拉取 {len(kl)} 根 (北京 23:00 ~ 23:25)")

docs = []
for k in kl:
    docs.append({
        "openTime": int(k[0]),
        "openTimeLocal": bj_str(k[0]),
        "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
        "volume": float(k[5]), "closeTime": int(k[6]),
        "closeTimeLocal": bj_str(k[6]),
        "quoteVolume": float(k[7]), "trades": int(k[8]),
        "takerBuyBaseVolume": float(k[9]), "takerBuyQuoteVolume": float(k[10]),
    })
if docs:
    kc.bulk_write([UpdateOne({"openTime": d["openTime"]}, {"$set": d}, upsert=True)
                   for d in docs], ordered=False)
print(f"落库 {len(docs)} 根")

# 3) 展示
print("\n=== 库里内容 ===")
for d in kc.find().sort("openTime", 1):
    print(f"  openTime={d['openTime']} | openTimeLocal={d['openTimeLocal']} | "
          f"closeTime={d['closeTime']} | closeTimeLocal={d['closeTimeLocal']} | C={d['close']}")
print("字段:", [f for f in kc.find_one()])
