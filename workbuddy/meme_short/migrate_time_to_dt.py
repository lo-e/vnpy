"""存量迁移(幂等): 
1) openTime/closeTime 毫秒 int -> UTC datetime;
2) 补 openTimeLocal/closeTimeLocal 北京时间字符串(UTC+8)。
"""
import pymongo
from datetime import datetime, timezone, timedelta

cli = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
col = cli["Workbuddy_5Min_Db"]["BTCUSDT.BINANCE"]
LOCAL_TZ = timezone(timedelta(hours=8))


def ms_to_dt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def local_str(dt_naive_utc):
    """读回的 naive UTC datetime -> 北京时间字符串"""
    return dt_naive_utc.replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


n = 0
for doc in col.find():
    upd = {}
    if isinstance(doc.get("openTime"), (int, float)):
        upd["openTime"] = ms_to_dt(doc["openTime"])
    if isinstance(doc.get("closeTime"), (int, float)):
        upd["closeTime"] = ms_to_dt(doc["closeTime"])
    ot = upd.get("openTime", doc.get("openTime"))
    ct = upd.get("closeTime", doc.get("closeTime"))
    if "openTimeLocal" not in doc and ot is not None:
        upd["openTimeLocal"] = local_str(ot)
    if "closeTimeLocal" not in doc and ct is not None:
        upd["closeTimeLocal"] = local_str(ct)
    if upd:
        col.update_one({"_id": doc["_id"]}, {"$set": upd})
        n += 1

print("updated docs:", n)
d = col.find_one(sort=[("openTime", 1)])
print("openTime:", d["openTime"], "| openTimeLocal:", d.get("openTimeLocal"))
