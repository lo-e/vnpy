"""MongoDB 数据访问：5m K 线 -> 1h 聚合 + 日级成交额聚合。

采用服务端聚合（$group），只回传聚合结果，避免拉取亿级原始 5m 文档。
返回 numpy 数组，便于后续向量化指标计算。
"""

import pymongo
import numpy as np
from lib import config

_CLIENT = None


def client():
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = pymongo.MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=8000)
    return _CLIENT


def _coll(symbol):
    db = client()[config.DB_5M]
    return db[f"{symbol}.{config.EXCHANGE}"]


def load_1h(symbol, start_ms=None, end_ms=None):
    """返回该 symbol 的 1h 聚合 K 线（ dict of np arrays ）。

    字段：open_time(ms, int64), open, high, low, close, volume(base),
          qvol(quote USD), trades, tbq(taker买quote), n(子5m根数)。
    丢弃最后一根未走完的小时（n<12）。
    """
    start_ms = start_ms or config.START_MS
    end_ms = end_ms or config.END_MS
    coll = _coll(symbol)
    pipeline = [
        {"$match": {"openTime": {"$gte": start_ms, "$lte": end_ms}}},
        {"$sort": {"openTime": 1}},
        {"$addFields": {"bh": {"$subtract": ["$openTime", {"$mod": ["$openTime", 3600000]}]}}},
        {"$group": {
            "_id": "$bh",
            "o": {"$first": "$open"},
            "h": {"$max": "$high"},
            "l": {"$min": "$low"},
            "c": {"$last": "$close"},
            "v": {"$sum": "$volume"},
            "qv": {"$sum": "$quoteVolume"},
            "tr": {"$sum": "$trades"},
            "tbq": {"$sum": "$takerBuyQuoteVolume"},
            "n": {"$sum": 1},
        }},
        {"$sort": {"_id": 1}},
    ]
    rows = list(coll.aggregate(pipeline, allowDiskUse=True))
    if not rows:
        return None
    # 过滤未走完的最后一根小时
    if rows[-1]["n"] < 12:
        rows = rows[:-1]
    arr = {k: np.array([r[k2] for r in rows], dtype=dt)
           for k, k2, dt in [("open_time", "_id", "int64"), ("o", "o", "float64"),
                             ("h", "h", "float64"), ("l", "l", "float64"),
                             ("c", "c", "float64"), ("v", "v", "float64"),
                             ("qv", "qv", "float64"), ("tr", "tr", "float64"),
                             ("tbq", "tbq", "float64"), ("n", "n", "int64")]}
    return arr


def load_daily_volume(symbol, start_ms=None, end_ms=None):
    """返回该 symbol 的日级聚合：date_ms, qvol, trades, tbq, n(5m根数)。"""
    start_ms = start_ms or config.START_MS
    end_ms = end_ms or config.END_MS
    coll = _coll(symbol)
    # 仅用 sum，无需 $sort（省去大排序，加速宇宙筛选）
    pipeline = [
        {"$match": {"openTime": {"$gte": start_ms, "$lte": end_ms}}},
        {"$addFields": {"bd": {"$subtract": ["$openTime", {"$mod": ["$openTime", 86400000]}]}}},
        {"$group": {
            "_id": "$bd",
            "qv": {"$sum": "$quoteVolume"},
            "tr": {"$sum": "$trades"},
            "tbq": {"$sum": "$takerBuyQuoteVolume"},
            "n": {"$sum": 1},
        }},
        {"$sort": {"_id": 1}},
    ]
    rows = list(coll.aggregate(pipeline, allowDiskUse=True))
    if not rows:
        return None
    return {
        "date_ms": np.array([r["_id"] for r in rows], dtype="int64"),
        "qv": np.array([r["qv"] for r in rows], dtype="float64"),
        "tr": np.array([r["tr"] for r in rows], dtype="float64"),
        "tbq": np.array([r["tbq"] for r in rows], dtype="float64"),
        "n": np.array([r["n"] for r in rows], dtype="int64"),
    }


def all_symbols():
    """Workbuddy_5Min_Db 中所有 <SYMBOL>.BINANCE 集合名 -> 纯 SYMBOL。"""
    db = client()[config.DB_5M]
    cols = db.list_collection_names()
    out = []
    for c in cols:
        if c.endswith(f".{config.EXCHANGE}"):
            out.append(c[: -len(f".{config.EXCHANGE}")])
    return out
