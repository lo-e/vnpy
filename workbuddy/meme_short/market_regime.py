# -*- coding: utf-8 -*-
"""
market_regime.py — BTC 全市场状态体温计
==================================================================
从 MongoDB BTCUSDT.BINANCE 取日级收盘, 算多周期复合年化斜率,
分类 BULL / BEAR / SIDEWAYS, 输出日级 regime 序列 CSV。

用法:
    python market_regime.py
    # 或 import: from market_regime import get_regime_map

输出: data/regime_btc.csv  (date, close, r20_ann, r60_ann, r120_ann, r200_ann, regime)

阈值(可调, 命令行/常量):
    BULL:    r200_ann > +0.25 且 r60 > 0 且 r20 >= 0
    BEAR:    r200_ann < -0.15 或 (r60 < 0 且 r120 < 0)
    SIDEWAYS: 其余(符号打架/斜率近零/低波动)
"""
import os
import csv
import pymongo
from datetime import datetime, timezone, timedelta

BULL_R200 = float(os.environ.get("BULL_R200", "0.25"))   # r200 年化 > 此值且中短同向向上
BEAR_R200 = float(os.environ.get("BEAR_R200", "-0.15"))  # r200 年化 < 此值直接判熊
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")
SYM = os.environ.get("REGIME_SYM", "BTCUSDT")
OUT = os.environ.get(
    "REGIME_OUT",
    os.path.join(os.path.dirname(__file__), "data", "regime_btc.csv"),
)
TZ = timezone(timedelta(hours=8))


def fetch_daily_close(sym=SYM, uri=MONGO_URI):
    """日级收盘: 按北京时间日期取当天最后一根 5m 的 close。
    同时取当天 max(openTime), 供 trim_incomplete 识别'未走完的日'(防半截close污染regime)。
    返回 [(date_str, close, max_open_ms), ...] 按日期升序。
    """
    cli = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
    col = cli["Workbuddy_5Min_Db"][sym + ".BINANCE"]
    pipeline = [
        {"$group": {
            "_id": {"$dateToString": {
                "date": {"$toDate": "$openTime"},
                "format": "%Y-%m-%d", "timezone": "Asia/Shanghai"}},
            "close": {"$last": "$close"},
            "maxOpen": {"$max": "$openTime"},
        }},
        {"$sort": {"_id": 1}},
    ]
    docs = list(col.aggregate(pipeline))
    return [(d["_id"], float(d["close"]), int(d["maxOpen"])) for d in docs]


def _expected_last_slot_ms(date_str):
    """北京时间 date D 的最后一笔 5m K 的 openTime (D 23:55:00 +08)"""
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=TZ)
    d = d.replace(hour=23, minute=55, second=0, microsecond=0)
    return int(d.timestamp() * 1000)


def trim_incomplete(daily):
    """剔除末尾'未走完的日': 该日 max(openTime) < 当日 23:55 时戳 → 日K未收盘,
    其 $last close 是盘中价, 算出的 r20/r60/r120/r200 失真。
    只从尾部剔除(历史日均已完整, 不会误删)。返回 [(date, close, max_open), ...]"""
    out = list(daily)
    dropped = []
    while out:
        date, _close, max_open = out[-1]
        if max_open < _expected_last_slot_ms(date):
            dropped.append(date)
            out.pop()
        else:
            break
    return out, dropped


def ann_slope(closes, i, n):
    """第 i 根(0基)往前 n 天的复合年化收益率; 数据不足返回 None"""
    j = i - n
    if j < 0:
        return None
    p = closes[j]
    if p <= 0:
        return None
    ret = closes[i] / p - 1
    return (1 + ret) ** (365.0 / n) - 1


def classify(r20, r60, r120, r200):
    if r200 is None or r60 is None or r120 is None or r20 is None:
        return "SIDEWAYS"
    # 熊: 长周期明显下行, 或中长双负
    if r200 < BEAR_R200 or (r60 < 0 and r120 < 0):
        return "BEAR"
    # 牛: 长周期强上行 + 中短不向下
    if r200 > BULL_R200 and r60 > 0 and r20 >= 0:
        return "BULL"
    return "SIDEWAYS"


def build_regime(daily):
    """daily=[(date_str, close),...] -> [(date_str, close, r20,r60,r120,r200, regime),...]"""
    dates = [d[0] for d in daily]
    closes = [d[1] for d in daily]
    N = len(closes)
    out = []
    for i in range(N):
        r20 = ann_slope(closes, i, 20)
        r60 = ann_slope(closes, i, 60)
        r120 = ann_slope(closes, i, 120)
        r200 = ann_slope(closes, i, 200)
        reg = classify(r20, r60, r120, r200)
        out.append((dates[i], closes[i],
                    r20 if r20 is not None else "",
                    r60 if r60 is not None else "",
                    r120 if r120 is not None else "",
                    r200 if r200 is not None else "",
                    reg))
    return out


def get_regime_map(csv_path=OUT):
    """返回 {date_str: regime} 供其他脚本 import 复用"""
    m = {}
    if not os.path.exists(csv_path):
        # 自动生成
        daily = fetch_daily_close()
        daily, dropped = trim_incomplete(daily)
        if dropped:
            print("[regime] 剔除未走完的日(防半截close): %s" % dropped, flush=True)
        rows = build_regime(daily)
        write_csv(rows, csv_path)
    with open(csv_path, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            m[r["date"]] = r["regime"]
    return m


def write_csv(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "close", "r20_ann", "r60_ann",
                    "r120_ann", "r200_ann", "regime"])
        for r in rows:
            w.writerow(r)
    return path


def main():
    print("拉取 %s 日级收盘..." % SYM, flush=True)
    daily = fetch_daily_close()
    daily, dropped = trim_incomplete(daily)
    if dropped:
        print("[regime] 剔除未走完的日(防半截close污染): %s" % dropped, flush=True)
    print("  日K天数: %d ( %s ~ %s )" % (len(daily), daily[0][0], daily[-1][0]), flush=True)
    rows = build_regime(daily)
    path = write_csv(rows, OUT)
    # 统计分布
    from collections import Counter
    c = Counter(r[6] for r in rows)
    print("regime 分布:", dict(c), flush=True)
    print("写出: %s" % path, flush=True)
    # 打印最近 10 天
    print("最近 10 天:", flush=True)
    for r in rows[-10:]:
        print("  %s close=%.0f r200=%s %s" % (r[0], r[1],
              ("%.3f" % r[5] if r[5] != "" else "N/A"), r[6]), flush=True)


if __name__ == "__main__":
    main()
