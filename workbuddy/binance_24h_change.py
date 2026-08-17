"""多币种 5m 整10分钟点 24h 涨跌幅计算 (结果只落 CSV, 不写 MongoDB 结果集)。

策略: 拉取前先查库(K线落库复用), 已有直接拿; 缺失则从起始 5m 时间拉一次(limit=1000)补齐落库。
口径: close[T] = openTime = T-5min 的 5m K close; change% = (close[T]-close[T-24h])/close[T-24h]*100
时间点: 起始(北京 00:00) -> 当前最近的整 10 分钟点。
输出:   workbuddy/data/change_{起}_{止}/{symbol_lower}.csv
"""
import os
import csv
import requests
import pymongo
from datetime import datetime, timezone, timedelta

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
EXCHANGE = "BINANCE"
DB_NAME = "Workbuddy_5Min_Db"
KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"

LOCAL_TZ = timezone(timedelta(hours=8))  # 北京时间 UTC+8

# ---------- 1. 时间点: 起始日 00:00 -> 当前最近整10分 (北京) ----------
now_local = datetime.now(LOCAL_TZ)
p_end = now_local - timedelta(minutes=now_local.minute % 10,
                              seconds=now_local.second,
                              microseconds=now_local.microsecond)
p_start = datetime(2026, 8, 9, 0, 0, 0, tzinfo=LOCAL_TZ)  # 起始: 2026-08-09 00:00 北京
OUT_DIR = os.path.join("workbuddy", "data", f"change_{p_start:%Y%m%d}_{p_end:%Y%m%d}")
os.makedirs(OUT_DIR, exist_ok=True)

points = []
t = p_start
while t <= p_end:
    points.append(t)
    t += timedelta(minutes=10)
print(f"时间点: {len(points)} 个 ({p_start:%Y-%m-%d %H:%M} -> {p_end:%H:%M} 北京), 共 {len(SYMBOLS)} 币")


def ms_to_dt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def local_str(dt_local):
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")


cli = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
db = cli[DB_NAME]


def get_k(kc, ot_local):
    return kc.find_one({"openTime": ms_to_dt(int(ot_local.timestamp() * 1000))})


def upsert_needed(kc, kl, needed_ms):
    n = 0
    for k in kl:
        ot_ms = k[0]
        if ot_ms not in needed_ms:
            continue
        d = {
            "openTime": ms_to_dt(ot_ms),
            "openTimeLocal": ms_to_dt(ot_ms).astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
            "volume": float(k[5]), "closeTime": ms_to_dt(k[6]),
            "closeTimeLocal": ms_to_dt(k[6]).astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "quoteVolume": float(k[7]), "trades": int(k[8]),
            "takerBuyBaseVolume": float(k[9]), "takerBuyQuoteVolume": float(k[10]),
        }
        kc.replace_one({"openTime": d["openTime"]}, d, upsert=True)
        n += 1
    return n


def fetch_all(sym, start_ms, end_ms):
    """分页拉取 [start_ms, end_ms] 全部 5m K (每页 limit=1000, cursor 滚动)"""
    out = []
    cursor = start_ms
    while cursor <= end_ms:
        params = {
            "symbol": sym, "interval": "5m",
            "startTime": cursor, "endTime": end_ms, "limit": 1000,
        }
        r = requests.get(KLINE_URL, params=params, timeout=20)
        r.raise_for_status()
        kl = r.json()
        if not kl:
            break
        out.extend(kl)
        nxt = kl[-1][0] + 5 * 60 * 1000
        if nxt <= cursor:
            break
        cursor = nxt
    return out


for sym in SYMBOLS:
    coll_name = f"{sym}.{EXCHANGE}"
    kc = db[coll_name]
    kc.create_index("openTime", unique=True)
    out_csv = os.path.join(OUT_DIR, f"{sym.lower()}.csv")

    # 所需 K (每币独立)
    needed_ots = set()
    for p in points:
        needed_ots.add(p - timedelta(minutes=5))
        needed_ots.add(p - timedelta(hours=24, minutes=5))
    start_needed = min(needed_ots)
    end_needed = max(needed_ots)
    needed_ms = {int(x.timestamp() * 1000) for x in needed_ots}

    missing = [ot for ot in sorted(needed_ots) if get_k(kc, ot) is None]
    have = len(needed_ots) - len(missing)
    print(f"\n[{sym}] 所需 {len(needed_ots)} 根 K, 库内已有 {have}, 缺失 {len(missing)}")
    if missing:
        kl = fetch_all(sym, int(start_needed.timestamp() * 1000),
                       int(end_needed.timestamp() * 1000))
        upserted = upsert_needed(kc, kl, needed_ms)
        print(f"  [{sym}] 拉取 {len(kl)} 根, 筛选落库 {upserted} 根")
        still = [ot for ot in sorted(needed_ots) if get_k(kc, ot) is None]
        if still:
            print(f"  [{sym}] 警告: 仍缺失 {len(still)} 根")

    # 计算 -> CSV
    rows = []
    ok = err = 0
    for p in points:
        ot1 = p - timedelta(minutes=5)
        ot2 = p - timedelta(hours=24, minutes=5)
        k1, k2 = get_k(kc, ot1), get_k(kc, ot2)
        if k1 is None or k2 is None:
            miss = [f"{x:%Y-%m-%d %H:%M}" for x in (ot1, ot2) if get_k(kc, x) is None]
            rows.append([local_str(p), None, None, None, "error", f"数据缺失: {miss}"])
            err += 1
            continue
        change = (k1["close"] - k2["close"]) / k2["close"] * 100
        rows.append([local_str(p), k1["close"], k2["close"], round(change, 6), "ok", ""])
        ok += 1

    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["pointTimeLocal", "closeT", "closeT24h", "changePct", "status", "errorDetail"])
        w.writerows(rows)
    print(f"  [{sym}] {ok} ok / {err} error -> {out_csv}")

print(f"\n全部完成, 输出目录: {OUT_DIR}")
