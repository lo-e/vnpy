"""回测数据补齐: 对 96 个上榜币按 per-symbol K 区间 [首上榜-5min, 末上榜+24h-5min] 全量拉取落库。

用途: 修正回测所需连续 5m K 的缺失(MV5ryp 时代 upsert 未落库问题)。
"""
import os
import csv
import time
import requests
import pymongo
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import UpdateOne

LOCAL = timezone(timedelta(hours=8))
KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
SIGNAL_FILE = "data/change_20260701_20260811/000000_top1_rise.csv"
EXCHANGE = "BINANCE"
STEP5 = 5 * 60 * 1000
DAY_MS = 24 * 3600 * 1000
LOOKBACK_MS = 30 * DAY_MS   # 开仓确认规则: 开仓前看过去30天最高价 → 每币K区间头部需前移30天
MAX_WORKERS = int(os.environ.get("BW", "3"))


def ms_to_naive(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def ms_to_local(ms):
    """统一规范: 时间一律本地时区(北京) naive datetime 入库, 不存 timeLocal"""
    return datetime.fromtimestamp(ms / 1000, tz=LOCAL)


def local_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=LOCAL).strftime("%Y-%m-%d %H:%M:%S")


# 信号 -> 每币首/末上榜
signals = []
with open(SIGNAL_FILE, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        t = datetime.strptime(r["pointTimeLocal"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL)
        signals.append((int(t.timestamp() * 1000), r["topSymbol"]))
first_sig, last_sig = {}, {}
for t_ms, sym in signals:
    if sym not in first_sig:
        first_sig[sym] = t_ms
    last_sig[sym] = t_ms
syms = sorted(set(s for _, s in signals))
print(f"币种 {len(syms)} 个", flush=True)

cli = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
db = cli["Workbuddy_5Min_Db"]


def fetch_all(sym, start_ms, end_ms):
    out = []
    cursor = start_ms
    while cursor <= end_ms:
        kl = None
        for attempt in range(3):
            try:
                params = {"symbol": sym, "interval": "5m",
                          "startTime": cursor, "endTime": end_ms, "limit": 1000}
                r = requests.get(KLINE_URL, params=params, timeout=30)
                r.raise_for_status()
                kl = r.json()
                break
            except Exception as e:
                if attempt == 2:
                    raise
                time.sleep(3)
        if not kl:
            break
        out.extend(kl)
        nxt = kl[-1][0] + STEP5
        if nxt <= cursor:
            break
        cursor = nxt
    return out


def upsert(kc, kl):
    docs = []
    seen = set()
    for k in kl:
        ot = k[0]
        if ot in seen:
            continue
        seen.add(ot)
        docs.append({
            "openTime": int(ot),
            "openTimeLocal": local_str(ot),
            "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
            "volume": float(k[5]), "closeTime": int(k[6]),
            "closeTimeLocal": local_str(k[6]),
            "quoteVolume": float(k[7]), "trades": int(k[8]),
            "takerBuyBaseVolume": float(k[9]), "takerBuyQuoteVolume": float(k[10]),
        })
    if docs:
        kc.bulk_write([UpdateOne({"openTime": d["openTime"]}, {"$set": d}, upsert=True)
                       for d in docs], ordered=False)
    return len(docs)


def work(sym):
    start_ms = first_sig[sym] - LOOKBACK_MS - STEP5   # 首上榜-15天-5min(开仓确认需看15天high)
    end_ms = last_sig[sym] + DAY_MS - STEP5
    kc = db[f"{sym.upper()}.{EXCHANGE}"]  # collection 命名规范: 大写 SYMBOL.BINANCE
    kc.create_index("openTime", unique=True)
    before = kc.count_documents({})
    kl = fetch_all(sym, start_ms, end_ms)
    upsert(kc, kl)
    after = kc.count_documents({})
    last = kc.find_one(sort=[("openTime", -1)])
    last_bj = local_str(last["openTime"]) if last else None
    return sym, len(kl), after - before, str(last_bj)


results = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    futs = {ex.submit(work, s): s for s in syms}
    for fut in as_completed(futs):
        try:
            results.append(fut.result())
        except Exception as e:
            results.append((futs[fut], 0, -1, f"ERR {e}"))

results.sort()
bad = [r for r in results if r[2] < 0]
print(f"\n完成 {len(results)} 币, 失败 {len(bad)}")
for r in results[:3] + results[-3:]:
    print(" ", r)
if bad:
    print("失败:", bad)
