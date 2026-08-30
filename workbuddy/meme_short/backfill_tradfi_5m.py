#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""补 TradFi 永续(美股/商品代币) 5m K 线进 MongoDB。
- 源: fapi 合约端点 (与 BTCUSDT.BINANCE 同源)
- 起点: 各币 exchangeInfo.onboardDate (上线日)
- 终点: 当前时间
- 幂等 upsert (openTime 唯一索引)
- 代理 127.0.0.1:10809 自愈
- 拉完即做连续性核查: 找缺口(>5min), 区分周末停牌 vs 异常缺失
"""
import os, time, requests, pymongo
from datetime import datetime, timezone, timedelta

LOCAL_TZ = timezone(timedelta(hours=8))


def local_str(ms):
    # 北京时间字符串, 与涨跌幅项目 ensure_klines 落库格式一致
    return datetime.fromtimestamp(ms / 1000, tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


PROX = "http://127.0.0.1:10809"
os.environ["HTTP_PROXY"] = PROX
os.environ["HTTPS_PROXY"] = PROX

KURL = "https://fapi.binance.com/fapi/v1/klines"
EXURL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
STEP5 = 5 * 60 * 1000
URI = "mongodb://127.0.0.1:27017/"
DB = "Workbuddy_5Min_Db"
SYMS = ["SNDKUSDT", "TSLAUSDT", "NVDAUSDT", "AAPLUSDT", "SPYUSDT", "XAUUSDT"]


def new_sess():
    s = requests.Session()
    s.proxies = {"http": PROX, "https": PROX}
    return s


def _onboard_cache():
    r = requests.get(EXURL, timeout=30)
    r.raise_for_status()
    m = {}
    for x in r.json()["symbols"]:
        m[x["symbol"]] = x.get("onboardDate")
    return m


def fetch(sym, start_ms, end_ms, ob_cache):
    s = new_sess()
    out = []
    page = 0
    while start_ms < end_ms:
        k = None
        for attempt in range(4):
            try:
                r = s.get(KURL, params={"symbol": sym, "interval": "5m",
                                         "startTime": start_ms, "endTime": end_ms,
                                         "limit": 1000}, timeout=30)
                if r.status_code == 200:
                    k = r.json()
                    break
                print(f"  [{sym}] HTTP {r.status_code}, retry {attempt}")
                time.sleep(2)
            except Exception as e:
                print(f"  [{sym}] ERR {repr(e)[:80]}, retry {attempt}")
                time.sleep(3)
        if k is None:
            print(f"  [{sym}] 4次失败, 终止本币")
            return out
        if not k:
            break
        out.extend(k)
        last = k[-1][0]
        if last >= end_ms - STEP5:
            break
        start_ms = last + STEP5
        page += 1
        if page % 20 == 0:
            print(f"  [{sym}] 已拉 {len(out)} 根, 到 {datetime.utcfromtimestamp(start_ms/1000)}")
    return out


def upsert(sym, klines):
    from pymongo import UpdateOne
    c = pymongo.MongoClient(URI)
    col = c[DB][f"{sym}.BINANCE"]
    col.create_index([("openTime", 1)], unique=True)
    docs = []
    seen = set()
    for k in klines:
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
            "quoteVolume": float(k[7]), "trades": int(float(k[8])),
            "takerBuyBaseVolume": float(k[9]), "takerBuyQuoteVolume": float(k[10]),
        })
    if docs:
        col.bulk_write([UpdateOne({"openTime": d["openTime"]}, {"$set": d}, upsert=True)
                        for d in docs], ordered=False)
    c.close()
    return len(docs)


def check_continuity(sym):
    c = pymongo.MongoClient(URI)
    col = c[DB][f"{sym}.BINANCE"]
    ts = [d["openTime"] for d in col.find({}, {"openTime": 1}).sort("openTime", 1)]
    c.close()
    if len(ts) < 2:
        return len(ts), 0, 0, 0
    gaps = []
    maxgap = 0
    for i in range(1, len(ts)):
        d = ts[i] - ts[i - 1]
        if d > STEP5:
            gaps.append((ts[i - 1], ts[i], d))
            maxgap = max(maxgap, d)
    wknd = 0
    for g in gaps:
        dt = datetime.utcfromtimestamp(g[0] / 1000)
        if dt.weekday() >= 5:  # 5=Sat 6=Sun
            wknd += 1
    return len(ts), len(gaps), maxgap, wknd


def main():
    end_ms = int(time.time() * 1000)
    ob = _onboard_cache()
    print(f"当前时间(UTC): {datetime.utcfromtimestamp(end_ms/1000)}")
    for sym in SYMS:
        start = ob.get(sym)
        if not start:
            start = end_ms - 180 * 24 * 3600 * 1000
            print(f"=== {sym} (onboard未知, 兜底半年前) ===")
        else:
            print(f"=== {sym} (onboard={datetime.utcfromtimestamp(start/1000)}) ===")
        kl = fetch(sym, start, end_ms, ob)
        if not kl:
            print(f"  !! {sym} 无数据, 跳过")
            continue
        n = upsert(sym, kl)
        print(f"  upsert {n} 根完成")
        tot, ngap, mg, wk = check_continuity(sym)
        mg_h = mg / 3600 / 1000
        print(f"  连续性: 总{tot}根, 缺口{ngap}处, 最大缺口{mg_h:.1f}h, 其中周末缺口{wk}处")
    print("全部完成")


if __name__ == "__main__":
    main()
