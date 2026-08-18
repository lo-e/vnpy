"""拉取资金费率(fundingRate)与未平仓量(OI)历史数据, 供入场点方案E/F使用.
输出: workbuddy/data/change_20251201_20260816/ef_{SYMBOL}.csv (funding_rate, oi 合并)
依赖: 系统代理 127.0.0.1:10809
"""
import os, json, time, csv, requests
from datetime import datetime, timezone, timedelta

LOCAL_TZ = timezone(timedelta(hours=8))
START_DT = datetime(2025, 11, 20, 0, 0, tzinfo=LOCAL_TZ)   # 提前 11 天(资金费率窗口/前情)
END_DT = datetime(2026, 8, 17, 0, 0, tzinfo=LOCAL_TZ)
START_MS = int(START_DT.timestamp() * 1000)
END_MS = int(END_DT.timestamp() * 1000)
OUT_DIR = os.path.join("workbuddy", "data", "change_20251201_20260816")
os.makedirs(OUT_DIR, exist_ok=True)
FR_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
OI_URL = "https://fapi.binance.com/futures/data/openInterestHist"
SYMS = json.load(open("workbuddy/data/symbols_usdt_perp.json", encoding="utf-8"))

def req(url, params, retry=3):
    for i in range(retry):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i == retry - 1:
                return None
            time.sleep(2)
    return None

def fetch_funding(sym):
    rows, cur = [], START_MS
    while cur <= END_MS:
        d = req(FR_URL, {"symbol": sym, "startTime": cur, "endTime": END_MS, "limit": 1000})
        if not d:
            break
        rows.extend(d)
        if len(d) < 1000:
            break
        cur = d[-1]["fundingTime"] + 1
        time.sleep(0.15)
    return rows

def fetch_oi(sym):
    rows, cur = [], START_MS
    while cur <= END_MS:
        d = req(OI_URL, {"symbol": sym, "period": "1h", "startTime": cur, "endTime": END_MS, "limit": 500})
        if not d:
            break
        rows.extend(d)
        if len(d) < 500:
            break
        cur = d[-1]["timestamp"] + 1
        time.sleep(0.15)
    return rows

def main():
    done = {}
    if os.path.exists(os.path.join(OUT_DIR, "ef_done.json")):
        done = json.load(open(os.path.join(OUT_DIR, "ef_done.json"), encoding="utf-8"))
    for i, sym in enumerate(SYMS):
        if done.get(sym):
            continue
        fr = fetch_funding(sym)
        oi = fetch_oi(sym)
        if fr is None or oi is None:
            print(f"[skip] {sym} 拉取失败", flush=True)
            continue
        fr_map = {r["fundingTime"]: float(r["fundingRate"]) for r in fr}
        oi_map = {r["timestamp"]: float(r["sumOpenInterest"]) for r in oi}
        with open(os.path.join(OUT_DIR, f"ef_{sym}.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ts_ms", "funding_rate", "oi"])
            for ts in sorted(set(fr_map) | set(oi_map)):
                w.writerow([ts, fr_map.get(ts, ""), oi_map.get(ts, "")])
        done[sym] = 1
        if (i + 1) % 20 == 0:
            json.dump(done, open(os.path.join(OUT_DIR, "ef_done.json"), "w", encoding="utf-8"))
            print(f"进度 {i + 1}/{len(SYMS)} ({sym})", flush=True)
        time.sleep(0.1)
    json.dump(done, open(os.path.join(OUT_DIR, "ef_done.json"), "w", encoding="utf-8"))
    print("EF 数据拉取完成", flush=True)

if __name__ == "__main__":
    main()
