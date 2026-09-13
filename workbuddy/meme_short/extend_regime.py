# -*- coding: utf-8 -*-
"""扩展 regime_btc.csv 覆盖到回测窗口终点(09-12)。
market_regime.py 只从 Mongo 取 BTC 日线, 而 Mongo 止于 08-28; 本脚本用 fapi(10809)
补 BTCUSDT 5m(08-29~09-12), 按北京时间 date 聚合每日最后 close, 与现有日线合并后
全量重算 r20/r60/r120/r200 + regime, 写回 regime_btc.csv。
口径与 Mongo 聚合一致(北京时间 date 最后一根5m close), 保证与引擎 _regime_ok 对齐。
"""
import os
import sys
import time
import csv
import datetime
import requests

os.environ["HTTPS_PROXY"] = "http://127.0.0.1:10809"
os.environ["HTTP_PROXY"] = "http://127.0.0.1:10809"
os.environ["NO_PROXY"] = "127.0.0.1,localhost"

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import market_regime as mr

SYM = "BTCUSDT"
# 2026-08-29 00:00 +08 ~ 2026-09-13 00:00 +08 (多拉一天, 防引擎用 T 日 regime)
START_MS = 1756406400000
END_MS = 1757692800000
TZ8 = datetime.timezone(datetime.timedelta(hours=8))
KL_URL = "https://fapi.binance.com/fapi/v1/klines"


def fetch_btc_5m_daily(start, end, max_retry=10):
    daily = {}
    cur = start
    while cur < end:
        kl = None
        for attempt in range(max_retry):
            try:
                r = requests.get(KL_URL, params={
                    "symbol": SYM, "interval": "5m",
                    "startTime": cur, "endTime": end, "limit": 1000}, timeout=30)
                r.raise_for_status()
                kl = r.json()
                break
            except Exception as e:
                print(f"  [retry {attempt+1}/{max_retry}] {type(e).__name__}: {str(e)[:80]}", flush=True)
                time.sleep(8)
        if kl is None:
            raise RuntimeError("BTC 5m 拉取多次失败, 代理可能抖动")
        if not kl:
            break
        for k in kl:
            ot = k[0]
            close = float(k[4])
            d = datetime.datetime.fromtimestamp(ot / 1000, tz=TZ8).strftime("%Y-%m-%d")
            if d not in daily or ot > daily[d][0]:
                daily[d] = (ot, close)
        nxt = kl[-1][0] + 300000
        if nxt <= cur:
            break
        cur = nxt
    return daily


def main():
    print("=== extend_regime: 补 BTC 日线到 09-12 ===", flush=True)
    # 现有 regime 日线 (date, close)
    existing = []
    with open(mr.OUT, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            existing.append((row["date"], float(row["close"])))
    print(f"现有 regime 日线: {len(existing)} 天, 覆盖 {existing[0][0]} ~ {existing[-1][0]}", flush=True)

    new_daily = fetch_btc_5m_daily(START_MS, END_MS)
    print(f"fapi 新增日线: {len(new_daily)} 天", flush=True)
    if not new_daily:
        print("未新增, 退出", flush=True)
        return

    em = {d: c for d, c in existing}
    added = 0
    for d, (ot, c) in new_daily.items():
        if d not in em:
            added += 1
        em[d] = c  # 新覆盖旧(同一天取 fapi 口径, 一致)
    merged = sorted(em.items())
    print(f"合并后: {len(merged)} 天 (新增 {added}), 末 {merged[-1][0]}", flush=True)

    rows = mr.build_regime(merged)
    mr.write_csv(rows, mr.OUT)
    from collections import Counter
    c = Counter(r[6] for r in rows)
    print("regime 分布:", dict(c), flush=True)
    print("写出:", mr.OUT, flush=True)
    print("最近 14 天:", flush=True)
    for r in rows[-14:]:
        print("  %s close=%.0f r200=%s %s" % (
            r[0], r[1], ("%.3f" % r[5] if r[5] != "" else "N/A"), r[6]), flush=True)


if __name__ == "__main__":
    main()
