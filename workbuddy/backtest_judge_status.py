"""按用户设想: 全量模拟成交订单(读 nosim CSV) → 按时间顺序判定 sim/live/dropped。
判定: 订单 T 看过去 30 天(窗口 [T-30d, T)) 同币【已平仓】(closeTime < T) 订单平均盈亏:
  无已平仓 → sim(放行) | 平均>0 → live(放行) | 平均≤0 → dropped(剔除)
cumPnl 只累计 live 订单。
输出: workbuddy/data/backtest_short_top1_20260601_20260811_judged.csv
"""
import csv
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict

LOCAL = timezone(timedelta(hours=8))
DAY = 24 * 3600 * 1000
LOOKBACK = 30 * DAY
SRC = os.environ.get("JUDGE_SRC", "workbuddy/data/change_20260101_20260812/_backtest_top1_short_all.csv")
OUT = os.environ.get("JUDGE_OUT", "workbuddy/data/change_20260101_20260812/_backtest_top1_short_judged.csv")


def to_ms(s):
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL).timestamp() * 1000)


rows = list(csv.DictReader(open(SRC, encoding="utf-8-sig")))
rows.sort(key=lambda r: r["openTime"])

# 每币已平仓订单历史: 按平仓时间维护, 供后续订单判定
hist = defaultdict(list)   # sym -> [(close_ms, pnl), ...] 按 close_ms 升序

out_rows = []
cum_live = 0.0
n_sim = n_live = 0
cum_all = 0.0
for r in rows:
    open_ms = to_ms(r["openTime"])
    close_ms = to_ms(r["closeTime"])
    pnl = float(r["pnlPct"])
    weight = float(r.get("weight", 1.0) or 1.0)
    # 判定: 上次(最近一笔已平仓)盈亏 > 0 → live; 无历史或上次亏损 → sim
    prev = hist[r["symbol"]][-1] if hist[r["symbol"]] else None   # hist 按平仓时间升序追加
    if prev and prev[1] > 0:
        status = "live"
        n_live += 1
        cum_live += pnl * weight
    else:
        status = "sim"
        n_sim += 1
    cum_all += pnl * weight               # cumPnl: 全部订单加权累计(盈亏×权重)
    out_rows.append([r["symbol"], r["openTime"], r["openPrice"], r["closeTime"], r["closePrice"],
                     r["pnlPct"], r["reason"], status, weight, round(cum_all, 4), round(cum_live, 4)])
    # 本单平仓后进入历史(供后续订单判定) —— sim/live 都保留(验证价值)
    hist[r["symbol"]].append((close_ms, pnl))

with open(OUT, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["symbol", "openTime", "openPrice", "closeTime", "closePrice",
                "pnlPct", "reason", "status", "weight", "cumPnl", "liveCumPnl"])
    for o in out_rows:
        w.writerow(o)

print(f"总订单 {len(out_rows)}: sim={n_sim} live={n_live}")
print(f"全部订单累计(cumPnl): {cum_all:+.2f}%")
print(f"live 累计(liveCumPnl): {cum_live:+.2f}%")
l = [float(r[5]) for r in out_rows if r[7] == "live"]
if l:
    print(f"live: {len(l)}笔 平均 {sum(l)/len(l):+.2f}% 胜率 {sum(1 for x in l if x>0)/len(l)*100:.1f}%")
print(f"输出: {OUT}")
