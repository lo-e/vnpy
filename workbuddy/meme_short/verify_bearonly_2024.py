# -*- coding: utf-8 -*-
"""
verify_bearonly_2024.py — 2024 牛市段「只做 BEAR」隔离验证
========================================================================
等价逻辑: 回测 filter 在 permit 创建前生效 -> 创建日 regime == BEAR 才开仓。
这里直接对 _bt_2024window_v2.csv 按 permitOpenTime 的日期查 regime,
只保留 BEAR 段, 重算累计盈亏 / maxDD / ddRatio, 写出结果 CSV。

关键: 原 CSV 的 cumPnl/maxDD/ddRatio 是全窗口(FULL)口径, 混入了 BULL/SIDEWAYS 的贡献,
不能直接用。本脚本对 BEAR-only 子集重新累计, 保证曲线与指标自洽。

输出:
    data/change_20240101_20251201/_bt_2024window_bearonly.csv   (BEAR-only 结果)
    (曲线 HTML 由 plot_final_curve.py 对上面的 CSV 生成)
"""
import csv
import os
import sys
from collections import defaultdict, Counter

sys.path.insert(0, os.path.dirname(__file__))
from market_regime import get_regime_map

BASE = os.path.join(os.path.dirname(__file__), "data", "change_20240101_20251201")
SRC = os.path.join(BASE, "_bt_2024window_v2.csv")
OUT = os.path.join(BASE, "_bt_2024window_bearonly.csv")

# 原 CSV 列顺序 (保持, 便于 diff / 复用曲线脚本)
COLS = ["symbol", "openTime", "openPrice", "closeTime", "closePrice",
        "pnlPct", "reason", "signalType", "weight", "cumPnl",
        "maxDD", "maxAdvPct", "permitOpenTime", "ddRatio"]


def fnum(r, k):
    try:
        return float(r[k])
    except Exception:
        return 0.0


def day_of(r):
    return r["permitOpenTime"].split(" ")[0]


def metrics(rows):
    if not rows:
        return {"n": 0, "wpnl": 0.0, "wr": 0.0, "maxDD": 0.0, "ddRatio": 0.0, "wtot": 0.0}
    srt = sorted(rows, key=lambda r: r["openTime"])
    wpnl = sum(fnum(r, "pnlPct") * fnum(r, "weight") for r in srt)
    win = sum(1 for r in srt if fnum(r, "pnlPct") > 0)
    n = len(srt)
    wtot = sum(fnum(r, "weight") for r in srt)
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in srt:
        cum += fnum(r, "pnlPct") * fnum(r, "weight")
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
    ddRatio = (max_dd / wpnl) if wpnl > 0 else 0.0
    return {"n": n, "wpnl": wpnl, "wr": win / n * 100,
            "maxDD": max_dd, "ddRatio": ddRatio, "wtot": wtot}


def main():
    regime = get_regime_map()
    with open(SRC, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    # 按 permit 创建日归类 (用于展示分布)
    by_reg = defaultdict(list)
    missing = 0
    for r in rows:
        d = day_of(r)
        reg = regime.get(d, "SIDEWAYS")   # 越界/缺失 -> 默认 SIDEWAYS(不放行)
        if d not in regime:
            missing += 1
        by_reg[reg].append(r)

    full = metrics(rows)
    bear_rows = by_reg.get("BEAR", [])
    bear = metrics(bear_rows)

    # 重新累计 BEAR-only 的 cumPnl / maxDD / ddRatio, 写回结果 CSV
    srt = sorted(bear_rows, key=lambda r: r["openTime"])
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    # 与 backtest_short_top1.py line 983-994 完全一致: 逐行写 running 值
    for r in srt:
        cum += fnum(r, "pnlPct") * fnum(r, "weight")
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
        r["cumPnl"] = "%.4f" % cum
        r["maxDD"] = "%.4f" % max_dd
        r["ddRatio"] = "%.4f" % ((max_dd / cum) if cum > 0 else 0.0)

    with open(OUT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        for r in srt:
            w.writerow({c: r.get(c, "") for c in COLS})

    def line(tag, m, note=""):
        print("  %-14s n=%-5d wpnl=%+9.2fpp  win=%.1f%%  maxDD=%.2f  ddRatio=%.4f  wtot=%.0f%s"
              % (tag, m["n"], m["wpnl"], m["wr"], m["maxDD"], m["ddRatio"], m["wtot"],
                 "  " + note))

    print("=" * 80)
    print("2024 牛市段 (_bt_2024window_v2.csv) — 只做 BEAR 隔离验证")
    print("=" * 80)
    dc = Counter(regime.get(day_of(r), "SIDEWAYS") for r in rows)
    print("\n[0] 单笔 regime 分布: BULL=%d BEAR=%d SIDEWAYS=%d  (越界缺失=%d)"
          % (dc.get("BULL", 0), dc.get("BEAR", 0), dc.get("SIDEWAYS", 0), missing))
    print("\n[1] 全样本(不过滤, 参照):")
    line("FULL", full)
    print("\n[2] 只做 BEAR (剔除 BULL + SIDEWAYS):")
    line("BEAR_ONLY", bear, "<- 本任务结果")
    print("\n[3] 增量 (BEAR_ONLY - FULL):")
    print("  笔数:   %d -> %d (%+d, %+.1f%%)" %
          (full["n"], bear["n"], bear["n"] - full["n"],
           (bear["n"] - full["n"]) / full["n"] * 100))
    print("  加权pnl: %+.2fpp -> %+.2fpp (%+.2fpp)" %
          (full["wpnl"], bear["wpnl"], bear["wpnl"] - full["wpnl"]))
    print("  胜率:    %.1f%% -> %.1f%% (%+.1fpp)" %
          (full["wr"], bear["wr"], bear["wr"] - full["wr"]))
    print("  maxDD:   %.2f -> %.2f (%+.2f)" %
          (full["maxDD"], bear["maxDD"], bear["maxDD"] - full["maxDD"]))
    print("  ddRatio: %.4f -> %.4f (%+.4f, 越小越好)" %
          (full["ddRatio"], bear["ddRatio"], bear["ddRatio"] - full["ddRatio"]))
    print("\n[4] 输出文件:")
    print("  结果CSV : %s (%d 笔)" % (OUT, bear["n"]))
    print("  曲线HTML: 用 plot_final_curve.py 对上面 CSV 生成 (CURVE_SRC=%s)" % OUT)


if __name__ == "__main__":
    main()
