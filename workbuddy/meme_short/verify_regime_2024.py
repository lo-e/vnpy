# -*- coding: utf-8 -*-
"""
verify_regime_2024.py — 验证市场状态过滤器对 2024 牛市段的增量价值
========================================================================
等价逻辑: 回测里 filter 在 permit 创建前生效 -> 创建日=BULL 则整段 permit 不产生单。
这里直接对 _bt_2024window_v2.csv 按 permitOpenTime 的日期查 regime, 剔除 BULL 段,
重算指标对比。

口径(与 backtest_short_top1.py 一致):
    加权pnl  = Σ(pnlPct * weight)
    胜率     = 盈利笔数 / 总笔数
    maxDD    = 按 openTime 排序后 cumPnl 序列的最大回撤
    ddRatio  = maxDD / 加权pnl (cum<=0 记 0, 越小越好)
"""
import csv
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from market_regime import get_regime_map

SRC = os.path.join(os.path.dirname(__file__), "data", "change_20240101_20251201",
                   "_bt_2024window_v2.csv")


def fnum(r, k):
    try:
        return float(r[k])
    except Exception:
        return 0.0


def metrics(rows):
    """rows: list of dict; 返回指标 dict"""
    if not rows:
        return {"n": 0, "wpnl": 0.0, "wr": 0.0, "maxDD": 0.0, "ddRatio": 0.0}
    srt = sorted(rows, key=lambda r: r["openTime"])
    wpnl = sum(fnum(r, "pnlPct") * fnum(r, "weight") for r in srt)
    win = sum(1 for r in srt if fnum(r, "pnlPct") > 0)
    n = len(srt)
    # 重算 cum 序列 + maxDD
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
    return {"n": n, "wpnl": wpnl, "wr": win / n * 100, "maxDD": max_dd, "ddRatio": ddRatio}


def main():
    regime = get_regime_map()
    with open(SRC, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    # 按 permit 创建日归类
    by_reg = defaultdict(list)
    for r in rows:
        d = r["permitOpenTime"].split(" ")[0]
        reg = regime.get(d, "SIDEWAYS")  # 找不到(越界)默认放行
        by_reg[reg].append(r)

    full = metrics(rows)
    bull = metrics(by_reg.get("BULL", []))
    bear = metrics(by_reg.get("BEAR", []))
    side = metrics(by_reg.get("SIDEWAYS", []))

    # filter 后: 剔除 BULL
    filtered = [r for r in rows if regime.get(r["permitOpenTime"].split(" ")[0], "SIDEWAYS") != "BULL"]
    filt = metrics(filtered)

    def line(tag, m, note=""):
        print("  %-16s n=%-5d wpnl=%+9.2fpp  win=%.1f%%  maxDD=%.2f  ddRatio=%.4f%s"
              % (tag, m["n"], m["wpnl"], m["wr"], m["maxDD"], m["ddRatio"], "  " + note))

    print("=" * 78)
    print("2024 牛市段 (_bt_2024window_v2.csv) 市场状态过滤验证")
    print("=" * 78)
    print("\n[1] 全样本(不过滤):")
    line("FULL", full)
    print("\n[2] 按 permit 创建日 regime 拆分:")
    line("  BULL段", bull, "<- 牛市做空")
    line("  BEAR段", bear, "<- 策略甜区")
    line("  SIDEWAYS段", side, "<- 震荡")
    print("\n[3] 应用 filter(剔除 BULL 段):")
    line("FILTERED", filt)
    print("\n[4] 增量 (filter - full):")
    print("  笔数变化: %d -> %d (%+d, -%.1f%%)" %
          (full["n"], filt["n"], filt["n"] - full["n"],
           (filt["n"] - full["n"]) / full["n"] * 100))
    print("  加权pnl: %+.2fpp -> %+.2fpp (%+.2fpp)" %
          (full["wpnl"], filt["wpnl"], filt["wpnl"] - full["wpnl"]))
    print("  胜率:    %.1f%% -> %.1f%% (%+.1fpp)" %
          (full["wr"], filt["wr"], filt["wr"] - full["wr"]))
    print("  maxDD:   %.2f -> %.2f (%+.2f)" %
          (full["maxDD"], filt["maxDD"], filt["maxDD"] - full["maxDD"]))
    print("  ddRatio: %.4f -> %.4f (%+.4f, 越小越好)" %
          (full["ddRatio"], filt["ddRatio"], filt["ddRatio"] - full["ddRatio"]))
    print("\n[5] regime 覆盖天数(2024段窗口内):")
    from collections import Counter
    dc = Counter()
    for r in rows:
        dc[regime.get(r["permitOpenTime"].split(" ")[0], "SIDEWAYS")] += 1
    print("  单笔分布: BULL=%d BEAR=%d SIDEWAYS=%d" %
          (dc.get("BULL", 0), dc.get("BEAR", 0), dc.get("SIDEWAYS", 0)))

    # [6] 反向假设: 剔除 SIDEWAYS 段(保留 BULL+BEAR)
    no_side = [r for r in rows
               if regime.get(r["permitOpenTime"].split(" ")[0], "SIDEWAYS") != "SIDEWAYS"]
    nos = metrics(no_side)
    print("\n[6] 反向假设 — 剔除 SIDEWAYS 段(保留 BULL+BEAR):")
    line("NO_SIDEWAYS", nos)
    print("  增量(no_side - full):")
    print("    笔数:   %d -> %d (%+d, %+.1f%%)" %
          (full["n"], nos["n"], nos["n"] - full["n"],
           (nos["n"] - full["n"]) / full["n"] * 100))
    print("    加权pnl: %+.2fpp -> %+.2fpp (%+.2fpp)" %
          (full["wpnl"], nos["wpnl"], nos["wpnl"] - full["wpnl"]))
    print("    胜率:    %.1f%% -> %.1f%% (%+.1fpp)" %
          (full["wr"], nos["wr"], nos["wr"] - full["wr"]))
    print("    maxDD:   %.2f -> %.2f (%+.2f)" %
          (full["maxDD"], nos["maxDD"], nos["maxDD"] - full["maxDD"]))
    print("    ddRatio: %.4f -> %.4f (%+.4f, 越小越好)" %
          (full["ddRatio"], nos["ddRatio"], nos["ddRatio"] - full["ddRatio"]))


if __name__ == "__main__":
    main()
