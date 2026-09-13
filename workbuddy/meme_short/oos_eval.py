# -*- coding: utf-8 -*-
"""
oos_eval.py — meme_short 样本外增量评估
========================================
拿扩展窗口的定稿回测 CSV, 按时间切一刀(样本内 / 样本外),
把样本外那段的加权收益放到样本内"滚动同长度窗口"的分布里, 看它排在第几百分位。
不靠感觉判 EDGE HOLD / DEGRADED / BROKEN。

用法:
    python oos_eval.py <bt_csv> [cutoff_yyyy-mm-dd]

默认 cutoff = 2026-08-29 (基线窗口 2025-12-01~2026-08-28 的次日)。
日志命名约定: 输出文件带 "_" 前缀。
"""
import csv
import os
import sys
from datetime import datetime, timedelta

WD = os.path.dirname(os.path.abspath(__file__))
FMT = "%Y-%m-%d %H:%M:%S"


def load(path):
    rows = list(csv.DictReader(open(path, encoding="utf-8-sig")))
    out = []
    for r in rows:
        out.append({
            "sym": r["symbol"],
            "t": datetime.strptime(r["openTime"], FMT),
            "sig": datetime.strptime(r["permitOpenTime"], FMT),
            "ct": datetime.strptime(r["closeTime"], FMT),
            "pnl": float(r["pnlPct"]),
            "w": float(r["weight"]),
            "reason": r["reason"],
        })
    out.sort(key=lambda x: x["t"])
    return out


def contrib(tr):
    """加权贡献 = weight × pnlPct (与引擎 cumPnl 逐行增量一致)"""
    return tr["w"] * tr["pnl"]


def seg_stats(trs):
    if not trs:
        return {"n": 0, "sum": 0.0, "win": 0.0, "avg": 0.0,
                "avg_win": 0.0, "avg_los": 0.0}
    s = sum(contrib(t) for t in trs)
    wins = [t for t in trs if t["pnl"] > 0]
    loss = [t for t in trs if t["pnl"] <= 0]
    return {
        "n": len(trs),
        "sum": s,
        "win": 100.0 * len(wins) / len(trs),
        "avg": sum(t["pnl"] for t in trs) / len(trs),
        "avg_win": (sum(t["pnl"] for t in wins) / len(wins)) if wins else 0.0,
        "avg_los": (sum(t["pnl"] for t in loss) / len(loss)) if loss else 0.0,
    }


def rolling_sums(trs, days):
    """样本内所有'起点滑动、长度=days'的连续窗口加权收益分布"""
    if not trs:
        return []
    step = timedelta(minutes=10)  # 信号粒度
    res = []
    t0, t1 = trs[0]["t"], trs[-1]["t"]
    cur = t0
    # 用双指针滚动, 避免 O(n^2)
    j = 0
    k = 0
    n = len(trs)
    while cur + timedelta(days=days) <= t1:
        end = cur + timedelta(days=days)
        while j < n and trs[j]["t"] < cur:
            j += 1
        if k < j:
            k = j
        while k < n and trs[k]["t"] < end:
            k += 1
        res.append(sum(contrib(trs[m]) for m in range(j, k)))
        cur += step
    return res


def pct_rank(dist, v):
    if not dist:
        return None
    below = sum(1 for x in dist if x < v)
    return 100.0 * below / len(dist)


def main():
    bt = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        WD, "data", "change_20251201_20260912", "_bt_mp9_final.csv")
    cutoff_s = sys.argv[2] if len(sys.argv) > 2 else "2026-08-29"
    cutoff = datetime.strptime(cutoff_s + " 00:00:00", FMT)

    trs = load(bt)
    # 按平仓时间决定归属, 避免持仓跨界的重复计入
    ins = [t for t in trs if t["ct"] < cutoff]
    oos = [t for t in trs if t["ct"] >= cutoff]

    print("=" * 74)
    print("样本外增量评估  |  文件: %s" % os.path.basename(bt))
    print("切点(按平仓时间): %s" % cutoff_s)
    print("=" * 74)

    si, so = seg_stats(ins), seg_stats(oos)
    print("\n%-12s %6s %8s %12s %10s %10s %10s" %
          ("区间", "笔数", "胜率%", "加权收益pp", "均笔%", "均盈%", "均亏%"))
    print("-" * 74)
    for nm, s in (("样本内", si), ("样本外", so)):
        print("%-12s %6d %8.2f %12.2f %10.2f %10.2f %10.2f" %
              (nm, s["n"], s["win"], s["sum"], s["avg"], s["avg_win"], s["avg_los"]))
    print("-" * 74)
    print("%-12s %6d %8.2f %12.2f" % ("合计", si["n"] + so["n"], 0, si["sum"] + so["sum"]))

    # 全窗口最大回撤(峰值-谷值, 单位pp, 按加权权益曲线)
    eq = 0.0
    peak = 0.0
    mdd = 0.0
    for t in trs:
        eq += contrib(t)
        peak = max(peak, eq)
        mdd = max(mdd, peak - eq)
    ddr = (mdd / peak) if peak > 0 else 0.0
    print("\n全窗口加权权益: %.2f pp  |  最大回撤 %.2f pp  |  ddRatio %.4f" %
          (eq, mdd, ddr))

    # 样本外长度换算成天数
    span_days = (oos[-1]["ct"] - oos[0]["t"]).total_seconds() / 86400.0 if oos else 0.0
    print("样本外跨度: %.1f 天" % span_days)

    print("\n" + "-" * 74)
    print("把样本外收益放进样本内'滚动同长度窗口'分布:")
    print("-" * 74)
    print("%-8s %6s %10s %10s %10s %10s %8s" %
          ("窗口", "样本", "P5", "中位", "P95", "样本外值", "百分位"))
    for days in (7, 14, 21):
        dist = rolling_sums(ins, days)
        if not dist:
            continue
        dist_s = sorted(dist)
        p5 = dist_s[int(0.05 * (len(dist_s) - 1))]
        p50 = dist_s[int(0.50 * (len(dist_s) - 1))]
        p95 = dist_s[int(0.95 * (len(dist_s) - 1))]
        # 把样本外收益按天数等比折算, 让可比
        scaled = so["sum"] * (days / span_days) if span_days > 0 else 0.0
        rk = pct_rank(dist, scaled)
        print("%-8s %6d %10.2f %10.2f %10.2f %10.2f %7.1f%%" %
              ("%dd" % days, len(dist), p5, p50, p95, scaled, rk))

    neg = rolling_sums(ins, 14)
    if neg:
        bad = sum(1 for x in neg if x <= 0)
        print("\n样本内 14 天滚动窗口中, 收益 <= 0 的占比: %.1f%% (%d/%d)" %
              (100.0 * bad / len(neg), bad, len(neg)))
        srt = sorted(neg)
        print("样本内 14 天滚动窗口最差 %.2f pp / 最好 %.2f pp" % (srt[0], srt[-1]))

    print("\n" + "-" * 74)
    print("月度分解(全窗口, 按平仓时间):")
    print("-" * 74)
    bym = {}
    for t in trs:
        bym.setdefault(t["ct"].strftime("%Y-%m"), []).append(t)
    print("%-9s %6s %8s %12s %10s" % ("月份", "笔数", "胜率%", "加权收益pp", "累计pp"))
    cum = 0.0
    for m in sorted(bym):
        s = seg_stats(bym[m])
        cum += s["sum"]
        print("%-9s %6d %8.2f %12.2f %10.2f" %
              (m, s["n"], s["win"], s["sum"], cum))

    regime_report(trs, cutoff)


def regime_report(trs, cutoff):
    """按 regime[T-1] 拆分(与引擎 gate 口径一致: T 日信号用 T-1 日 regime)。
    这是判断 regime 过滤该不该开的核心依据。"""
    rp = os.path.join(WD, "data", "regime_btc.csv")
    if not os.path.exists(rp):
        print("\n(regime_btc.csv 不存在, 跳过 regime 分组)")
        return
    reg = {r["date"]: r["regime"]
           for r in csv.DictReader(open(rp, encoding="utf-8-sig"))}

    print("\n" + "-" * 74)
    print("按 regime[T-1] 分组 (引擎 gate 口径: T 日信号引用 T-1 日 regime)")
    print("-" * 74)
    buckets = {}
    missing = 0
    for t in trs:
        # 用 permitOpenTime(信号 tick, 引擎 gate 的锚点), 不是 openTime(成交 tick)
        d1 = (t["sig"] - timedelta(days=1)).strftime("%Y-%m-%d")
        g = reg.get(d1)
        if g is None:
            missing += 1
            g = "MISSING"
        buckets.setdefault(g, []).append(t)
    if missing:
        print("  ⚠️ regime 缺失(=白放行) %d 笔" % missing)

    print("\n%-8s %-12s %6s %8s %12s %10s %10s" %
          ("区间", "regime", "笔数", "胜率%", "加权收益pp", "均笔%", "pump_stop%"))
    for tag, sel in (("样本内", {k: [x for x in v if x["ct"] < cutoff] for k, v in buckets.items()}),
                     ("样本外", {k: [x for x in v if x["ct"] >= cutoff] for k, v in buckets.items()}),
                     ("全窗口", buckets)):
        for g in ("BEAR", "BULL", "SIDEWAYS", "MISSING"):
            lst = sel.get(g, [])
            if not lst:
                continue
            s = seg_stats(lst)
            ps = 100.0 * sum(1 for x in lst if x["reason"] == "pump_stop") / len(lst)
            print("%-8s %-12s %6d %8.2f %12.2f %10.2f %10.1f" %
                  (tag, g, s["n"], s["win"], s["sum"], s["avg"], ps))

    print("\n样本内 pump_stop 率 vs 样本外 (全窗口对照):")
    for tag, sel in (("样本内", [x for x in trs if x["ct"] < cutoff]),
                     ("样本外", [x for x in trs if x["ct"] >= cutoff])):
        if not sel:
            continue
        c = {}
        for x in sel:
            c[x["reason"]] = c.get(x["reason"], 0) + 1
        tot = len(sel)
        print("  %-8s " % tag + " | ".join(
            "%s=%d(%.0f%%)" % (k, v, 100.0 * v / tot) for k, v in sorted(c.items())))


if __name__ == "__main__":
    main()
