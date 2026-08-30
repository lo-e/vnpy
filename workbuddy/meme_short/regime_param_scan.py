# -*- coding: utf-8 -*-
"""regime 参数网格粗筛: 24 组合 × 训练/验证切分(样本外纪律)
训练 = 2024-01~2024-12(前12月), 验证 = 2025-01~2025-11(后11月)
事后过滤(T-1日regime, 防前视一致) → 秒级粗筛; top 组合再引擎内验证
"""
import os, csv, subprocess, sys, tempfile
from datetime import datetime, timedelta
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)
from market_regime import get_regime_map, write_csv, fetch_daily_close, build_regime

SRC = os.path.join(BASE, "data", "change_20240101_20251201", "_bt_2024window_v2.csv")
TRAIN_END = "2024-12-31"   # 训练段: openTime <= 2024-12-31
rows = list(csv.DictReader(open(SRC, encoding="utf-8-sig")))

def fnum(r, k):
    try: return float(r[k])
    except: return 0.0

def metrics(srt):
    if not srt: return (0, 0.0, 0.0, 0.0)
    wpnl = sum(fnum(r, "pnlPct") * fnum(r, "weight") for r in srt)
    win = sum(1 for r in srt if fnum(r, "pnlPct") > 0)
    cum = peak = maxdd = 0.0
    for r in srt:
        cum += fnum(r, "pnlPct") * fnum(r, "weight")
        peak = max(peak, cum); maxdd = max(maxdd, peak - cum)
    return (len(srt), wpnl, win / len(srt) * 100 if srt else 0,
            (maxdd / wpnl) if wpnl > 0 else 9.99)

def evaluate(regime_map):
    """事后过滤: permitOpenTime 所在日 T-1 的 regime ∈ allow; 返回 (训练指标, 验证指标)"""
    res_train, res_valid = [], []
    for r in rows:
        d = r["permitOpenTime"].split(" ")[0]
        d1 = (datetime.strptime(d, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        if regime_map.get(d1, "SIDEWAYS") not in ALLOW:
            continue
        if r["openTime"] <= TRAIN_END + " 23:59:59":
            res_train.append(r)
        else:
            res_valid.append(r)
    tr = metrics(sorted(res_train, key=lambda r: r["openTime"]))
    va = metrics(sorted(res_valid, key=lambda r: r["openTime"]))
    return tr, va

results = []
for br in [0.15, 0.20, 0.25, 0.30]:
    for ber in [-0.10, -0.15, -0.20]:
        for allow in [["BEAR"], ["BULL", "BEAR"]]:
            # 生成该参数下的 regime(独立进程, 防缓存干扰)
            out_f = os.path.join(tempfile.gettempdir(), "regime_scan.csv")
            env = dict(os.environ)
            env.update({"BULL_R200": str(br), "BEAR_R200": str(ber), "REGIME_OUT": out_f})
            subprocess.run([sys.executable, os.path.join(BASE, "market_regime.py")],
                           env=env, capture_output=True, timeout=120)
            if not os.path.exists(out_f):
                results.append((br, ber, ",".join(allow), None, None)); continue
            rm = {}
            with open(out_f, encoding="utf-8-sig") as f:
                for rr in csv.DictReader(f):
                    rm[rr["date"]] = rr["regime"]
            global ALLOW
            ALLOW = set(allow)
            tr, va = evaluate(rm)
            results.append((br, ber, ",".join(allow), tr, va))

# 输出: 按训练段 wpnl 排序
print("BULL_R200 BEAR_R200 ALLOW        | 训练[2024前12月] n/wpnl/ddRatio | 验证[2025后11月] n/wpnl/ddRatio")
print("-" * 100)
for br, ber, al, tr, va in sorted(results, key=lambda x: -(x[3][1] if x[3] else 0)):
    if tr and va:
        print(f"{br:>9} {ber:>9} {al:13s} | {tr[0]:5d} {tr[1]:+8.1f} {tr[3]:7.4f} | {va[0]:5d} {va[1]:+8.1f} {va[3]:7.4f}")
    else:
        print(f"{br:>9} {ber:>9} {al:13s} | 生成失败")
