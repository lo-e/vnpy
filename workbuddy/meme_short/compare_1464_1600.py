# -*- coding: utf-8 -*-
"""对比 +1464% 历史残缺版(_bt_pump8h.csv) vs +1600% 新版(_nop24_8h.csv)"""
import csv
import os

D = r"C:/Users/lo-e/Quant/vnpy/workbuddy/meme_short/data/change_20260101_20260812"


def load(f):
    rows = list(csv.DictReader(open(os.path.join(D, f), encoding="utf-8-sig")))
    return rows, {(r["symbol"], r["openTime"]): r for r in rows}


def stat(name, rows):
    last = rows[-1]
    wins = sum(1 for r in rows if float(r["pnlPct"]) > 0)
    w = sum(float(r["weight"]) for r in rows)
    print(f"{name:24s} 笔数={len(rows):3d} 加权={w:3.0f} 胜率={wins/len(rows)*100:5.1f}% 毛cumPnl={float(last['cumPnl']):+8.1f}%")


a, da = load("_bt_pump8h.csv")    # +1464% 历史残缺版
b, db = load("_nop24_8h.csv")     # +1600% 新版
stat("+1464%版(_bt_pump8h)", a)
stat("+1600%版(_nop24_8h)", b)

sa, sb = set(da), set(db)
print()
print(f"开仓点重合: {len(sa & sb)} | 仅+1464%版: {len(sa - sb)} | 仅+1600%版: {len(sb - sa)}")
print()

common = sa & sb
diffs = sorted(common, key=lambda k: (float(db[k]["pnlPct"]) - float(da[k]["pnlPct"])))
print("=== 共同单 pnl 差异最大（新-旧，前5差最大+后5涨最多）===")
print(f"{'symbol':<14s} {'openTime':<20s} {'旧pnl':>8s} {'新pnl':>8s} {'差':>8s} | {'旧reason':<10s} {'新reason':<10s}")
for k in diffs[:5] + diffs[-5:]:
    d = float(db[k]["pnlPct"]) - float(da[k]["pnlPct"])
    print(f"{k[0]:<14s} {k[1]:<20s} {float(da[k]['pnlPct']):+7.1f} {float(db[k]['pnlPct']):+7.1f} {d:+7.1f} | {da[k]['reason']:<10s} {db[k]['reason']:<10s}")

print()
print("=== 仅一版有的开仓点 ===")
print("--- 仅+1464%版有 ---")
for k in sorted(sa - sb)[:6]:
    print(f"  {k[0]} {k[1]}  pnl={float(da[k]['pnlPct']):+.1f}% {da[k]['reason']}")
print("--- 仅+1600%版有 ---")
for k in sorted(sb - sa)[:6]:
    print(f"  {k[0]} {k[1]}  pnl={float(db[k]['pnlPct']):+.1f}% {db[k]['reason']}")
