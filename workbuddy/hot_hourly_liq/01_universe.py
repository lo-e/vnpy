"""01_universe.py —— 流动性宇宙筛选。

按研究窗口内「日成交额中位数」对全市场 USDT 永续排序，取前 TOP_N_UNIVERSE 名
（或越过 LIQ_FLOOR_USD 地板者）作为流动性宇宙。这些名字即「成交额口径下最热且最流动」的标的。
结果写入 data/universe.json。
"""

import json
import numpy as np
from lib import config, mongo_io

config.ensure_dirs()


def main():
    syms = mongo_io.all_symbols()
    print(f"全市场符号数: {len(syms)}")

    recs = []
    for i, s in enumerate(syms):
        d = mongo_io.load_daily_volume(s)
        if d is None or len(d["qv"]) == 0:
            continue
        med = float(np.median(d["qv"]))
        total = float(np.sum(d["qv"]))
        days = int(len(d["qv"]))
        recs.append((s, med, total, days))
        if (i + 1) % 100 == 0:
            print(f"  进度 {i+1}/{len(syms)} ...")

    recs.sort(key=lambda r: r[1], reverse=True)
    med_all = np.array([r[1] for r in recs])
    p90 = float(np.percentile(med_all, 90))
    print(f"日成交额中位数 全市场 P50={np.median(med_all):,.0f}  P90={p90:,.0f}  P10={np.percentile(med_all,10):,.0f}")

    chosen = []
    for s, med, total, days in recs:
        if len(chosen) < config.TOP_N_UNIVERSE or med >= config.LIQ_FLOOR_USD:
            chosen.append({"symbol": s, "median_daily_qv": med,
                           "total_qv": total, "days": days})
        if len(chosen) >= config.TOP_N_UNIVERSE and med < config.LIQ_FLOOR_USD:
            # 已取满 TOP_N 且后续都低于地板 -> 停止
            if len([c for c in chosen if c["median_daily_qv"] >= config.LIQ_FLOOR_USD]) >= config.TOP_N_UNIVERSE:
                break

    universe = {
        "window_start": config.WINDOW_START,
        "window_end": config.WINDOW_END,
        "top_n": config.TOP_N_UNIVERSE,
        "liq_floor_usd": config.LIQ_FLOOR_USD,
        "n_symbols": len(chosen),
        "median_daily_qv_p90": p90,
        "symbols": chosen,
    }
    out = config.DATA + "/universe.json"
    with open(out, "w") as f:
        json.dump(universe, f, indent=2)
    print(f"宇宙符号数: {len(chosen)}  写入 {out}")
    print("Top10 by median daily quote volume:")
    for c in chosen[:10]:
        print(f"  {c['symbol']:12s} med_daily_qv={c['median_daily_qv']:,.0f}")


if __name__ == "__main__":
    main()
