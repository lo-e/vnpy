"""03_screener.py —— 小时级「热度×波动率×流动性」筛选器。

对流动性宇宙内每个 symbol 的 1h 数据按 UTC 日聚合，每日做截面排名：
  vol_score   : 当日成交额百分位（热度/规模）
  vola_score  : 当日「小时级最大绝对波动」百分位（暴涨暴跌强度）
  taker_score : 当日 taker 买压偏离 0.5 的幅度百分位（散户 FOMO / 拥挤）
  liq_factor  : 流动性软门禁（当日成交额达地板=1，否则按比例打折）
  heat_score  = (0.5*vol + 0.3*vola + 0.2*taker) * liq_factor
每日输出 Top-N 榜单 data/screener/heat_YYYYMMDD.csv，并汇总常驻榜单 watchlist.csv。
"""

import os
import json
import glob
import numpy as np
import pandas as pd
from lib import config, metrics

config.ensure_dirs()
EXPECT_COLS = ["symbol", "day", "day_qv", "day_avg_abs_ret", "day_max_abs_ret", "day_taker"]


def day_records(npz_path):
    d = np.load(npz_path, allow_pickle=False)
    ot = d["open_time"]
    day = ot // 86400000
    qv = d["qvol"]
    absret = np.abs(d["ret1h"])
    taker = d["taker_ratio"]
    sym = os.path.basename(npz_path)[:-4]
    df = pd.DataFrame({"day": day, "qv": qv, "absret": absret, "taker": taker})
    g = df.groupby("day").agg(
        day_qv=("qv", "sum"),
        day_avg_abs_ret=("absret", "mean"),
        day_max_abs_ret=("absret", "max"),
        day_taker=("taker", "mean"),
    ).reset_index()
    g["symbol"] = sym
    return g[EXPECT_COLS]


def main():
    files = sorted(glob.glob(f"{config.DIR_1H}/*.npz"))
    print(f"读取 1h 文件: {len(files)}")
    frames = [day_records(f) for f in files]
    all_df = pd.concat(frames, ignore_index=True)
    print(f"总 (symbol,day) 记录: {len(all_df):,}")

    # 逐日截面排名
    out_rows = []
    for day, g in all_df.groupby("day"):
        g = g.copy()
        g["vol_score"] = metrics.percentile_rank(g["day_qv"].values)
        g["vola_score"] = metrics.percentile_rank(g["day_max_abs_ret"].values)
        g["taker_score"] = metrics.percentile_rank((g["day_taker"] - 0.5).abs().values * 2)
        liq = np.where(g["day_qv"].values >= config.LIQ_FLOOR_USD, 1.0,
                       g["day_qv"].values / config.LIQ_FLOOR_USD)
        g["liq_factor"] = liq
        g["heat_score"] = (0.5 * g["vol_score"] + 0.3 * g["vola_score"] + 0.2 * g["taker_score"]) * liq
        top = g.sort_values("heat_score", ascending=False).head(config.SCREENER_TOP_N)
        import datetime
        daystr = datetime.datetime.utcfromtimestamp(day * 86400).strftime("%Y%m%d")
        cols = ["symbol", "day_qv", "day_avg_abs_ret", "day_max_abs_ret", "day_taker",
                "vol_score", "vola_score", "taker_score", "liq_factor", "heat_score"]
        top[cols].to_csv(f"{config.DIR_SCREENER}/heat_{daystr}.csv", index=False)
        out_rows.append(top[["symbol", "heat_score"]])
        if len(out_rows) % 100 == 0:
            print(f"  已处理 {len(out_rows)} 天")

    # 常驻榜单：出现在 Top-N 的频次与平均 heat_score
    freq = pd.concat(out_rows, ignore_index=True)
    watch = freq.groupby("symbol").agg(appear=("heat_score", "size"),
                                       mean_heat=("heat_score", "mean")).reset_index()
    watch = watch.sort_values(["appear", "mean_heat"], ascending=False)
    watch.to_csv(f"{config.DIR_SCREENER}/watchlist.csv", index=False)
    print(f"筛选器完成。常驻榜单 Top10:")
    print(watch.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
