"""02_build_1h.py —— 为流动性宇宙构建带因子的 1h K 线并压缩落盘。

读取 data/universe.json -> 逐个 symbol 拉取 1h 聚合 + 计算 heat/vol/liq 因子 ->
保存 data/1h/<SYMBOL>.npz（含 OHLCV 与因子）。后续 screener / backtest 直接读本地产物，不再查 Mongo。
"""

import json
import numpy as np
from lib import config, mongo_io, metrics

config.ensure_dirs()


def main():
    uni = json.load(open(config.DATA + "/universe.json"))
    syms = [s["symbol"] for s in uni["symbols"]]
    print(f"宇宙符号数: {len(syms)}，开始构建 1h ...")
    for i, s in enumerate(syms):
        arr = mongo_io.load_1h(s)
        if arr is None or len(arr["c"]) < 48:
            print(f"  [{i+1}/{len(syms)}] {s}: 数据不足，跳过")
            continue
        feat = metrics.compute_features(arr)
        out = {
            "open_time": arr["open_time"],
            "open": arr["o"], "high": arr["h"], "low": arr["l"], "close": arr["c"],
            "volume": arr["v"], "qvol": arr["qv"], "trades": arr["tr"], "tbq": arr["tbq"],
            "ret1h": feat["ret1h"], "range1h": feat["range1h"], "body1h": feat["body1h"],
            "taker_ratio": feat["taker_ratio"], "vol24h_q": feat["vol24h_q"],
            "amihud": feat["amihud"],
        }
        np.savez_compressed(f"{config.DIR_1H}/{s}.npz", **out)
        if (i + 1) % 20 == 0:
            print(f"  [{i+1}/{len(syms)}] 已完成 {s}")
    print("1h 构建完成。")


if __name__ == "__main__":
    main()
