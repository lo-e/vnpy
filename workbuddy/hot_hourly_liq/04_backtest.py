"""04_backtest.py —— 小时级极端波动策略回测（反转 vs 动量，数据定方向）。

事件定义：某 1h 的 |收益率| >= EXTREME_PCT(默认5%) 且 当时24h成交额 >= 该币24h成交额中位数（流动性闸门）。
方向：
  reversion : 大涨(>=+阈值) -> 做空；大跌(<=-阈值) -> 做多
  momentum  : 大涨 -> 做多；大跌 -> 做空
出场：持有 HOLD 根 1h K（4/8/24h）后收盘价平仓；中途触发对称止损 STOP_PCT 即按止损价平仓（先到先得）。
成本：单边 (SLIPPAGE+FEE)，往返扣 2 份。
同币冷却：一笔未平不开新仓。

输出：data/backtest/<dir>_trades.csv 每笔交易；data/backtest/summary.csv 各配置指标；
      并选最优配置写 data/backtest/equity_<best>.csv 供绘图。
"""

import os
import json
import glob
import numpy as np
import pandas as pd
from lib import config

config.ensure_dirs()

EXTREME_PCT = 0.05
DIRECTIONS = ["reversion", "momentum"]
HOLD_LIST = config.HOLD_HOURS_LIST
STOP = config.STOP_PCT
SLIP = config.SLIPPAGE_PCT
FEE = config.FEE_PCT
COST = SLIP + FEE  # 单边


def load_sym(npz_path):
    d = np.load(npz_path, allow_pickle=False)
    return {
        "sym": os.path.basename(npz_path)[:-4],
        "ot": d["open_time"].astype("int64"),
        "o": d["open"].astype("float64"),
        "h": d["high"].astype("float64"),
        "l": d["low"].astype("float64"),
        "c": d["close"].astype("float64"),
        "ret": d["ret1h"].astype("float64"),
        "vol24": d["vol24h_q"].astype("float64"),
    }


def run_symbol(S, direction, hold):
    ret = S["ret"]
    n = len(ret)
    if n < hold + 5:
        return []
    med_vol = np.nanmedian(S["vol24"])
    # 事件掩码
    big_up = ret >= EXTREME_PCT
    big_dn = ret <= -EXTREME_PCT
    trades = []
    last_close = -1
    for t in range(1, n - hold - 1):
        if not (big_up[t] or big_dn[t]):
            continue
        if not np.isfinite(ret[t]) or not np.isfinite(S["vol24"][t]):
            continue
        if S["vol24"][t] < med_vol:      # 流动性闸门
            continue
        if t <= last_close:              # 同币冷却
            continue
        entry = S["o"][t + 1]
        if not np.isfinite(entry) or entry <= 0:
            continue
        # 方向
        if direction == "reversion":
            side = "short" if big_up[t] else "long"
        else:
            side = "long" if big_up[t] else "short"
        # 出场扫描
        exit_px = None
        exit_k = None
        for k in range(t + 1, min(t + 1 + hold, n)):
            if side == "long":
                if S["l"][k] <= entry * (1 - STOP):
                    exit_px = entry * (1 - STOP); exit_k = k; break
            else:
                if S["h"][k] >= entry * (1 + STOP):
                    exit_px = entry * (1 + STOP); exit_k = k; break
            if k == t + hold:
                exit_px = S["c"][k]; exit_k = k; break
        if exit_px is None:  # 安全兜底（不应发生）
            exit_px = S["c"][min(t + hold, n - 1)]; exit_k = min(t + hold, n - 1)
        raw = (exit_px / entry - 1) if side == "long" else (entry / exit_px - 1)
        net = raw - 2 * COST
        trades.append({
            "symbol": S["sym"], "entry_ot": int(S["ot"][t + 1]),
            "exit_ot": int(S["ot"][exit_k]), "side": side,
            "entry": entry, "exit": exit_px, "raw_pct": raw * 100,
            "net_pct": net * 100, "hold_h": exit_k - (t + 1),
        })
        last_close = exit_k
    return trades


def equity_curve(trades_df):
    df = trades_df.sort_values("exit_ot").copy()
    eq = 1.0
    rows = []
    peak = 1.0
    max_dd = 0.0
    for _, r in df.iterrows():
        eq *= (1 + r["net_pct"] / 100.0)
        peak = max(peak, eq)
        dd = (peak - eq) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)
        rows.append((r["exit_ot"], eq, dd))
    eq_df = pd.DataFrame(rows, columns=["exit_ot", "equity", "dd"])
    total_ret = eq - 1.0
    dd_ratio = max_dd / abs(total_ret) if total_ret != 0 else np.nan
    return eq_df, total_ret, max_dd, dd_ratio


def summarize(trades_df):
    p = trades_df["net_pct"].values
    n = len(p)
    win = (p > 0).mean() if n else np.nan
    total = p.sum()
    mean = p.mean() if n else np.nan
    std = p.std() if n else np.nan
    sharpe = mean / std if std and std > 0 else np.nan
    eq_df, total_ret, max_dd, dd_ratio = equity_curve(trades_df)
    return {
        "n_trades": n, "win_rate": win * 100, "total_pct": total,
        "mean_pct": mean, "std_pct": std, "sharpe": sharpe,
        "eq_total_ret": total_ret * 100, "max_dd": max_dd * 100,
        "dd_ratio": dd_ratio,
    }


def main():
    files = sorted(glob.glob(f"{config.DIR_1H}/*.npz"))
    print(f"回测载入 1h 文件: {len(files)}")
    results = []
    best_key = None
    best_score = -1e9
    for direction in DIRECTIONS:
        for hold in HOLD_LIST:
            key = f"{direction}_h{hold}"
            all_trades = []
            for f in files:
                S = load_sym(f)
                all_trades += run_symbol(S, direction, hold)
            tdf = pd.DataFrame(all_trades)
            if tdf.empty:
                print(f"  {key}: 无交易")
                continue
            tdf.to_csv(f"{config.DIR_BT}/{key}_trades.csv", index=False)
            summ = summarize(tdf)
            summ["config"] = key
            summ["direction"] = direction
            summ["hold_h"] = hold
            results.append(summ)
            # 评分：综合总收益与夏普，惩罚大回撤
            score = summ["eq_total_ret"] - summ["max_dd"] * 2
            if score > best_score:
                best_score = score
                best_key = key
            print(f"  {key}: trades={summ['n_trades']} win={summ['win_rate']:.1f}% "
                  f"total={summ['total_pct']:.1f}% eqRet={summ['eq_total_ret']:.1f}% "
                  f"maxDD={summ['max_dd']:.1f}% ddR={summ['dd_ratio']:.3f} sharpe={summ['sharpe']:.2f}")

    sdf = pd.DataFrame(results)[["config", "direction", "hold_h", "n_trades", "win_rate",
                                 "total_pct", "mean_pct", "std_pct", "sharpe",
                                 "eq_total_ret", "max_dd", "dd_ratio"]]
    sdf = sdf.sort_values("eq_total_ret", ascending=False)
    sdf.to_csv(f"{config.DIR_BT}/summary.csv", index=False)
    print(f"\n最优配置: {best_key}")
    # 导出最优净值曲线
    if best_key:
        bt = pd.read_csv(f"{config.DIR_BT}/{best_key}_trades.csv")
        eq_df, _, _, _ = equity_curve(bt)
        eq_df.to_csv(f"{config.DIR_BT}/equity_{best_key}.csv", index=False)
        print(f"已导出 data/backtest/equity_{best_key}.csv")
    print("回测完成。")


if __name__ == "__main__":
    main()
