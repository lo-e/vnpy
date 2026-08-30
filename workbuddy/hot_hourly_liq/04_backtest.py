"""04_backtest.py —— 「小时 K 走势图暴涨/暴跌」顺势策略回测。

事件定义（不再是单根 K 大涨跌幅，而是「连续多小时的明显走势」）：
  过去 K 小时累计收益 ret_K[t] = close[t]/close[t-K] - 1 突破 ±THR 阈值
  + 当前 1h 收益方向与 ret_K 同号（确认「此时仍在动」）
  + 最近 K 段内连续同向 candle ≥ MIN_RUN（走势有持续性，不是孤立跳涨）
方向：顺势，ret_K ≥ +THR 做多 / ret_K ≤ -THR 做空
出场（先到先得）：
  1) 跟踪止损：入场后最高(做空)/最低(做多)回撤 TRAIL_PCT 平仓
  2) 持有期上限 HOLD 小时，到期按收盘价平
成本：双边扣 SLIP+FEE（单边=COST）。
同币冷却：上一笔 exit 之前不开新仓。

输出：
  data/backtest/tf_<config>_trades.csv    每笔交易明细
  data/backtest/tf_summary.csv           各配置指标（含样本外一半/一半对比、尾部诊断）
  data/backtest/equity_<best>.csv         最优配置净值供画图
"""

import os
import glob
import numpy as np
import pandas as pd
from lib import config

config.ensure_dirs()

# ---- 参数网格：走势窗口 K × 累计阈值 THR × 持有期 HOLD × 跟踪止损 TRAIL ----
K_LIST = [4, 8, 12]                # 累计收益回看小时数
THR_LIST = [0.10, 0.15, 0.20]      # 过去 K 小时累计 |收益| 突破阈值
HOLD_LIST = [4, 8, 12]             # 最大持仓小时（到期收盘平）
TRAIL_LIST = [0.06, 0.10, 0.15]    # 跟踪止损：自最高/最低回撤幅度
MIN_RUN = 2                        # 趋势连续性要求：K 段内最近连续同向 candle 数 ≥ MIN_RUN
LIQ_GATE = 0.0                     # 24h 成交额 ≥ 数据自身中位数×此系数（0 = 不限）

COST = config.SLIPPAGE_PCT + config.FEE_PCT  # 单边成本


def load_sym(npz_path):
    d = np.load(npz_path, allow_pickle=False)
    S = {"sym": os.path.basename(npz_path)[:-4]}
    for k in ["open_time", "open", "high", "low", "close"]:
        S[k] = d[k.replace("open_time", "open_time").replace("open", "open")
                  if False else k].astype("int64" if k == "open_time" else "float64")
    return S


def _read_npz(npz_path):
    """直接读出原始键名，避免上面 load_sym 写错变量。"""
    d = np.load(npz_path, allow_pickle=False)
    return {
        "sym": os.path.basename(npz_path)[:-4],
        "ot":  d["open_time"].astype("int64"),
        "o":   d["open"].astype("float64"),
        "h":   d["high"].astype("float64"),
        "l":   d["low"].astype("float64"),
        "c":   d["close"].astype("float64"),
        "ret1h": d["ret1h"].astype("float64"),
        "vol24": d["vol24h_q"].astype("float64"),
        "ret_K": {K: d[f"ret_{K}h"].astype("float64") for K in K_LIST},
        "trend_dir": d["trend_dir"].astype("float64"),
        "trend_run": d["trend_run"].astype("int64"),
    }


def run_symbol(S, K, THR, HOLD, TRAIL):
    """对单 symbol 跑一组参数下的全部顺势交易。"""
    retK = S["ret_K"][K]
    n = len(retK)
    if n < K + HOLD + 2:
        return []
    # 流动性闸门：当时 24h 成交额 ≥ 该币自身中位数 × LIQ_GATE
    med_vol = np.nanmedian(S["vol24"]) if LIQ_GATE > 0 else 0.0
    dir_arr = S["trend_dir"]
    run_arr = S["trend_run"]
    trades = []
    last_exit = -1
    for t in range(K, n - HOLD - 1):
        r = retK[t]
        if not np.isfinite(r):
            continue
        side = None
        if r >= THR and dir_arr[t] >= 1 and run_arr[t] >= MIN_RUN:
            side = "long"
        elif r <= -THR and dir_arr[t] <= -1 and run_arr[t] >= MIN_RUN:
            side = "short"
        if side is None:
            continue
        if LIQ_GATE > 0 and (not np.isfinite(S["vol24"][t]) or S["vol24"][t] < med_vol * LIQ_GATE):
            continue
        entry = S["o"][t + 1]
        if not np.isfinite(entry) or entry <= 0:
            continue
        if t <= last_exit:
            continue
        # 出场扫描：先到先得
        peak = entry
        trough = entry
        exit_px = None
        exit_k = None
        for k in range(t + 1, min(t + 1 + HOLD, n)):
            if side == "long":
                peak = max(peak, S["h"][k])
                if S["l"][k] <= peak * (1 - TRAIL):
                    exit_px = peak * (1 - TRAIL); exit_k = k; break
            else:
                trough = min(trough, S["l"][k])
                if S["h"][k] >= trough * (1 + TRAIL):
                    exit_px = trough * (1 + TRAIL); exit_k = k; break
            if k == t + HOLD:
                exit_px = S["c"][k]; exit_k = k; break
        if exit_px is None:
            exit_px = S["c"][min(t + HOLD, n - 1)]; exit_k = min(t + HOLD, n - 1)
        raw = (exit_px / entry - 1) if side == "long" else (entry / exit_px - 1)
        net = raw - 2 * COST
        trades.append({
            "symbol": S["sym"], "entry_ot": int(S["ot"][t + 1]),
            "exit_ot":  int(S["ot"][exit_k]), "side": side,
            "config_K": K, "config_THR": THR, "config_HOLD": HOLD, "config_TRAIL": TRAIL,
            "ret_K_at_entry": r, "trend_run_at_entry": int(run_arr[t]),
            "entry": entry, "exit": exit_px, "raw_pct": raw * 100,
            "net_pct": net * 100, "hold_h": exit_k - (t + 1),
        })
        last_exit = exit_k
    return trades


def equity_curve(trades_df):
    """加法净值 cumPnl（等额本金），不被单笔极端单吃掉。起点 0，单位 pp。"""
    df = trades_df.sort_values("exit_ot").copy()
    eq = 0.0
    rows = []
    peak = 0.0
    max_dd = 0.0
    for _, r in df.iterrows():
        eq += r["net_pct"]
        peak = max(peak, eq)
        dd = peak - eq
        max_dd = max(max_dd, dd)
        rows.append((int(r["exit_ot"]), eq, dd))
    eq_df = pd.DataFrame(rows, columns=["exit_ot", "equity", "dd"])
    total_ret = eq
    dd_ratio = max_dd / abs(total_ret) if total_ret != 0 else np.nan
    return eq_df, total_ret, max_dd, dd_ratio


def split_time_robustness(trades_df):
    """样本外稳定性：按 exit_ot 时序 50/50 切分，看两段 split 表现。"""
    df = trades_df.sort_values("exit_ot")
    mid = len(df) // 2
    if mid == 0:
        return {"h1_n": 0, "h1_sum": np.nan, "h1_win": np.nan,
                "h2_n": 0, "h2_sum": np.nan, "h2_win": np.nan}
    h1, h2 = df.iloc[:mid], df.iloc[mid:]
    return {
        "h1_n":   len(h1),
        "h1_sum": float(h1["net_pct"].sum()),
        "h1_win": float((h1["net_pct"] > 0).mean() * 100),
        "h2_n":   len(h2),
        "h2_sum": float(h2["net_pct"].sum()),
        "h2_win": float((h2["net_pct"] > 0).mean() * 100),
    }


def summarize(trades_df):
    p = trades_df["net_pct"].values
    n = len(p)
    win = (p > 0).mean() if n else np.nan
    total = float(p.sum())
    mean = float(p.mean()) if n else np.nan
    median = float(np.median(p)) if n else np.nan
    std = float(p.std()) if n else np.nan
    sharpe = mean / std if std and std > 0 else np.nan
    # 尾部诊断（|pnl|>50pp）
    out_mask = np.abs(p) > 50.0
    n_out = int(out_mask.sum())
    contrib_out = float(p[out_mask].sum()) if n_out else 0.0
    contrib_out_ratio = (contrib_out / total) if total != 0 else np.nan
    # 剔除大单后的稳健性
    in_mask = ~out_mask
    n_in = int(in_mask.sum())
    robust_total = float(p[in_mask].sum()) if n_in else np.nan
    robust_median = float(np.median(p[in_mask])) if n_in else np.nan
    eq_df, total_ret, max_dd, dd_ratio = equity_curve(trades_df)
    sample_split = split_time_robustness(trades_df)
    return {
        "n_trades": n, "win_rate": float(win * 100), "total_pct": total,
        "mean_pct": mean, "median_pct": median, "std_pct": std, "sharpe": sharpe,
        "eq_total_ret": float(total_ret), "max_dd": float(max_dd), "dd_ratio": float(dd_ratio),
        "n_outlier_gt50pp": n_out, "outlier_contrib_ratio": float(contrib_out_ratio),
        "robust_total_pct": robust_total, "robust_median_pct": robust_median,
        **sample_split,
    }


def main():
    files = sorted(glob.glob(f"{config.DIR_1H}/*.npz"))
    print(f"回测载入 1h 文件: {len(files)}")
    # 预读数据避免重复 IO
    print("预读 1h 数据...")
    all_S = [_read_npz(f) for f in files]

    results = []
    best = {"score": -1e9, "key": None, "trades": None}
    grid = [(K, THR) for K in K_LIST for THR in THR_LIST]
    print(f"参数网格: {len(grid)} 组合  × 3 持有期 × 3 跟踪止损 = {len(grid)*len(HOLD_LIST)*len(TRAIL_LIST)} 配置")
    for K, THR in grid:
        for HOLD in HOLD_LIST:
            for TRAIL in TRAIL_LIST:
                key = f"K{K}_THR{int(THR*100)}_H{HOLD}_T{int(TRAIL*100)}"
                all_trades = []
                for S in all_S:
                    all_trades += run_symbol(S, K, THR, HOLD, TRAIL)
                if not all_trades:
                    continue
                tdf = pd.DataFrame(all_trades).sort_values("exit_ot").reset_index(drop=True)
                tdf.to_csv(f"{config.DIR_BT}/tf_{key}_trades.csv", index=False)
                summ = summarize(tdf)
                summ["config"] = key
                summ["K"] = K; summ["THR"] = THR; summ["HOLD"] = HOLD; summ["TRAIL"] = TRAIL
                results.append(summ)
                # 综合评分：总收益 - 2×maxDD（小回撤优先；负数会自然被淘汰）
                score = summ["eq_total_ret"] - 2 * summ["max_dd"]
                if score > best["score"]:
                    best.update({"score": score, "key": key, "trades": tdf})
                print(f"  {key}: n={summ['n_trades']:>4} win={summ['win_rate']:5.1f}% "
                      f"sum={summ['total_pct']:7.0f}pp  maxDD={summ['max_dd']:6.0f}pp  "
                      f"ddR={summ['dd_ratio']:.3f}  sharpe={summ['sharpe']:.2f}  "
                      f"h1/h2={summ['h1_sum']:+.0f}/{summ['h2_sum']:+.0f}pp")

    cols = ["config", "K", "THR", "HOLD", "TRAIL", "n_trades", "win_rate",
            "total_pct", "mean_pct", "median_pct", "std_pct", "sharpe",
            "eq_total_ret", "max_dd", "dd_ratio",
            "n_outlier_gt50pp", "outlier_contrib_ratio",
            "robust_total_pct", "robust_median_pct",
            "h1_n", "h1_sum", "h1_win", "h2_n", "h2_sum", "h2_win"]
    sdf = pd.DataFrame(results)[cols].sort_values("eq_total_ret", ascending=False)
    sdf.to_csv(f"{config.DIR_BT}/tf_summary.csv", index=False)
    print(f"\n最优配置: {best['key']}  score={best['score']:.1f}")
    if best["key"]:
        eq_df, _, _, _ = equity_curve(best["trades"])
        eq_df.to_csv(f"{config.DIR_BT}/equity_{best['key']}.csv", index=False)
        print(f"已导出 data/backtest/equity_{best['key']}.csv")
    print("回测完成。")


if __name__ == "__main__":
    main()
