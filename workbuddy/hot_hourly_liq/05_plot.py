"""05_plot.py —— 生成回测净值曲线（离线 Plotly，带缩放/平移/十字光标 + maxDD/ddRatio 头部）。

读取 data/backtest/summary.csv 与各 <config>_trades.csv，重建净值曲线：
  - 全部配置归一化对比图（看反转 vs 动量）
  - 最优配置详情图（标注 maxDD / ddRatio，绘制回撤带）
输出 data/backtest/curve_compare.html 与 curve_best.html。
"""

import os
import glob
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from lib import config
import importlib.util

spec = importlib.util.spec_from_file_location("bt", f"{config.ROOT}/04_backtest.py")
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)

config.ensure_dirs()


def main():
    sdf = pd.read_csv(f"{config.DIR_BT}/summary.csv")
    configs = sdf["config"].tolist()
    eqs = {}
    for cfg in configs:
        tdf = pd.read_csv(f"{config.DIR_BT}/{cfg}_trades.csv")
        eq_df, _, _, _ = bt.equity_curve(tdf)
        # 归一化到起点 1.0
        eqs[cfg] = eq_df

    # ---- 对比图：所有配置归一化净值 ----
    fig = go.Figure()
    palette = {"reversion": "rgb(255,90,90)", "momentum": "rgb(90,140,255)"}
    for cfg in configs:
        eq = eqs[cfg]
        col = palette.get(cfg.split("_")[0], "rgb(120,120,120)")
        fig.add_trace(go.Scatter(x=eq["exit_ot"], y=eq["equity"],
                                 mode="lines", name=cfg, line=dict(width=1.5, color=col)))
    fig.update_layout(title="全部配置归一化净值对比（起点=1.0，反转=红/动量=蓝）",
                     xaxis_title="时间(ms UTC)", yaxis_title="净值",
                     hovermode="x unified", template="plotly_dark", height=520)
    fig.write_html(f"{config.DIR_BT}/curve_compare.html", include_plotlyjs="inline", full_html=True)

    # ---- 最优配置详情图 ----
    best = sdf.sort_values("eq_total_ret", ascending=False).iloc[0]["config"]
    brow = sdf[sdf["config"] == best].iloc[0]
    eq = eqs[best]
    # 计算回撤带
    eq = eq.copy()
    eq["peak"] = eq["equity"].cummax()
    eq["dd_pct"] = (eq["peak"] - eq["equity"]) / eq["peak"] * 100

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=eq["exit_ot"], y=eq["equity"], mode="lines",
                              name="equity", line=dict(width=2, color="rgb(80,200,120)"),
                              hovertemplate="时间 %{x}<br>净值 %{y:.3f}<extra></extra>"))
    # 回撤填充（净值到峰值）
    fig2.add_trace(go.Scatter(x=eq["exit_ot"], y=eq["peak"], mode="lines",
                              line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig2.add_trace(go.Scatter(x=eq["exit_ot"], y=eq["equity"], mode="lines",
                              line=dict(width=0), fill="tonexty", fillcolor="rgba(255,80,80,0.18)",
                              name="回撤", hoverinfo="skip"))
    header = (f"最优配置 {best} ｜ 笔数 {int(brow['n_trades'])} ｜ 胜率 {brow['win_rate']:.1f}% "
              f"｜ 总收益 {brow['eq_total_ret']:.1f}% ｜ MaxDD {brow['max_dd']:.1f}% "
              f"｜ ddRatio {brow['dd_ratio']:.3f} ｜ 夏普 {brow['sharpe']:.2f}")
    fig2.update_layout(title=header, xaxis_title="时间(ms UTC)", yaxis_title="净值",
                       hovermode="x unified", template="plotly_dark", height=560)
    fig2.write_html(f"{config.DIR_BT}/curve_best.html", include_plotlyjs="inline", full_html=True)
    print(header)
    print(f"已写出 curve_compare.html / curve_best.html -> {config.DIR_BT}")


if __name__ == "__main__":
    main()
