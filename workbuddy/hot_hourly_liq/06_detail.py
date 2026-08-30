"""06_detail.py —— 把回测开平仓明细导出为可读 CSV + 交互式 HTML 图表。

针对定稿推荐配置（默认主推 K4_THR15_H4_T6，额外生成最均衡 K4_THR20_H4_T6 的 CSV）：
  - data/backtest/detail_<CFG>.csv        逐笔开平仓明细（中文列、累计净值）
  - data/backtest/detail_chart_<CFG>.html 净值曲线 + 逐笔多空散点 + 多空累计对比 + 按月收益
"""

import sys
sys.path.insert(0, ".")
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from lib import config

config.ensure_dirs()

# 主推 + 最均衡（都出明细 CSV；仅主推出图表）
CFGS = ["K4_THR15_H4_T6", "K4_THR20_H4_T6"]
CHART_CFG = "K4_THR15_H4_T6"


def build_detail(cfg):
    src = f"{config.DIR_BT}/tf_{cfg}_trades.csv"
    df = pd.read_csv(src).sort_values("exit_ot").reset_index(drop=True)

    def utc(ms):
        return pd.to_datetime(ms, unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M")

    out = pd.DataFrame()
    out["idx"] = range(1, len(df) + 1)
    out["symbol"] = df["symbol"]
    out["side"] = df["side"].map({"long": "多", "short": "空"})
    out["entry_time_utc"] = utc(df["entry_ot"])
    out["entry_price"] = df["entry"].round(6)
    out["exit_time_utc"] = utc(df["exit_ot"])
    out["exit_price"] = df["exit"].round(6)
    out["hold_h"] = df["hold_h"]
    out["ret_K_entry_%"] = (df["ret_K_at_entry"] * 100).round(2)   # 入场时过去 K 小时累计走势幅度
    out["trend_run"] = df["trend_run_at_entry"]
    out["raw_%"] = df["raw_pct"].round(3)
    out["net_%"] = df["net_pct"].round(3)
    out["cum_net_pp"] = df["net_pct"].cumsum().round(2)            # 加法累计净值
    out.to_csv(f"{config.DIR_BT}/detail_{cfg}.csv", index=False)
    return df, out


def build_chart(cfg, df):
    df = df.copy()
    df["exit_time_utc"] = pd.to_datetime(df["exit_ot"], unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M")
    n = len(df)
    win = (df["net_pct"] > 0).mean() * 100
    total = df["net_pct"].sum()
    eq = df["net_pct"].cumsum().values
    peak = np.maximum.accumulate(eq)
    dd = peak - eq
    max_dd = dd.max()
    dd_ratio = max_dd / abs(total) if total else np.nan
    short_m = df["side"] == "short"
    long_m = df["side"] == "long"

    dlong = df[long_m]
    dshort = df[short_m]
    eq_short = dshort["net_pct"].cumsum().values
    eq_long = dlong["net_pct"].cumsum().values

    df["ym"] = pd.to_datetime(df["exit_ot"], unit="ms", utc=True).dt.strftime("%Y-%m")
    mb = df.groupby("ym")["net_pct"].sum()

    s_total = dshort["net_pct"].sum()
    l_total = dlong["net_pct"].sum()

    fig = make_subplots(
        rows=4, cols=1,
        subplot_titles=(
            f"净值曲线 (加法 cumPnl)  总收益={total:.0f}pp  胜率={win:.1f}%  "
            f"maxDD={max_dd:.0f}pp  ddRatio={dd_ratio:.3f}",
            "逐笔净收益 (红=多 / 蓝=空，悬停看开平仓价)",
            f"多空累计净值对比  (空累计 {s_total:.0f}pp vs 多累计 {l_total:.0f}pp)",
            "按月净收益 (pp)",
        ),
        vertical_spacing=0.07,
        row_heights=[0.34, 0.30, 0.20, 0.16],
    )

    fig.add_trace(go.Scatter(
        x=df["exit_time_utc"], y=eq, mode="lines", name="净值pp",
        line=dict(color="rgb(90,200,140)", width=1.4),
        hovertemplate="%{x}<br>净值 %{y:.1f}pp<extra></extra>"), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=dlong["exit_time_utc"], y=dlong["net_pct"], mode="markers", name="多",
        marker=dict(color="rgb(255,90,90)", size=5),
        customdata=np.stack([dlong["symbol"], dlong["entry"], dlong["exit"], dlong["hold_h"]], axis=-1),
        hovertemplate="%{customdata[0]}<br>多 net %{y:.2f}%<br>开 %{customdata[1]} 平 %{customdata[2]} 持 %{customdata[3]}h<extra></extra>"),
        row=2, col=1)
    fig.add_trace(go.Scatter(
        x=dshort["exit_time_utc"], y=dshort["net_pct"], mode="markers", name="空",
        marker=dict(color="rgb(90,140,255)", size=5),
        customdata=np.stack([dshort["symbol"], dshort["entry"], dshort["exit"], dshort["hold_h"]], axis=-1),
        hovertemplate="%{customdata[0]}<br>空 net %{y:.2f}%<br>开 %{customdata[1]} 平 %{customdata[2]} 持 %{customdata[3]}h<extra></extra>"),
        row=2, col=1)

    fig.add_trace(go.Scatter(
        x=dlong["exit_time_utc"], y=eq_long, mode="lines", name="多累计",
        line=dict(color="rgb(255,90,90)", width=1.2)), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=dshort["exit_time_utc"], y=eq_short, mode="lines", name="空累计",
        line=dict(color="rgb(90,140,255)", width=1.2)), row=3, col=1)

    fig.add_trace(go.Bar(
        x=mb.index, y=mb.values, name="月净收益",
        marker_color=np.where(mb.values >= 0, "rgb(90,200,140)", "rgb(255,90,90)")),
        row=4, col=1)

    fig.update_layout(
        height=1120, template="plotly_dark", hovermode="closest",
        title=f"回测开平仓明细 · {cfg}  (n={n}, 双边成本 0.18pp/笔)",
        legend=dict(orientation="h", y=-0.02, x=0))
    fig.update_yaxes(title_text="累计 pp", row=1, col=1)
    fig.update_yaxes(title_text="每笔 net %", row=2, col=1)
    fig.update_yaxes(title_text="累计 pp", row=3, col=1)
    fig.update_yaxes(title_text="pp", row=4, col=1)

    out_html = f"{config.DIR_BT}/detail_chart_{cfg}.html"
    fig.write_html(out_html, include_plotlyjs="inline", full_html=True)
    print(f"图表 HTML: {out_html}")


if __name__ == "__main__":
    for cfg in CFGS:
        df, out = build_detail(cfg)
        out.to_csv(f"{config.DIR_BT}/detail_{cfg}.csv", index=False)
        print(f"明细 CSV: detail_{cfg}.csv  笔数={len(out)}")
    df_main, _ = build_detail(CHART_CFG)
    build_chart(CHART_CFG, df_main)
    print("完成。")
