"""05_plot.py —— 画回测净值曲线（offline plotly，缩放/平移/十字光标）。

- curve_best.html：主推荐稳健配置（默认 K4_THR15_H4_T6，ddR 最低、双正）
- curve_robust.html：样本外最均衡对比（默认 K4_THR20_H4_T6，h2 占比最高）
- curve_compare.html：所有稳健配置（双正 + ddR<0.15 + n≥1000）对比
- curve_top1.html：总收益最高配置（含警戒标记 h2 负）

顶部固定显示 maxDD 与 ddRatio（项目惯例）。配色：红=上涨/收益，绿=下跌/亏损（中文惯例）。
"""
import glob
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from lib import config

config.ensure_dirs()
pio.templates.default = "plotly_dark"

# 主推荐（综合 ddR 最低 + 双正 + 笔数较多）
BEST_KEY   = "tf_K4_THR15_H4_T6"
ROBUST_KEY = "tf_K4_THR20_H4_T6"
TOP1_KEY   = "tf_K12_THR10_H12_T15"   # 总收益最高(警戒：h2 负)


def load_trades(key):
    f = f"{config.DIR_BT}/{key}_trades.csv"
    return pd.read_csv(f)


def equity_curve_pp(df):
    df = df.sort_values("exit_ot")
    eq, peak = 0.0, 0.0
    max_dd = 0.0
    rows = []
    for _, r in df.iterrows():
        eq += r["net_pct"]
        peak = max(peak, eq)
        dd = peak - eq
        max_dd = max(max_dd, dd)
        rows.append((int(r["exit_ot"]), eq, dd))
    out = pd.DataFrame(rows, columns=["exit_ot", "equity", "dd"])
    total_ret = eq
    dd_ratio = max_dd / abs(total_ret) if total_ret != 0 else np.nan
    return out, total_ret, max_dd, dd_ratio


def split_pp(df):
    df = df.sort_values("exit_ot"); mid = len(df) // 2
    if mid == 0: return None
    h1 = df.iloc[:mid]["net_pct"]; h2 = df.iloc[mid:]["net_pct"]
    return {
        "h1_n": len(h1), "h1_sum": float(h1.sum()), "h1_win": float((h1>0).mean()*100),
        "h2_n": len(h2), "h2_sum": float(h2.sum()), "h2_win": float((h2>0).mean()*100),
    }


def plot_single(df, title, out_html):
    eq, total_ret, max_dd, dd_ratio = equity_curve_pp(df)
    split = split_pp(df)
    n = len(df)
    win = (df["net_pct"] > 0).mean() * 100
    median = float(df["net_pct"].median())
    mean = float(df["net_pct"].mean())

    header = (
        f"<b>{title}</b>  笔数={n}  胜率={win:.1f}%  "
        f"总收益={total_ret:+.0f}pp  均={mean:+.2f}pp  中位={median:+.2f}pp  "
        f"<span style='color:#FF2442'>maxDD={max_dd:.0f}pp</span>  "
        f"<span style='color:#FF2442'>ddR={dd_ratio:.3f}</span>  "
        f"前段={split['h1_sum']:+.0f}pp(n={split['h1_n']})  "
        f"<b>后段={split['h2_sum']:+.0f}pp(n={split['h2_n']})</b>"
    )

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.78, 0.22],
                        vertical_spacing=0.05)
    fig.add_trace(go.Scatter(
        x=eq["exit_ot"], y=eq["equity"],
        mode="lines+markers", name="累计 pp",
        line=dict(color="#FF2442", width=2),
        marker=dict(size=3),
        customdata=np.stack([eq["exit_ot"], eq["equity"].round(2),
                             eq["dd"].round(1)], axis=-1),
        hovertemplate="<b>%{customdata[0]}</b><br>累计 pp=%{customdata[1]}<br>回撤 pp=%{customdata[2]}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=eq["exit_ot"], y=eq["dd"],
        mode="lines", name="回撤 pp", line=dict(color="#00C853", width=1),
        fill="tozeroy", fillcolor="rgba(0,200,83,0.18)",
    ), row=2, col=1)
    fig.update_layout(
        title=dict(text=header, x=0.01, xanchor="left", y=0.99, font=dict(size=12)),
        hovermode="closest",
        template="plotly_dark", height=620,
        margin=dict(l=60, r=40, t=80, b=40),
        legend=dict(orientation="h", y=1.07, x=1, xanchor="right"),
    )
    fig.update_xaxes(title_text="exit 时间", row=2, col=1)
    fig.update_yaxes(title_text="累计收益 (pp)", row=1, col=1, zeroline=False)
    fig.update_yaxes(title_text="回撤 (pp)", row=2, col=1, zeroline=False)
    fig.write_html(out_html, include_plotlyjs="inline", full_html=True)
    print(f"saved: {out_html}")


def plot_compare(configs, out_html, title):
    """多配置叠加对比：用 '每配置独立起始基线 0pp' 的 cumPnl，避免起点不一致。"""
    fig = go.Figure()
    palette = ["#FF2442", "#FFB300", "#00C853", "#2196F3", "#9C27B0",
               "#FF5722", "#00BCD4", "#FFC107", "#E91E63"]
    for i, c in enumerate(configs):
        try:
            df = load_trades(c)
        except FileNotFoundError:
            continue
        eq, total, mdd, ddr = equity_curve_pp(df)
        col = palette[i % len(palette)]
        fig.add_trace(go.Scatter(
            x=eq["exit_ot"], y=eq["equity"],
            mode="lines", name=f"{c} | 累计={total:+.0f}pp ddR={ddr:.2f}",
            line=dict(width=1.4, color=col),
        ))
    title_text = f"<b>{title}</b>  起点全部归零（pp 累积）—— 走势相对位置即策略相对强弱"
    fig.update_layout(
        title=dict(text=title_text, x=0.01, xanchor="left", y=0.99, font=dict(size=12)),
        xaxis_title="exit 时间 (ms)", yaxis_title="累计 pp",
        hovermode="closest", template="plotly_dark", height=560,
        legend=dict(orientation="h", y=-0.18, x=0, xanchor="left"),
    )
    fig.write_html(out_html, include_plotlyjs="inline", full_html=True)
    print(f"saved: {out_html}")


if __name__ == "__main__":
    # 主推荐（双正 + ddR 最低 + 笔数较多）
    try:
        df = load_trades(BEST_KEY)
        plot_single(df, "主推荐: tf_K4_THR15_H4_T6 (4h 累计 ≥15% 顺势 + 跟踪 6%)",
                    f"{config.DIR_BT}/curve_best.html")
    except FileNotFoundError:
        print(f"missing: {BEST_KEY}_trades.csv")

    # 最均衡稳健（h2 占比最高）
    try:
        df = load_trades(ROBUST_KEY)
        plot_single(df, "最均衡: tf_K4_THR20_H4_T6 (4h 累计 ≥20% + 跟踪 6%, h2/h1≈52%)",
                    f"{config.DIR_BT}/curve_robust.html")
    except FileNotFoundError:
        print(f"missing: {ROBUST_KEY}_trades.csv")

    # 总收益最高（警戒）
    try:
        df = load_trades(TOP1_KEY)
        plot_single(df, "总收益最高: tf_K12_THR10_H12_T15（⚠ 后段 h2 为负，非稳健）",
                    f"{config.DIR_BT}/curve_top1.html")
    except FileNotFoundError:
        print(f"missing: {TOP1_KEY}_trades.csv")

    # 稳健配置对比：双正 + ddR<0.15 + n≥1000，按总收益排
    summary = pd.read_csv(f"{config.DIR_BT}/tf_summary.csv")
    robust = summary[(summary["h1_sum"] > 0) & (summary["h2_sum"] > 0)
                     & (summary["dd_ratio"] < 0.15) & (summary["n_trades"] >= 1000)]
    robust = robust.sort_values("total_pct", ascending=False)
    keys = [f"tf_{c}" for c in robust["config"].tolist()]
    plot_compare(keys, f"{config.DIR_BT}/curve_compare.html",
                 f"稳健配置对比（双正 + ddR<0.15 + n≥1000，按总收益排序）")
    print(f"\n入选稳健配置 {len(keys)} 个: {keys}")
