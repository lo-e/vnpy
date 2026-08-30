"""S/R 指标演示图 —— 在 SNDKUSDT 1h 上叠加 TradingView 社区主流压力支撑指标。

全屏 + 滚轮缩放 + 时间到小时(北京时间)。复用于 07 的全屏 JS 外壳。
输出: data/sndk_sr_chart.html
"""
import numpy as np
import pandas as pd
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from lib import config, sr_indicators as sr

SYM = "SNDKUSDT"
PERIOD = 24  # 1h 数据 -> 24 根 = 1 天, pivot 按日块


def load():
    p = os.path.join(config.DATA, "1h", f"{SYM}.npz")
    if not os.path.exists(p):
        from lib import mongo_io
        arr = mongo_io.load_1h(SYM)
        np.savez(p, open_time=arr["open_time"], open=arr["o"], high=arr["h"],
                 low=arr["l"], close=arr["c"], volume=arr["v"], qvol=arr["qv"],
                 trades=arr["tr"], tbq=arr["tbq"])
    return np.load(p)


def main():
    d = load()
    ot, o, h, l, c, qv = (d["open_time"], d["open"], d["high"],
                          d["low"], d["close"], d["qvol"])
    n = len(c)
    dt = pd.to_datetime(ot, unit="ms", utc=True) + pd.Timedelta(hours=8)  # 北京时间

    # ---- 指标 ----
    cla = sr.pivot_points_classic(h, l, c, PERIOD)
    fh, fl = sr.williams_fractal(h, l, 2, 2)
    sw = sr.swing_points(h, l, 10)
    dc24 = sr.donchian(h, l, 24)
    dc168 = sr.donchian(h, l, 168)
    vp = sr.volume_profile(c, qv, 120)

    # 最近一个完整日块的 classic pivot (最重要, 作为主水平线)
    lb_start = (n // PERIOD - 1) * PERIOD
    lb_end = min(n, (n // PERIOD) * PERIOD)
    block_date = dt[lb_start].strftime("%Y-%m-%d")
    px = {k: cla[k][lb_start] for k in ["pivot", "r1", "r2", "r3", "s1", "s2", "s3"]}

    # y 轴范围(主图 & VP 对齐)
    ylo, yhi = float(np.nanmin(l)) * 0.97, float(np.nanmax(h)) * 1.03

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.62, 0.13, 0.25], vertical_spacing=0.03,
        subplot_titles=(f"{SYM} 1h · 压力支撑指标叠加", "成交量(qUSD)", "Volume Profile (成交密集区)"),
    )

    # ===== 主图: K线 =====
    fig.add_trace(go.Candlestick(
        x=dt, open=o, high=h, low=l, close=c, name="K线",
        increasing_line_color="rgb(14,203,129)", decreasing_line_color="rgb(246,70,93)",
        increasing_fillcolor="rgba(14,203,129,0.7)", decreasing_fillcolor="rgba(246,70,93,0.7)",
    ), row=1, col=1)

    # Donchian 24 / 168
    fig.add_trace(go.Scatter(x=dt, y=dc24["upper"], name="Donchian24 压力",
                             line=dict(color="rgba(255,140,90,0.8)", width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=dt, y=dc24["lower"], name="Donchian24 支撑",
                             line=dict(color="rgba(90,200,140,0.8)", width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=dt, y=dc168["upper"], name="Donchian7d 压力",
                             line=dict(color="rgba(255,180,60,0.6)", width=1.4, dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=dt, y=dc168["lower"], name="Donchian7d 支撑",
                             line=dict(color="rgba(60,200,200,0.6)", width=1.4, dash="dot")), row=1, col=1)

    # Fractal 点
    fig.add_trace(go.Scatter(x=dt[fh], y=h[fh], name="Fractal 高点", mode="markers",
                             marker=dict(symbol="triangle-up", size=7, color="rgb(255,90,90)")), row=1, col=1)
    fig.add_trace(go.Scatter(x=dt[fl], y=l[fl], name="Fractal 低点", mode="markers",
                             marker=dict(symbol="triangle-down", size=7, color="rgb(90,200,140)")), row=1, col=1)

    # Swing 摆动点
    fig.add_trace(go.Scatter(x=dt[sw["sh"]], y=sw["sh_px"][sw["sh"]], name="Swing 高点",
                             mode="markers", marker=dict(symbol="circle-open", size=9, color="rgb(255,160,60)")), row=1, col=1)
    fig.add_trace(go.Scatter(x=dt[sw["sl"]], y=sw["sl_px"][sw["sl"]], name="Swing 低点",
                             mode="markers", marker=dict(symbol="circle-open", size=9, color="rgb(60,200,200)")), row=1, col=1)

    # Classic Pivot 最近日块水平线
    xline = [dt[0], dt[-1]]
    plines = [
        ("r3", px["r3"], "rgba(255,90,90,0.95)", "R3"),
        ("r2", px["r2"], "rgba(255,120,120,0.9)", "R2"),
        ("r1", px["r1"], "rgba(255,150,150,0.85)", "R1"),
        ("s1", px["s1"], "rgba(120,200,150,0.85)", "S1"),
        ("s2", px["s2"], "rgba(90,200,140,0.9)", "S2"),
        ("s3", px["s3"], "rgba(60,200,120,0.95)", "S3"),
    ]
    for key, val, col, lab in plines:
        fig.add_trace(go.Scatter(x=xline, y=[val, val], name=f"{lab} {block_date}",
                                 mode="lines", line=dict(color=col, width=1.2, dash="dash"),
                                 hovertemplate=f"{lab}={val:.4f}<extra></extra>"), row=1, col=1)

    # ===== 副图1: 成交量 =====
    fig.add_trace(go.Bar(x=dt, y=qv, name="成交量", marker_color="rgba(120,140,180,0.6)"), row=2, col=1)

    # ===== 副图2: Volume Profile (横向, y 与主图价格对齐) =====
    fig.add_trace(go.Bar(
        x=vp["vol"], y=vp["bin_centers"], orientation="h", name="Volume Profile",
        marker_color="rgba(150,170,210,0.55)",
        hovertemplate="价=%{y:.4f}<br>量=%{x:.0f}<extra></extra>",
    ), row=3, col=1)
    # POC / VAH / VAL 标注线
    for val, lab, col in [(vp["poc"], f"POC {vp['poc']:.2f}", "rgb(255,210,80)"),
                          (vp["vah"], f"VAH {vp['vah']:.2f}", "rgb(255,120,120)"),
                          (vp["val"], f"VAL {vp['val']:.2f}", "rgb(120,220,160)")]:
        fig.add_trace(go.Scatter(x=[0, float(vp["vol"].max())], y=[val, val],
                                 mode="lines", name=lab, line=dict(color=col, width=1.5, dash="dot"),
                                 hoverinfo="skip"), row=3, col=1)

    # ===== 布局 =====
    header = (f"<b>{SYM} 全周期 1h · 压力支撑指标</b>  |  "
              f"{dt[0].strftime('%Y-%m-%d')} ~ {dt[-1].strftime('%Y-%m-%d')} ({n} 根)<br>"
              f"VP POC={vp['poc']:.2f}  VAH={vp['vah']:.2f}  VAL={vp['val']:.2f}  |  "
              f"Fractal 高/低={int(fh.sum())}/{int(fl.sum())}  Swing 高/低={int(sw['sh'].sum())}/{int(sw['sl'].sum())}  |  "
              f"日块 Classic Pivot({block_date}): P={px['pivot']:.2f}  R1={px['r1']:.2f}  S1={px['s1']:.2f}")
    fig.update_layout(
        title=header, template="plotly_dark", showlegend=True,
        legend=dict(orientation="h", y=1.04, x=0, font=dict(size=10)),
        hovermode="x unified", xaxis_rangeslider_visible=False,
        margin=dict(l=60, r=30, t=70, b=30),
    )
    fig.update_yaxes(row=1, col=1, title_text="价格", range=[ylo, yhi])
    fig.update_yaxes(row=2, col=1, title_text="qUSD")
    fig.update_yaxes(row=3, col=1, title_text="价格(对齐主图)", range=[ylo, yhi])
    fig.update_xaxes(row=1, col=1, tickformat="%Y-%m-%d %H:%M")
    fig.update_xaxes(row=2, col=1, tickformat="%Y-%m-%d %H:%M")
    fig.update_xaxes(row=3, col=1, tickformat="%Y-%m-%d %H:%M")

    # ===== 全屏 JS 外壳 =====
    plot_cfg = {
        "responsive": True, "scrollZoom": True, "displaylogo": False,
        "modeBarButtonsToRemove": ["lasso2d", "select2d"], "doubleClick": "reset",
    }
    div = fig.to_html(include_plotlyjs="inline", full_html=False, div_id="chart", config=plot_cfg)
    html = f"""<!DOCTYPE html>
<html lang="zh">
<head><meta charset="utf-8"><title>{SYM} S/R 指标（全屏）</title>
<style>
  html,body{{margin:0;padding:0;height:100%;width:100%;background:#0d1117;overflow:hidden;}}
  #chart{{width:100%;height:100%;}}
</style></head>
<body>{div}
<script>
(function(){{function fit(){{var el=document.getElementById('chart');
  if(el&&window.Plotly){{Plotly.relayout(el,{{'width':window.innerWidth,'height':window.innerHeight}});}}}}
  window.addEventListener('resize',fit); window.addEventListener('load',function(){{setTimeout(fit,80);}});
  var t=setInterval(function(){{var el=document.getElementById('chart'); if(el&&window.Plotly&&el.data){{fit();clearInterval(t);}}}},200);
}})();
</script>
</body></html>"""
    out = os.path.join(config.DATA, "sndk_sr_chart.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print("已写出(全屏版):", out)
    print(f"VP poc={vp['poc']:.2f} vah={vp['vah']:.2f} val={vp['val']:.2f} | "
          f"Fractal {int(fh.sum())}/{int(fl.sum())} | Swing {int(sw['sh'].sum())}/{int(sw['sl'].sum())}")


if __name__ == "__main__":
    main()
