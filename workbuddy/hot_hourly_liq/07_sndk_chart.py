"""07_sndk_chart.py —— SNDKUSDT 全周期 1h 走势 + 滚动压力/支撑位标记。

数据：本地 MongoDB 币安 5m -> 1h（不限 universe，专拉 SNDKUSDT 全周期）。
支撑/压力位（滚动计算）：
  1) Donchian 滚动通道：窗口内 high.max = 压力(红)，low.min = 支撑(绿)；短窗24h + 长窗7d
  2) Swing 摆动高低点（left=right=4）：▲红=摆动高点(压力位)，▼绿=摆动低点(支撑位)
附：成交量副图 + 24h滚动成交额(热度代理)副图。
输出：data/sndk_1h_chart.html （plotly 离线，可缩放/平移/十字光标）
"""
import os
import math
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from lib import config, mongo_io

config.ensure_dirs()
SYM = "SNDKUSDT"
END_MS = 1_788_000_000_000  # 覆盖到 2026-08-25，确保拉到最新一根

print("拉取 SNDKUSDT 全周期 1h ...")
arr = mongo_io.load_1h(SYM, start_ms=0, end_ms=END_MS)
assert arr is not None and len(arr["c"]) > 0, "SNDKUSDT 无数据"
ot, o, h, l, c, v = arr["open_time"], arr["o"], arr["h"], arr["l"], arr["c"], arr["qv"]
dt = pd.to_datetime(ot, unit="ms", utc=True)
n = len(c)
print(f"  1h 根数={n}  区间 {dt[0]} ~ {dt[-1]}")

# ---- 滚动压力/支撑：Donchian 通道 ----
W_S, W_L = 24, 168  # 24h / 7d
roll_hi_s = pd.Series(h).rolling(W_S, min_periods=W_S).max()
roll_lo_s = pd.Series(l).rolling(W_S, min_periods=W_S).min()
roll_hi_l = pd.Series(h).rolling(W_L, min_periods=W_L).max()
roll_lo_l = pd.Series(l).rolling(W_L, min_periods=W_L).min()

# ---- Swing 摆动高低点（left=right=4）----
def pivots(high, low, left=4, right=4):
    m = len(high)
    ph = np.full(m, np.nan)
    pl = np.full(m, np.nan)
    for i in range(left, m - right):
        if high[i] == np.max(high[i - left:i + right + 1]):
            ph[i] = high[i]
        if low[i] == np.min(low[i - left:i + right + 1]):
            pl[i] = low[i]
    return ph, pl
ph, pl = pivots(h, l, 4, 4)

# ---- 统计 ----
hi_all, lo_all = h.max(), l.min()
ret_total = c[-1] / c[0] - 1
logret = np.diff(np.log(c))
ann_vol = np.std(logret) * math.sqrt(24 * 365)
cur = c[-1]
sup_now = roll_lo_l.iloc[-1]
res_now = roll_hi_l.iloc[-1]
print(f"  累计 {ret_total*100:+.1f}%  年化波动 {ann_vol*100:.0f}%")
print(f"  当前 {cur:.4f}  7d支撑 {sup_now:.4f} ({(cur/sup_now-1)*100:+.1f}%)  "
      f"7d压力 {res_now:.4f} ({(cur/res_now-1)*100:+.1f}%)")

# ---- 图 ----
fig = make_subplots(
    rows=3, cols=1, shared_xaxes=True,
    row_heights=[0.60, 0.20, 0.20], vertical_spacing=0.04,
    subplot_titles=(f"{SYM} 1h K线 + 滚动压力/支撑 (Donchian)",
                   "成交量 (quote USD)", "24h滚动成交额 (热度代理)"),
)
# 主图：K线（绿涨红跌：涨=绿 跌=红，用户习惯，覆盖系统默认红涨绿跌）
fig.add_trace(go.Candlestick(
    x=dt, open=o, high=h, low=l, close=c, name="1h K",
    increasing_line_color="rgb(14,203,129)", decreasing_line_color="rgb(246,70,93)",
    increasing_fillcolor="rgba(14,203,129,0.28)", decreasing_fillcolor="rgba(246,70,93,0.24)",
), row=1, col=1)
# Donchian 长窗
fig.add_trace(go.Scatter(x=dt, y=roll_hi_l, name="压力(7d)",
                          line=dict(color="rgba(255,80,80,0.75)", width=1, dash="dot")), row=1, col=1)
fig.add_trace(go.Scatter(x=dt, y=roll_lo_l, name="支撑(7d)",
                          line=dict(color="rgba(80,200,120,0.75)", width=1, dash="dot")), row=1, col=1)
# Donchian 短窗
fig.add_trace(go.Scatter(x=dt, y=roll_hi_s, name="压力(24h)",
                          line=dict(color="rgba(255,150,150,0.45)", width=1, dash="dash")), row=1, col=1)
fig.add_trace(go.Scatter(x=dt, y=roll_lo_s, name="支撑(24h)",
                          line=dict(color="rgba(150,220,170,0.45)", width=1, dash="dash")), row=1, col=1)
# Swing 摆动高低点
mh, ml = ~np.isnan(ph), ~np.isnan(pl)
fig.add_trace(go.Scatter(x=dt[mh], y=ph[mh], mode="markers", name="摆动高点(压力位)",
                          marker=dict(symbol="triangle-down", size=8, color="rgb(255,80,80)")), row=1, col=1)
fig.add_trace(go.Scatter(x=dt[ml], y=pl[ml], mode="markers", name="摆动低点(支撑位)",
                          marker=dict(symbol="triangle-up", size=8, color="rgb(80,200,120)")), row=1, col=1)
# 成交量
fig.add_trace(go.Bar(x=dt, y=v, name="vol", marker_color="rgba(120,150,200,0.55)"), row=2, col=1)
# 24h 滚动成交额（热度）
vol24 = pd.Series(v).rolling(24, min_periods=1).sum()
fig.add_trace(go.Scatter(x=dt, y=vol24, name="vol24h",
                          line=dict(color="rgba(230,200,90,0.85)", width=1.2), fill="tozeroy",
                          fillcolor="rgba(230,200,90,0.12)"), row=3, col=1)

header = (f"<b>{SYM} 全周期 1h 走势图（全屏·可缩放·时间到小时）</b>  |  "
            f"{dt[0].strftime('%Y-%m-%d')} ~ {dt[-1].strftime('%Y-%m-%d')} "
            f"({n} 根)  |  区间高 {hi_all:.4f} / 低 {lo_all:.4f}  |  累计 {ret_total*100:+.1f}%  |  "
            f"年化波动 {ann_vol*100:.0f}%<br>"
            f"当前 {cur:.4f}  |  7d压力 {res_now:.4f} ({(cur/res_now-1)*100:+.1f}%)  |  "
            f"7d支撑 {sup_now:.4f} ({(cur/sup_now-1)*100:+.1f}%)")
# 全屏 + 响应式 + 滚轮缩放：autosize 让 div(CSS 100vw/100vh) 决定尺寸
fig.update_layout(
    title=header, template="plotly_dark", autosize=True,
    margin=dict(l=60, r=30, t=80, b=40),
    xaxis_rangeslider_visible=False,
    legend=dict(orientation="h", y=1.045, x=0), hovermode="x",
)
# x 轴 tick 与悬停都精确到「年-月-日 时:分」
fig.update_xaxes(tickformat="%Y-%m-%d %H:%M", hoverformat="%Y-%m-%d %H:%M")
fig.update_yaxes(title_text="价格", row=1, col=1)
fig.update_yaxes(title_text="quote", row=2, col=1)
fig.update_yaxes(title_text="USD", row=3, col=1)

OUT = os.path.join(config.DATA, "sndk_1h_chart.html")
plot_cfg = {
    "responsive": True,        # 窗口缩放时图表自适应（全屏关键）
    "scrollZoom": True,        # 滚轮缩放
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    "doubleClick": "reset",    # 双击复位
}
div = fig.to_html(include_plotlyjs="inline", full_html=False,
                  div_id="chart", config=plot_cfg)
# 双重保险：CSS 100% 后备 + JS 强制把图表尺寸绑定到真实窗口像素（破解嵌套预览 iframe 高度受限）
html = f"""<!DOCTYPE html>
<html lang="zh">
<head><meta charset="utf-8"><title>{SYM} 1h 走势（全屏）</title>
<style>
  html,body{{margin:0;padding:0;height:100%;width:100%;background:#0d1117;overflow:hidden;}}
  #chart{{width:100%;height:100%;}}
</style></head>
<body>{div}
<script>
(function(){{
  function fit(){{
    var el=document.getElementById('chart');
    if(!el || !window.Plotly) return;
    try{{ Plotly.relayout(el, {{height: window.innerHeight, width: window.innerWidth}}); }}catch(e){{}}
  }}
  window.addEventListener('resize', fit);
  function once(){{ fit(); if(!document.querySelector('#chart .plot-container')) setTimeout(once, 200); }}
  window.addEventListener('load', once);
}})();
</script>
</body></html>"""
with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)
print("已写出(全屏版):", OUT)
