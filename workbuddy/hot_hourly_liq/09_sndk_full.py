"""09_sndk_full.py —— SNDKUSDT 纯 K线全周期（5m/1h 切换，绿涨红跌，无任何指标）。

数据：本地 MongoDB 币安 5m（Workbuddy_5Min_Db / SNDKUSDT.BINANCE），聚合为 1h。
      默认渲染 1h(约3327根, 顺滑)，默认视图只显示最近 200 根(不挤，纵轴占满高度)；
      无 5m/1h 切换(已取消，减轻加载负担)。
配色：绿涨红跌 EXPLICIT 锁死（不依赖主题默认，避免回退成蓝灰）。
      涨 = rgb(14,203,129)  跌 = rgb(246,70,93)（Binance/TradingView 风格）。
交互：全屏 + 滚轮横向缩放(时间) + 左键拖动平移(dragmode=pan, 默认可拖) + 悬停显示 OHLCV。
      右侧整条方向缩放条(向上拉升/向下缩短)。无任何副图/指标/成交量/压力支撑线。
输出：data/sndk_kline_full_chart.html
"""
import os
import math
import json
import pymongo
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from lib import config
from lib.sr_indicators import pivot_points_classic

config.ensure_dirs()
SYM = "SNDKUSDT"
COLL = "Workbuddy_5Min_Db"
GREEN = "rgb(14,203,129)"        # 涨 = 绿 (EXPLICIT)
RED = "rgb(246,70,93)"           # 跌 = 红 (EXPLICIT)
GREEN_FILL = "rgba(14,203,129,0.55)"
RED_FILL = "rgba(246,70,93,0.55)"

print("连接 MongoDB 拉取 SNDKUSDT 全部 5m K线 ...")
mc = pymongo.MongoClient("mongodb://127.0.0.1:27017/", serverSelectionTimeoutMS=8000)
coll = mc[COLL][f"{SYM}.BINANCE"]
docs = list(coll.find(
    {}, {"openTime": 1, "open": 1, "high": 1, "low": 1,
         "close": 1, "volume": 1, "quoteVolume": 1, "trades": 1}
).sort("openTime", 1))
assert docs, "SNDKUSDT 无 5m 数据"
raw = pd.DataFrame(docs)
raw["ts"] = pd.to_datetime(raw["openTime"], unit="ms", utc=True)
n5 = len(raw)
print(f"  原始 5m 根数={n5}  区间 {raw['ts'].iloc[0]} ~ {raw['ts'].iloc[-1]}")


def build(tf_min: int):
    if tf_min == 5:
        g = raw.copy(); g["bucket"] = g["ts"]
        agg = g
    else:
        g = raw.copy(); g["bucket"] = g["ts"].dt.floor("h")
        agg = g.groupby("bucket", as_index=False).agg(
            open=("open", "first"), high=("high", "max"), low=("low", "min"),
            close=("close", "last"), volume=("volume", "sum"),
            quoteVolume=("quoteVolume", "sum"), trades=("trades", "sum"),
        ).dropna(subset=["open", "high", "low", "close"]).sort_values("bucket")
    o = agg["open"].to_numpy(dtype="float64")
    h = agg["high"].to_numpy(dtype="float64")
    l = agg["low"].to_numpy(dtype="float64")
    c = agg["close"].to_numpy(dtype="float64")
    vb = agg["volume"].to_numpy(dtype="float64")
    vq = agg["quoteVolume"].to_numpy(dtype="float64")
    tc = agg["trades"].to_numpy(dtype="float64")
    # UTC -> 北京时间, 再去掉时区信息(naive)避免 Plotly 对 tz-aware 解析异常导致缩放消失
    dt = agg["bucket"].dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    dt = pd.Index(dt).to_pydatetime()   # 转回 python datetime(naive), 兼容 strftime 与 Plotly
    n = len(dt)
    hover = [f"时间 {dt[i].strftime('%Y-%m-%d %H:%M')}<br>开 {o[i]:.4f}<br>高 {h[i]:.4f}<br>"
             f"低 {l[i]:.4f}<br>收 {c[i]:.4f}<br>数量 {vb[i]:,.0f}<br>"
             f"成交额 {vq[i]:,.0f} USDT<br>笔数 {tc[i]:,.0f}" for i in range(n)]
    return dict(n=n, dt=[d.strftime("%Y-%m-%d %H:%M") for d in dt], dt_obj=dt,
                o=o.tolist(), h=h.tolist(), l=l.tolist(), c=c.tolist(), hover=hover,
                hi_all=float(h.max()), lo_all=float(l.min()),
                ret_total=float(c[-1] / c[0] - 1), cur=float(c[-1]))


print("聚合 1h ...")
D1 = build(60)
print(f"  1h 根数={D1['n']}")

# ---- Pivot Points (Classic, 日级=24根1h) 作为水平压力/支撑线 ----
# 用前一周期(前24根)的 H/L/C 算当前块的 pivot 系 -> 无未来函数
pv = pivot_points_classic(np.array(D1["h"]), np.array(D1["l"]), np.array(D1["c"]), period=24)
PV_KEYS = ["r3", "r2", "r1", "pivot", "s1", "s2", "s3"]


def make_candle(x, o, h, l, c, hover, name):
    return go.Candlestick(
        x=x, open=o, high=h, low=l, close=c, name=name,
        increasing_line_color=GREEN, decreasing_line_color=RED,
        increasing_fillcolor=GREEN_FILL, decreasing_fillcolor=RED_FILL,
        line=dict(width=1.3), text=hover, hoverinfo="text",
    )


def make_pivot_line(key, vals, color, dash, xidx):
    """水平压力/支撑线：vals 为对齐的价格数组(nan=无值处断开)，画成连续水平段。"""
    return go.Scatter(
        x=xidx, y=vals, name=f"PP {key.upper()}", mode="lines",
        line=dict(color=color, width=1.1, dash=dash),
        hoverinfo="skip", opacity=0.85,
    )


# 单图：纯 K线 + Pivot Points 水平压力支撑线
traces_1h = [make_candle(list(range(D1["n"])), D1["o"], D1["h"], D1["l"], D1["c"], D1["hover"], "1h K")]
xidx = list(range(D1["n"]))
# 颜色：压力(红系) / 支撑(绿系) / 轴(黄)，dash 区分档位
PV_STYLE = {
    "r3": ("rgba(246,70,93,0.9)", "dot"),
    "r2": ("rgba(246,70,93,0.7)", "dash"),
    "r1": ("rgba(246,70,93,0.5)", "dash"),
    "pivot": ("rgba(240,190,60,0.95)", "solid"),
    "s1": ("rgba(14,203,129,0.5)", "dash"),
    "s2": ("rgba(14,203,129,0.7)", "dash"),
    "s3": ("rgba(14,203,129,0.9)", "dot"),
}
for k in PV_KEYS:
    col, dash = PV_STYLE[k]
    traces_1h.append(make_pivot_line(k, pv[k].tolist(), col, dash, xidx))

fig = go.Figure()
for tr in traces_1h:
    fig.add_trace(tr)

header = (f"<b>{SYM} K线 + Pivot Points（绿涨红跌·全屏·缩放·拖动·悬停）</b>  |  "
          f"1h {D1['n']:,} 根  |  "
          f"区间高 {D1['hi_all']:.4f} / 低 {D1['lo_all']:.4f}  |  "
          f"累计 {D1['ret_total']*100:+.1f}%  |  当前 {D1['cur']:.4f}")
# 默认视图：只显示最近 DEFAULT_VIEW 根(避免全周期太挤)，其余靠滚轮/拖动看
DEFAULT_VIEW = 300
x0_1h = max(0, D1["n"] - DEFAULT_VIEW)
# 初始 Y 范围收紧到当前可见 300 根的极值附近(上下各留 17% padding)，K线约占 2/3 高度
vis_lo = min(D1["l"][x0_1h:])
vis_hi = max(D1["h"][x0_1h:])
pad = (vis_hi - vis_lo) * 0.17
y0, y1 = vis_lo - pad, vis_hi + pad
fig.update_layout(
    title=header, template="plotly_dark", autosize=True,
    margin=dict(l=60, r=46, t=100, b=40),
    xaxis_rangeslider_visible=False, dragmode="pan",   # 默认可拖动平移
    legend=dict(orientation="h", y=1.02, x=0), hovermode="x",
    xaxis=dict(title_text="K线序号 (悬停/刻度=北京时间 UTC+8)",
               range=[x0_1h, D1["n"] - 1]),
    yaxis=dict(range=[y0, y1]),
)
fig.update_yaxes(title_text="价格")

OUT = os.path.join(config.DATA, "sndk_kline_full_chart.html")
plot_cfg = {
    "responsive": True, "scrollZoom": False, "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d", "autoScale2d"], "doubleClick": "reset",
}
div = fig.to_html(include_plotlyjs="inline", full_html=False, div_id="chart", config=plot_cfg)

JS = f"""
<div id="yzoom" style="position:absolute;top:108px;bottom:44px;right:6px;z-index:10;
     display:flex;flex-direction:column;align-items:center;
     background:rgba(20,24,32,0.7);border:1px solid #444;border-radius:8px;padding:8px 4px;">
  <div style="color:#9aa4b2;font-size:11px;margin-bottom:4px;">价</div>
  <div id="ytrack" style="flex:1;width:22px;cursor:ns-resize;position:relative;
       background:#2a2f3a;border-radius:6px;overflow:hidden;"></div>
  <div style="color:#9aa4b2;font-size:11px;margin-top:4px;">↑缩 ↓拉</div>
</div>
<div id="crossH" style="position:absolute;height:0;border-top:1px dashed #9aa4b2;left:0;width:100%;z-index:9;pointer-events:none;display:none;"></div>
<div id="crossV" style="position:absolute;width:0;border-left:1px dashed #9aa4b2;top:0;height:100%;z-index:9;pointer-events:none;display:none;"></div>
<script>
var LO={D1['lo_all']:.6f}, HI={D1['hi_all']:.6f};   // 全周期绝对边界(仅用于 Y 缩放夹取)
// 北京时间字符串数组(已 +8h)，用于 X 轴刻度动态映射
var T1={json.dumps(D1['dt'])};
// X 轴是整数索引(第几根K线)，按当前 range 动态把刻度文本映射成北京时间
function updateTicks(el){{
  var xa=el.layout.xaxis; if(!xa||!xa.range) return;
  var T = T1;
  var lo=Math.max(0, Math.floor(xa.range[0]));
  var hi=Math.min(T.length-1, Math.ceil(xa.range[1]));
  if(hi<=lo) return;
  var N=8, vals=[], txt=[];
  for(var i=0;i<=N;i++){{
    var idx=Math.round(lo + (hi-lo)*i/N);
    idx=Math.max(0, Math.min(T.length-1, idx));
    vals.push(idx); txt.push(T[idx]);
  }}
  Plotly.relayout(el, {{'xaxis.tickmode':'array', 'xaxis.tickvals':vals, 'xaxis.ticktext':txt}});
}}
// Y 轴缩放条 = 纯方向增量控制器(不表示数值、无颜色、全局识别方向)：
//  - 整条轨道都是热区，按住任意位置上下拖动
//  - 向上拖(指针上移) => 拉升(缩小/拉长视野)
//  - 向下拖(指针下移) => 缩短(放大视野)
//  - 全区域只识别方向，无端点、无回头、可无限拉缩
//  - 用 rAF 合帧：pointermove 只累计位移，每帧最多 relayout 一次，避免卡顿
var Y_PER_PX=0.0028;    // 每像素对应的对数缩放量(连续，非离散档)
function yApplyZoom(factor){{
  var el=document.getElementById('chart');
  var ya=el.layout.yaxis; if(!ya||!ya.range) return;
  var c=(ya.range[0]+ya.range[1])/2, half=(ya.range[1]-ya.range[0])/2;
  var nh=half*factor;
  var span=HI-LO;
  nh=Math.max(span*0.0005, Math.min(span*2, nh));
  Plotly.relayout(el, {{'yaxis.range':[c-nh, c+nh]}});
}}
var ytrack=document.getElementById('ytrack');
var yDrag=false, yLastY=0, yAccum=0, yRAF=0;
function yStart(ev){{
  yDrag=true; yLastY=ev.clientY; yAccum=0;
  ytrack.setPointerCapture && ytrack.setPointerCapture(ev.pointerId);
  ev.preventDefault();
}}
function yMove(ev){{
  if(!yDrag) return;
  var dy = yLastY - ev.clientY;   // 上移 dy>0
  yLastY = ev.clientY;
  yAccum += dy;                    // 累计位移(像素)
  if(!yRAF) yRAF=requestAnimationFrame(yFlush);
  ev.preventDefault();
}}
function yFlush(){{
  yRAF=0;
  if(yAccum===0) return;
  // 连续系数：上移(yAccum>0)拉升(缩小/拉长视野)，下移缩短(放大)
  var factor=Math.exp(-yAccum*Y_PER_PX);
  yApplyZoom(factor);
  yAccum=0;
}}
function yEnd(ev){{
  if(!yDrag) return;
  yDrag=false;
  if(yRAF){{ cancelAnimationFrame(yRAF); yRAF=0; }}
  yFlush();
}}
ytrack.addEventListener('pointerdown', yStart);
window.addEventListener('pointermove', yMove);
window.addEventListener('pointerup', yEnd);
window.addEventListener('pointercancel', yEnd);
window.addEventListener('load', function(){{
  var el=document.getElementById('chart');
  // ---- 滚轮 = 只缩放 X 轴(时间)，以光标为锚；Y 轴不动(仿 TradingView) ----
  var gd=el;
  gd.addEventListener('wheel', function(ev){{
    ev.preventDefault();
    var ly=gd._fullLayout, xa=ly.xaxis;
    if(!xa || !xa.range) return;
    var r=xa.range, lo=r[0], hi=r[1], span=hi-lo;
    var bb=gd.getBoundingClientRect();
    var plotW = bb.width - (ly.margin.l + ly.margin.r);
    var relX = (ev.clientX - bb.left - ly.margin.l) / plotW;
    relX = Math.max(0, Math.min(1, relX));
    var anchor = lo + relX*span;
    var factor = ev.deltaY > 0 ? 1.15 : 1/1.15;
    var nlo = anchor - (anchor-lo)*factor;
    var nhi = anchor + (hi-anchor)*factor;
    Plotly.relayout(gd, {{'xaxis.range':[nlo, nhi]}});
    setTimeout(function(){{ updateTicks(gd); }}, 0);  // 缩放后重映射刻度
  }}, {{passive:false}});
  // 初始刻度映射(全周期)
  setTimeout(function(){{ updateTicks(gd); }}, 800);
  // ---- 自定义十字光标(横竖虚线，跟手，不依赖 Plotly spikeline) ----
  var ch=document.getElementById('crossH'), cv=document.getElementById('crossV');
  var cx=0, cy=0, cShown=false, cRAF=0;
  function cDraw(){{
    cRAF=0;
    ch.style.top=cy+'px'; cv.style.left=cx+'px';
  }}
  function cSched(){{
    if(!cRAF) cRAF=requestAnimationFrame(cDraw);
  }}
  gd.addEventListener('mousemove', function(ev){{
    cx=ev.clientX; cy=ev.clientY;
    if(!cShown){{ cShown=true; ch.style.display='block'; cv.style.display='block'; }}
    cSched();
  }});
  gd.addEventListener('mouseleave', function(){{
    cShown=false; ch.style.display='none'; cv.style.display='none';
  }});
}});
</script>
"""
html = f"""<!DOCTYPE html>
<html lang="zh">
<head><meta charset="utf-8"><title>{SYM} 纯K线全周期（绿涨红跌·5m/1h切换）</title>
<style>
  html,body{{margin:0;padding:0;height:100%;width:100%;background:#0d1117;overflow:hidden;}}
  #chart{{width:100%;height:100%;}}
</style></head>
<body>
{JS}
{div}
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
print("已写出(纯K线·绿涨红跌显式锁死·5m/1h切换·默认可拖·无指标无成交量):", OUT)
