# -*- coding: utf-8 -*-
"""通用 CVD 背离绘图脚本(任意币种 / 任意近 N 天)。
复用 holo 版双状态+缩放+悬停逻辑, 参数化币种与时间周期, 删去硬编码代币。

用法(环境变量, 均有默认值):
    SYMBOL=TRUMPUSDT CVD_DAYS=31 CVD_W=12 python gen_cvd_kline.py
    SYMBOL=PUMPUSDT   CVD_DAYS=90 CVD_W=12 python gen_cvd_kline.py
    # 指定历史时刻"前后"窗口(北京时间): 中心±CVD_HALF_H小时
    SYMBOL=ZECUSDT CVD_CENTER="2026-08-22 10:00:00" CVD_HALF_H=12 python gen_cvd_kline.py

CVD口径复刻 backtest_short_top1.cvd_diverged:
    delta = 2*takerBuyQuoteVolume - quoteVolume
    value = 近W根delta之和 - 前W根delta之和 (W=12 → 60min, 即1h累计CVD)
    value > 0 → 买盘旺(OK, 橙▲)   value < 0 → 买盘衰竭(DIV背离, 蓝▼)
两种状态都标记; 支持滚轮/滑块缩放(dataZoom); 鼠标十字光标悬停显示开收高低/涨跌幅/taker买占比/value/状态。
"""
import os
import pymongo, json, datetime as dt
from datetime import timedelta, timezone

SYMBOL = os.environ.get("SYMBOL", "TRUMPUSDT").strip().upper()
CVD_DAYS = int(os.environ.get("CVD_DAYS", "31"))      # 近 N 天(从当前往前推)
CVD_W = int(os.environ.get("CVD_W", "12"))            # CVD_DIVERGE_W = 12 (12×5min = 60min)
CVD_CENTER = os.environ.get("CVD_CENTER", "").strip() # 中心时刻(北京时间), 如 "2026-08-22 10:00:00"; 指定则走"前后"窗口模式
CVD_HALF_H = float(os.environ.get("CVD_HALF_H", "12"))# 中心前后各 N 小时(默认12, 共24h窗口)
CVD_MODE = os.environ.get("CVD_MODE", "both").strip().lower()  # both=双状态 | short=只标卖盘背离(value<0, 回测开仓侧)
BJ = timezone(timedelta(hours=8))

warmup = timedelta(hours=3)  # 头部预热(算value需2W=24根历史)
if CVD_CENTER:
    center_bj = None
    for _fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            center_bj = dt.datetime.strptime(CVD_CENTER, _fmt).replace(tzinfo=BJ)
            break
        except ValueError:
            continue
    if center_bj is None:
        raise SystemExit("无法解析 CVD_CENTER=%r, 请用 'YYYY-MM-DD HH:MM:SS' (北京时间)" % CVD_CENTER)
    end_bj = center_bj + timedelta(hours=CVD_HALF_H)
    start_bj = center_bj - timedelta(hours=CVD_HALF_H)
else:
    end_bj = dt.datetime.now(BJ)
    start_bj = end_bj - timedelta(days=CVD_DAYS)
q_start = start_bj - warmup
start_ms = int(q_start.timestamp() * 1000)
end_ms = int(end_bj.timestamp() * 1000)

cli = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
col = cli["Workbuddy_5Min_Db"][SYMBOL + ".BINANCE"]
docs = list(col.find({"openTime": {"$gte": start_ms, "$lte": end_ms}},
                     {"openTime": 1, "open": 1, "high": 1, "low": 1, "close": 1,
                      "quoteVolume": 1, "takerBuyQuoteVolume": 1})
            .sort("openTime", 1))
print(f"拉取 {SYMBOL} 5m {len(docs)} 根 [{q_start} ~ {end_bj}] (CVD_W={CVD_W}, DAYS={CVD_DAYS})", flush=True)

# 计算 delta 序列
deltas = []
for d in docs:
    tbq = float(d.get("takerBuyQuoteVolume", 0.0) or 0.0)
    qv = float(d.get("quoteVolume", 0.0) or 0.0)
    deltas.append(2 * tbq - qv)

klines = []
for j, d in enumerate(docs):
    ot_ms = int(d["openTime"])
    # value: 需 j>=2W 才有前后各W根
    if j >= 2 * CVD_W:
        rec = sum(deltas[j - CVD_W:j])
        ref = sum(deltas[j - 2 * CVD_W:j - CVD_W])
        value = round((rec - ref) / 1e6, 4)   # 缩放(quote计价, 单位百万USD)
    else:
        value = None
    tbq = float(d.get("takerBuyQuoteVolume", 0.0) or 0.0)
    qv = float(d.get("quoteVolume", 0.0) or 0.0)
    ratio = round(tbq / qv, 4) if qv > 0 else None
    klines.append([ot_ms, round(float(d["open"]), 8), round(float(d["high"]), 8),
                   round(float(d["low"]), 8), round(float(d["close"]), 8), value, ratio])

print(f"  value有效 {sum(1 for k in klines if k[5] is not None)} 根, "
      f"DIV(<0) {sum(1 for k in klines if k[5] is not None and k[5] < 0)} 根, "
      f"OK(>0) {sum(1 for k in klines if k[5] is not None and k[5] > 0)} 根", flush=True)

# 标记指定时刻那根 K(仅 CVD_CENTER 模式): 找 openTime<=center_ms 的最近一根(即该5min块K)
MARK = None
if CVD_CENTER:
    center_ms = int(center_bj.timestamp() * 1000)
    _cand = [r for r in klines if r[0] <= center_ms]
    if _cand:
        _mk = max(_cand, key=lambda r: r[0])
        MARK = [_mk[0], _mk[4]]   # [openTime_ms, close]
data = {"klines": klines, "mark": MARK}
MARK_LEGEND = ' &nbsp;|&nbsp; <span style="color:#ffd54f">⚑ 黄菱形+竖线 标记中心K</span>' if CVD_CENTER else ''
# CVD_MODE: both=双状态 | short=只标value<0(卖盘增强/买盘衰竭, meme_short做空加仓侧) | long=只标value>0(卖盘衰减/吸筹, meme_long做多加仓侧)
if CVD_MODE == "short":
    SHOW_UP, SHOW_DOWN = False, True
    LEGEND_UP = '<span class="lg-dn">▼ 蓝三角 value&lt;0</span> 卖盘增强/买盘衰竭(DIV, 回测做空加仓侧)'
    MODE_TAG = "· CVD卖盘背离(short)"
elif CVD_MODE == "long":
    SHOW_UP, SHOW_DOWN = True, False
    LEGEND_UP = '<span class="lg-up">▲ 橙三角 value&gt;0</span> 卖盘衰减/吸筹(DIV, 回测做多加仓侧)'
    MODE_TAG = "· CVD卖盘衰减(long)"
else:  # both
    SHOW_UP, SHOW_DOWN = True, True
    LEGEND_UP = ('<span class="lg-up">▲ 橙三角 value&gt;0</span> 近1h买盘强于前1h(OK) &nbsp;|&nbsp; '
                 '<span class="lg-dn">▼ 蓝三角 value&lt;0</span> 买盘衰竭(DIV背离)')
    MODE_TAG = "· CVD背离双状态"
if CVD_CENTER:
    TITLE = f"{SYMBOL} {center_bj.strftime('%Y-%m-%d %H:%M')}前后{CVD_HALF_H:.0f}h {MODE_TAG}"
    WIN = f"{start_bj.strftime('%Y-%m-%d %H:%M')} ~ {end_bj.strftime('%Y-%m-%d %H:%M')} (中心 {center_bj.strftime('%Y-%m-%d %H:%M')})"
else:
    TITLE = f"{SYMBOL} 近{CVD_DAYS}天 5m {MODE_TAG}"
    WIN = f"{start_bj.strftime('%Y-%m-%d')} ~ {end_bj.strftime('%Y-%m-%d %H:%M')}"

HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>{title}</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
 body{{margin:0;background:#0f1419;color:#e6e6e6;font-family:-apple-system,'Segoe UI',sans-serif}}
 #top{{padding:10px 16px;display:flex;gap:12px;align-items:center;flex-wrap:wrap}}
 #chart{{width:100%;height:calc(100vh - 56px)}}
 .legend{{font-size:12px;color:#9fb3c8}}
 .lg-up{{color:#ff9800}} .lg-dn{{color:#42a5f5}}
</style>
</head>
<body>
<div id="top">
 <span class="legend">🟠 {sym} {win} &nbsp;|&nbsp; {legend_up} &nbsp;|&nbsp; 绿涨红跌 {markleg} &nbsp;|&nbsp; 滚轮缩放 / 滑块平移 / 十字光标悬停</span>
</div>
<div id="chart"></div>
<script>
const DATA = {datajson};
const chart = echarts.init(document.getElementById('chart'));
const UP='#26a69a', DOWN='#ef5350';
function fmtTime(ms){{const d=new Date(ms);const p=n=>String(n).padStart(2,'0');return `${{d.getFullYear()}}-${{p(d.getMonth()+1)}}-${{p(d.getDate())}} ${{p(d.getHours())}}:${{p(d.getMinutes())}}`;}}
const kl=DATA.klines;
const MARK=DATA.mark;
const SHOW_UP={show_up};
const SHOW_DOWN={show_down};
const candle=kl.map(r=>[r[0],r[1],r[4],r[3],r[2],r[5],r[6]]);  // ECharts candlestick: [t,open,close,low,high] + value/ratio
const up=SHOW_UP ? kl.filter(r=>r[5]!==null && r[5]>0).map(r=>[r[0], r[4]*1.004, r[5]]) : [];
const down=SHOW_DOWN ? kl.filter(r=>r[5]!==null && r[5]<0).map(r=>[r[0], r[4]*1.004, r[5]]) : [];
const opt={{
  backgroundColor:'#0f1419', animation:false,
  tooltip:{{trigger:'axis', axisPointer:{{type:'cross',label:{{backgroundColor:'#2d3a48'}}}},
    backgroundColor:'#1c2530', borderColor:'#2d3a48', textStyle:{{color:'#e6e6e6'}},
    formatter:function(ps){{
      const c=ps.find(p=>p.seriesType==='candlestick'); if(!c) return '';
      const d=c.data; const v=d[5]; const ratio=d[6];
      const chg=((d[2]-d[1])/d[1]*100);
      const div = v===null?'N/A':(v<0?'<span style="color:#42a5f5">DIV(买盘衰竭) True</span>':'<span style="color:#ff9800">OK(买盘旺) False</span>');
      return `<b>${{fmtTime(d[0])}}</b><br/>开 ${{d[1]}}　收 ${{d[2]}} (${{chg>=0?'+':''}}${{chg.toFixed(2)}}%)<br/>高 ${{d[4]}}　低 ${{d[3]}}<br/>taker买占比 ${{ratio}}<br/>value(近{W}−前{W}, 百万USD) = <b>${{v}}</b><br/>${{div}}`;
    }}}},
  grid:{{left:64,right:30,top:18,bottom:82}},
  xAxis:{{type:'time', axisLine:{{lineStyle:{{color:'#2d3a48'}}}}, axisLabel:{{color:'#9fb3c8'}}}},
  yAxis:{{scale:true, axisLine:{{lineStyle:{{color:'#2d3a48'}}}}, axisLabel:{{color:'#9fb3c8'}}, splitLine:{{lineStyle:{{color:'#161e27'}}}}}},
  dataZoom:[{{type:'inside', start:0, end:100}},{{type:'slider', start:0, end:100, height:20, bottom:18, borderColor:'#2d3a48', textStyle:{{color:'#9fb3c8'}}}}],
  series:[
    {{name:'K', type:'candlestick', data:candle,
     itemStyle:{{color:UP, color0:DOWN, borderColor:UP, borderColor0:DOWN}},
     markLine: MARK ? {{symbol:'none', lineStyle:{{color:'#ffd54f', type:'dashed', width:1.5}},
       label:{{show:true, color:'#ffd54f', position:'end', backgroundColor:'rgba(0,0,0,0.5)', padding:[2,4], formatter:'⚑ '+fmtTime(MARK[0])}},
       data:[{{xAxis: MARK[0]}}]}} : undefined}},
    {{name:'value>0', type:'scatter', data:up, symbol:'triangle', symbolSize:7, itemStyle:{{color:'#ff9800'}}}},
    {{name:'value<0', type:'scatter', data:down, symbol:'triangle', symbolRotate:180, symbolSize:7, itemStyle:{{color:'#42a5f5'}}}},
    ...(MARK ? [{{name:'标记K', type:'scatter', data:[[MARK[0], MARK[1]]], symbol:'diamond', symbolSize:16,
       itemStyle:{{color:'#ffd54f', borderColor:'#000', borderWidth:1}}, z:20,
       label:{{show:true, color:'#ffd54f', position:'top', formatter:fmtTime(MARK[0])}}}}] : [])
  ]
}};
chart.setOption(opt, true);
window.addEventListener('resize', ()=>chart.resize());
</script>
</body>
</html>""".format(title=TITLE, sym=SYMBOL, win=WIN, W=CVD_W, datajson=json.dumps(data, ensure_ascii=False), markleg=MARK_LEGEND, legend_up=LEGEND_UP, show_up=str(SHOW_UP).lower(), show_down=str(SHOW_DOWN).lower())

OUT_TAG = center_bj.strftime("%Y%m%d_%H%M") if CVD_CENTER else end_bj.strftime("%Y%m%d_%H%M")
OUT_SUFFIX = {"short": "_short", "long": "_long"}.get(CVD_MODE, "")
OUT = "data/cvd_%s_%s%s.html" % (SYMBOL.lower(), OUT_TAG, OUT_SUFFIX)
with open(OUT, "w", encoding="utf-8") as f:
    f.write(HTML)
print(f"已写出: {OUT}")
