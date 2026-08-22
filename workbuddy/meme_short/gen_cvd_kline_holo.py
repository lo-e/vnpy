# -*- coding: utf-8 -*-
"""绘制 HOLOUSDT 在指定中心时刻 ±24h 的5m K线 + CVD背离标记(ECharts)。
用法: python gen_cvd_kline_holo.py
口径复刻 backtest_short_top1.cvd_diverged: delta=2*takerBuyQuoteVolume-quoteVolume;
     value = 近12根delta之和 - 前12根delta之和; value<0 → 背离(DIV, 买盘衰竭, 可开)
"""
import sys
from datetime import datetime, timedelta, timezone
import pymongo
import json

SYM = "1MBABYDOGEUSDT"
CENTER = datetime(2024, 12, 7, 17, 10, 0, tzinfo=timezone(timedelta(hours=8)))  # 中心时刻(北京时间)
W = 12                       # CVD窗口(与CVD_DIVERGE_W=12一致)
HALF = timedelta(hours=24)   # ±24h
WARMUP = timedelta(hours=3)  # 头部预热(算value需2W=24根历史)

start = CENTER - HALF - WARMUP
end = CENTER + HALF
start_ms = int(start.timestamp() * 1000)
end_ms = int(end.timestamp() * 1000)

cli = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
col = cli["Workbuddy_5Min_Db"][SYM + ".BINANCE"]
docs = list(col.find({"openTime": {"$gte": start_ms, "$lte": end_ms}},
                     {"openTime": 1, "open": 1, "high": 1, "low": 1, "close": 1,
                      "quoteVolume": 1, "takerBuyQuoteVolume": 1})
            .sort("openTime", 1))
print(f"拉取 {SYM} K线 {len(docs)} 根 [{start} ~ {end}]", flush=True)

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
    if j >= 2 * W:
        rec = sum(deltas[j - W:j])
        ref = sum(deltas[j - 2 * W:j - W])
        value = round((rec - ref) / 1e6, 4)   # 缩放避免数值过大(quote计价, 单位百万)
    else:
        value = None
    tbq = float(d.get("takerBuyQuoteVolume", 0.0) or 0.0)
    qv = float(d.get("quoteVolume", 0.0) or 0.0)
    ratio = round(tbq / qv, 4) if qv > 0 else None
    klines.append([ot_ms, round(float(d["open"]), 8), round(float(d["high"]), 8),
                   round(float(d["low"]), 8), round(float(d["close"]), 8), value, ratio])

center_ms = int(CENTER.timestamp() * 1000)
data = {SYM: {"permitTime": center_ms,
              "permitStr": CENTER.strftime("%Y-%m-%d %H:%M:%S"),
              "klines": klines}}

HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>CVD K线 - {sym} {ctr}</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
 body{{margin:0;background:#0f1419;color:#e6e6e6;font-family:-apple-system,'Segoe UI',sans-serif}}
 #top{{padding:10px 16px;display:flex;gap:12px;align-items:center;flex-wrap:wrap}}
 #chart{{width:100%;height:calc(100vh - 56px)}}
 .legend{{font-size:12px;color:#9fb3c8}}
 .lg-up{{color:#ff9800}} .lg-dn{{color:#42a5f5}} .lg-line{{color:#ffd54f}}
</style>
</head>
<body>
<div id="top">
 <span class="legend">🟠 {sym} @ {ctr} ±24h &nbsp;|&nbsp; <span class="lg-up">▲ value&gt;0</span> 近12根买盘强于前12根(OK) &nbsp;|&nbsp; <span class="lg-dn">▼ value&lt;0</span> 买盘衰竭(DIV, 背离) &nbsp;|&nbsp; 红涨绿跌 &nbsp;|&nbsp; <span class="lg-line">┊ 中心时刻</span></span>
</div>
<div id="chart"></div>
<script>
const DATA = {datajson};
const chart = echarts.init(document.getElementById('chart'));
const UP='#ef5350', DOWN='#26a69a';
function fmtTime(ms){{const d=new Date(ms);const p=n=>String(n).padStart(2,'0');return `${{d.getFullYear()}}-${{p(d.getMonth()+1)}}-${{p(d.getDate())}} ${{p(d.getHours())}}:${{p(d.getMinutes())}}`;}}
const D=DATA["{sym}"]; const kl=D.klines;
const candle=kl.map(r=>[r[0],r[1],r[4],r[3],r[2],r[5],r[6]]);  // ECharts candlestick 顺序: [time,open,close,low,high]
const up=kl.filter(r=>r[5]!==null && r[5]>0).map(r=>[r[0], r[4]*1.004, r[5]]);
const down=kl.filter(r=>r[5]!==null && r[5]<0).map(r=>[r[0], r[4]*1.004, r[5]]);
const opt={{
  backgroundColor:'#0f1419', animation:false,
  tooltip:{{trigger:'axis', axisPointer:{{type:'cross',label:{{backgroundColor:'#2d3a48'}}}},
    backgroundColor:'#1c2530', borderColor:'#2d3a48', textStyle:{{color:'#e6e6e6'}},
    formatter:function(ps){{
      const c=ps.find(p=>p.seriesType==='candlestick'); if(!c) return '';
      const d=c.data; const v=d[5]; const ratio=d[6];
      const chg=((d[2]-d[1])/d[1]*100);
      const div = v===null?'N/A':(v<0?'<span style="color:#42a5f5">DIV(买盘衰竭) True</span>':'<span style="color:#ff9800">OK(买盘旺) False</span>');
      return `<b>${{fmtTime(d[0])}}</b><br/>开 ${{d[1]}}　收 ${{d[2]}} (${{chg>=0?'+':''}}${{chg.toFixed(2)}}%)<br/>高 ${{d[4]}}　低 ${{d[3]}}<br/>taker买占比 ${{ratio}}<br/>value(近12−前12) = <b>${{v}}</b><br/>${{div}}`;
    }}}},
  grid:{{left:64,right:30,top:18,bottom:82}},
  xAxis:{{type:'time', axisLine:{{lineStyle:{{color:'#2d3a48'}}}}, axisLabel:{{color:'#9fb3c8'}}}},
  yAxis:{{scale:true, axisLine:{{lineStyle:{{color:'#2d3a48'}}}}, axisLabel:{{color:'#9fb3c8'}}, splitLine:{{lineStyle:{{color:'#161e27'}}}}}},
  dataZoom:[{{type:'inside', start:2, end:100}},{{type:'slider', start:2, end:100, height:20, bottom:18, borderColor:'#2d3a48', textStyle:{{color:'#9fb3c8'}}}}],
  series:[
    {{name:'K', type:'candlestick', data:candle,
     itemStyle:{{color:UP, color0:DOWN, borderColor:UP, borderColor0:DOWN}},
     markLine:{{symbol:'none', silent:true, lineStyle:{{color:'#ffd54f', type:'dashed', width:1.5}},
       data:[{{xAxis:D.permitTime, label:{{formatter:'中心时刻', color:'#ffd54f', position:'insideEndTop'}}}}]}}}},
    {{name:'value>0', type:'scatter', data:up, symbol:'triangle', symbolSize:7, itemStyle:{{color:'#ff9800'}}}},
    {{name:'value<0', type:'scatter', data:down, symbol:'triangle', symbolRotate:180, symbolSize:7, itemStyle:{{color:'#42a5f5'}}}}
  ]
}};
chart.setOption(opt, true);
</script>
</body>
</html>""".format(sym=SYM, ctr=CENTER.strftime("%Y-%m-%d %H:%M:%S"), datajson=json.dumps(data, ensure_ascii=False))

OUT = "data/change_20251201_20260816/cvd_kline_%s_%s.html" % (SYM.lower(), CENTER.strftime("%Y%m%d_%H%M"))
with open(OUT, "w", encoding="utf-8") as f:
    f.write(HTML)
print(f"已写出: {OUT}  (K线 {len(klines)} 根, 其中 value!=null {sum(1 for k in klines if k[5] is not None)} 根)")
