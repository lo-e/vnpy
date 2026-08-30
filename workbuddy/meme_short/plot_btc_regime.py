# -*- coding: utf-8 -*-
"""plot_btc_regime.py — BTC 日线走势图 + regime(BULL/BEAR/SIDEWAYS) 分段着色。

主图: BTCUSDT 日级 K 线(取自 MongoDB 5m 库) 或回退 close 折线
副图: r200 年化斜率(读 regime_btc.csv) —— 直观展现斜率正负↔regime
背景: 按连续相同 regime 分段着色 (BULL绿/BEAR红/SIDEWAYS灰, 契合中文红跌绿涨)
输出: data/btc_regime_chart.html
"""
import os
import csv
import json
import pymongo

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "data", "regime_btc.csv")
OUT = os.path.join(HERE, "data", "btc_regime_chart.html")
URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")

# ---------- 1. 读 regime ----------
regimes = {}
r200_raw = {}
with open(CSV, encoding="utf-8-sig") as f:
    for r in csv.DictReader(f):
        regimes[r["date"]] = r["regime"]
        r200_raw[r["date"]] = r["r200_ann"]

# ---------- 2. 取 BTC 日级 OHLC ----------
mongo_map = {}
mode = "line"
try:
    cli = pymongo.MongoClient(URI, serverSelectionTimeoutMS=6000)
    col = cli["Workbuddy_5Min_Db"]["BTCUSDT.BINANCE"]
    pipe = [
        {"$group": {"_id": {"$dateToString": {"date": {"$toDate": "$openTime"},
                                                "format": "%Y-%m-%d", "timezone": "Asia/Shanghai"}},
                     "open": {"$first": "$open"}, "high": {"$max": "$high"},
                     "low": {"$min": "$low"}, "close": {"$last": "$close"}}},
        {"$sort": {"_id": 1}},
    ]
    for d in col.aggregate(pipe):
        # ECharts candlestick 顺序: [open, close, low, high]
        mongo_map[d["_id"]] = [d["open"], d["close"], d["low"], d["high"]]
    if mongo_map:
        mode = "kline"
    print(f"[ok] MongoDB 取日级 OHLC: {len(mongo_map)} 天", flush=True)
except Exception as e:
    print(f"[warn] MongoDB 不可达, 回退 close 折线: {e}", flush=True)
    mode = "line"

# ---------- 3. 对齐序列 ----------
dates, ohlc, r200series = [], [], []
with open(CSV, encoding="utf-8-sig") as f:
    for r in csv.DictReader(f):
        dt = r["date"]
        if mode == "kline" and dt in mongo_map:
            dates.append(dt)
            ohlc.append(mongo_map[dt])
        elif mode == "line":
            dates.append(dt)
            ohlc.append(float(r["close"]))
        else:
            continue
        rv = r200_raw.get(dt, "")
        r200series.append(None if rv in ("", "None", None) else float(rv))

# ---------- 4. 合并连续 regime 段 → markArea ----------
cmap = {"BULL": "rgba(38,166,154,0.18)",
        "BEAR": "rgba(239,83,80,0.18)",
        "SIDEWAYS": "rgba(140,144,150,0.14)"}
segs, cur, start, prev = [], None, None, None
for dt in dates:
    rg = regimes.get(dt, "SIDEWAYS")
    if rg != cur:
        if cur is not None:
            segs.append((start, prev, cur))
        cur, start = rg, dt
    prev = dt
if cur is not None:
    segs.append((start, prev, cur))
mark_area = [[{"xAxis": s[0]}, {"xAxis": s[1], "itemStyle": {"color": cmap[s[2]]}}] for s in segs]

# ---------- 5. 主图 series ----------
MAIN_K = ('{"type":"candlestick","data":__OHLC__,'
           '"itemStyle":{"color":"#ef5350","color0":"#26a69a",'
           '"borderColor":"#ef5350","borderColor0":"#26a69a"},'
           '"markArea":{"silent":true,"data":__MARKAREA__}}')
MAIN_LINE = ('{"type":"line","data":__OHLC__,"showSymbol":false,'
             '"lineStyle":{"color":"#26a69a","width":1.2},'
             '"areaStyle":{"color":"rgba(38,166,154,0.08)"},'
             '"markArea":{"silent":true,"data":__MARKAREA__}}')
main_series = (MAIN_K if mode == "kline" else MAIN_LINE)\
    .replace("__OHLC__", json.dumps(ohlc))\
    .replace("__MARKAREA__", json.dumps(mark_area))

R200_TMPL = ('{"type":"line","xAxisIndex":1,"yAxisIndex":1,"data":__R200__,'
             '"showSymbol":false,"lineStyle":{"color":"#f0a020","width":1},'
             '"markLine":{"silent":true,"symbol":"none","data":[{"yAxis":0}],'
             '"lineStyle":{"color":"#888","type":"dashed"}}}')
r200_series = R200_TMPL.replace("__R200__", json.dumps(r200series))

TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>BTC 日线 · Regime 分段</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
  html,body{margin:0;padding:0;height:100%;background:#0d1117;color:#c9d1d9;
    font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;}
  #hud{padding:10px 16px 0;font-size:13px;line-height:1.6;}
  .tag{display:inline-block;padding:1px 8px;border-radius:4px;margin-right:14px;font-weight:600;}
  .bull{background:rgba(38,166,154,.25);color:#26a69a;}
  .bear{background:rgba(239,83,80,.25);color:#ef5350;}
  .side{background:rgba(140,144,150,.25);color:#b0b4ba;}
  #chart{width:100%;height:78vh;}
</style>
</head>
<body>
<div id="hud">
  <b>BTC 日线走势 · 市场状态(regime)分段</b><br>
  <span class="tag bull">■ BULL 牛市(绿)</span>
  <span class="tag bear">■ BEAR 熊市(红)</span>
  <span class="tag side">■ SIDEWAYS 震荡(灰)</span>
  &nbsp;背景色=该段市场状态 · 副图= r200 年化斜率(红涨绿跌同义)
</div>
<div id="chart"></div>
<script>
var dates = __DATES__;
var chart = echarts.init(document.getElementById('chart'), 'dark');
var option = {
  backgroundColor:'#0d1117',
  tooltip:{trigger:'axis', axisPointer:{type:'cross'}},
  legend:{data:['BTC','r200年化'], top:4, textStyle:{color:'#c9d1d9'}},
  axisPointer:{link:[{xAxisIndex:'all'}]},
  grid:[{left:60,right:30,top:40,height:'62%'},
        {left:60,right:30,top:'74%',height:'16%'}],
  xAxis:[{type:'category',data:dates,gridIndex:0,axisLabel:{color:'#8b949e'},
          axisLine:{lineStyle:{color:'#30363d'}}},
         {type:'category',data:dates,gridIndex:1,axisLabel:{show:false},
          axisLine:{lineStyle:{color:'#30363d'}}}],
  yAxis:[{scale:true,gridIndex:0,name:'价格',axisLabel:{color:'#8b949e'},
          splitLine:{lineStyle:{color:'#21262d'}}},
         {gridIndex:1,name:'r200',axisLabel:{color:'#8b949e',formatter:'{value}'},
          splitLine:{lineStyle:{color:'#21262d'}}}],
  dataZoom:[{type:'inside',xAxisIndex:[0,1]},
            {type:'slider',xAxisIndex:[0,1],bottom:6,height:18}],
  series:[
    Object.assign(__MAIN__, {name:'BTC'}),
    Object.assign(__R200S__, {name:'r200年化'})
  ]
};
chart.setOption(option);
window.addEventListener('resize', function(){chart.resize();});
</script>
</body>
</html>"""

html = (TEMPLATE
        .replace("__DATES__", json.dumps(dates))
        .replace("__MAIN__", main_series)
        .replace("__R200S__", r200_series))

with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)
print(f"[done] 模式={mode} 天数={len(dates)} regime段={len(segs)} -> {OUT}", flush=True)
