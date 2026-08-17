"""ESPORTSUSDT 5m 反转指标信号可视化(07-15 ~ 07-20, 北京)。
reversal_score: 逐根 K 统计 bull/bear 指标共振计数, 阈值可调(默认>=3)。
输出 HTML: K线 + 信号标记 + 悬停显示时间/指标明细。
"""
import json
from datetime import datetime, timezone, timedelta

import reversal_indicators as ri  # 项目内指标(workbuddy/reversal_indicators.py), 不依赖外部项目

LOCAL = timezone(timedelta(hours=8))
import pymongo
cli = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
kc = cli["Workbuddy_5Min_Db"]["ESPORTSUSDT.BINANCE"]

# 计算区间: 07-12 起(预热) ~ 07-20 23:55; 展示区间: 07-15 00:00 起
CALC_START = int(datetime(2026, 7, 12, 0, 0, 0, tzinfo=LOCAL).timestamp() * 1000)
CALC_END = int(datetime(2026, 7, 20, 23, 55, 0, tzinfo=LOCAL).timestamp() * 1000)
SHOW_START = int(datetime(2026, 7, 15, 0, 0, 0, tzinfo=LOCAL).timestamp() * 1000)

docs = list(kc.find({"openTime": {"$gte": CALC_START, "$lte": CALC_END}}).sort("openTime", 1))
print(f"读取 K: {len(docs)} 根")

opens = [d["open"] for d in docs]
highs = [d["high"] for d in docs]
lows = [d["low"] for d in docs]
closes = [d["close"] for d in docs]
vols = [d["volume"] for d in docs]

scores = ri.reversal_score(opens, highs, lows, closes, vols)

# 组装展示数据(仅 SHOW_START 起)
pts = []
n_bull = n_bear = 0
for i, d in enumerate(docs):
    if d["openTime"] < SHOW_START:
        continue
    s = scores[i]
    bull, bear = s["bull"], s["bear"]
    if bull >= 3:
        n_bull += 1
    if bear >= 3:
        n_bear += 1
    pts.append({
        "t": datetime.fromtimestamp(d["openTime"] / 1000, tz=LOCAL).strftime("%Y-%m-%d %H:%M"),
        "o": d["open"], "h": d["high"], "l": d["low"], "c": d["close"], "v": d["volume"],
        "bull": bull, "bear": bear, "sig": s["signals"],
    })
print(f"展示点: {len(pts)} 根 | bull>=3: {n_bull} 根 | bear>=3: {n_bear} 根")

data_js = json.dumps(pts, ensure_ascii=False)
OUT = "workbuddy/data/esports_reversal_0715_0720.html"

html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>ESPORTSUSDT 5m 反转信号 · 07-15 ~ 07-20</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
  html,body{{margin:0;padding:0;height:100%;background:#0d1117;font-family:"Segoe UI",system-ui,sans-serif}}
  #chart{{width:100%;height:100%}}
  .ctrl{{position:fixed;top:14px;left:50%;transform:translateX(-50%);z-index:10;display:flex;align-items:center;gap:14px;
        background:rgba(22,27,34,.92);border:1px solid #30363d;border-radius:10px;padding:8px 18px;color:#c9d1d9;font-size:13px;backdrop-filter:blur(4px)}}
  .ctrl label{{color:#8b949e}}
  .ctrl input[type=range]{{width:120px;accent-color:#58a6ff}}
  .ctrl b{{color:#58a6ff;min-width:22px;text-align:center}}
  .tag{{padding:2px 8px;border-radius:6px;font-size:12px}}
  .tag.g{{background:rgba(63,185,80,.15);color:#3fb950}}
  .tag.r{{background:rgba(248,81,73,.15);color:#f85149}}
</style>
</head>
<body>
<div class="ctrl">
  <span style="color:#e6edf3;font-weight:600">ESPORTSUSDT 5m</span>
  <span style="color:#8b949e">反转信号阈值(reversal_score ≥)</span>
  <input type="range" id="th" min="1" max="8" step="1" value="3">
  <b id="thv">3</b>
  <span class="tag g" id="cntB">bull 0</span>
  <span class="tag r" id="cntR">bear 0</span>
</div>
<div id="chart"></div>
<script>
var pts = {data_js};
var n = pts.length;
var chart = echarts.init(document.getElementById('chart'), 'dark');
var priceScale = 1;
function range(){{
  var hs = pts.map(p=>p.h), ls = pts.map(p=>p.l);
  var h = Math.max.apply(null,hs), l = Math.min.apply(null,ls);
  return (h-l)*0.03;
}}
var pad = range();

function render(th){{
  var kdata = pts.map(p=>[p.o, p.c, p.l, p.h]);
  var bmarks = [], rmarks = [], bdet = [], rdet = [];
  for (var i=0;i<n;i++){{
    var p = pts[i];
    if (p.bull >= th){{ bmarks.push([i, p.l - pad]); bdet.push(p); }}
    if (p.bear >= th){{ rmarks.push([i, p.h + pad]); rdet.push(p); }}
  }}
  document.getElementById('cntB').textContent = 'bull ' + bdet.length;
  document.getElementById('cntR').textContent = 'bear ' + rdet.length;
  chart.setOption({{
    tooltip:{{
      trigger:'axis', confine:true, backgroundColor:'rgba(22,27,34,.95)', borderColor:'#30363d',
      textStyle:{{color:'#c9d1d9',fontSize:12}},
      formatter:function(ps){{
        var p = pts[ps[0].dataIndex];
        var s = '<b>'+p.t+'</b><br>O:'+p.o+' H:'+p.h+' L:'+p.l+' C:'+p.c+'<br>' +
          '量: '+p.v+'<br>' +
          'bull得分: <span style="color:#3fb950">'+p.bull+'</span> | bear得分: <span style="color:#f85149">'+p.bear+'</span>';
        if (p.sig.length) s += '<br>信号: ' + p.sig.join(' · ');
        return s;
      }}
    }},
    grid:{{left:70,right:30,top:60,bottom:70}},
    xAxis:{{
      type:'category', data:pts.map(p=>p.t), boundaryGap:true,
      axisLabel:{{color:'#8b949e',fontSize:11,formatter:function(v){{return v.slice(5,16)}}}},
      axisLine:{{lineStyle:{{color:'#30363d'}}}}, axisTick:{{show:false}}
    }},
    yAxis:{{
      type:'value', scale:true,
      axisLabel:{{color:'#8b949e',formatter:'{{value}}'}},
      splitLine:{{lineStyle:{{color:'#21262d'}}}}
    }},
    dataZoom:[
      {{type:'inside', xAxisIndex:0, zoomOnMouseWheel:true, moveOnMouseMove:true, moveOnMouseWheel:false, start:0, end:100}},
      {{type:'slider', xAxisIndex:0, bottom:15, height:20, borderColor:'#30363d', backgroundColor:'#161b22',
        fillerColor:'rgba(56,139,253,.25)', textStyle:{{color:'#8b949e'}},
        start:0, end:100}}
    ],
    series:[
      {{
        name:'K线', type:'candlestick', data:kdata,
        itemStyle:{{color:'#3fb950',color0:'#f85149',borderColor:'#3fb950',borderColor0:'#f85149'}},
        barWidth:'70%'
      }},
      {{
        name:'做多反转', type:'scatter', data:bmarks, symbol:'triangle', symbolSize:12,
        itemStyle:{{color:'#3fb950'}}, z:10,
        label:{{show:true,position:'bottom',color:'#3fb950',fontSize:10,formatter:'▲'}}
      }},
      {{
        name:'做空反转', type:'scatter', data:rmarks, symbol:'triangle', symbolSize:12,
        itemStyle:{{color:'#f85149'}}, z:10,
        label:{{show:true,position:'top',color:'#f85149',fontSize:10,formatter:'▼'}}
      }}
    ]
  }}, true);
}}

var th = document.getElementById('th');
th.oninput = function(){{
  document.getElementById('thv').textContent = th.value;
  render(parseInt(th.value));
}};
render(parseInt(th.value));
window.addEventListener('resize', function(){{ chart.resize(); }});
</script>
</body>
</html>"""

with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)
print(f"生成 {OUT} | {len(pts)} 根K | 文件 {len(html)/1024:.0f}KB")
