"""从回测 CSV 生成累计盈亏曲线 HTML(ECharts: 缩放 + 悬停显示时间)。"""
import csv
import json

SRC = "data/backtest_short_top1_20260701_20260811.csv"
OUT = "data/backtest_cum_pnl.html"

rows = list(csv.DictReader(open(SRC, encoding="utf-8-sig")))
rows.sort(key=lambda r: r["openTime"])

# 折线数据 + 悬停明细
pts = []
for r in rows:
    pts.append({
        "t": r["openTime"],
        "cum": float(r["cumPnl"]),
        "pnl": float(r["pnlPct"]),
        "sym": r["symbol"],
        "ct": r["closeTime"],
        "reason": r["reason"],
    })

total = pts[-1]["cum"] if pts else 0
# 10% 固定仓位资金曲线(每笔盈亏×10%, 复利滚动) — 实盘口径
equity = 100.0
for p in pts:
    equity *= (1 + p["pnl"] / 100 * 0.1)
    p["eq"] = round(equity - 100, 4)
total_eq = pts[-1]["eq"] if pts else 0
n = len(pts)
wins = sum(1 for p in pts if p["pnl"] > 0)

data_js = json.dumps(pts, ensure_ascii=False)

html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>做空涨幅Top1 回测累计盈亏曲线</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
  html,body {{ margin:0; padding:0; height:100%; background:#0d1117; }}
  #chart {{ width:100%; height:100%; }}
  .hud {{
    position:fixed; top:16px; left:50%; transform:translateX(-50%);
    color:#c9d1d9; font:13px/1.6 "Segoe UI", system-ui, sans-serif;
    background:rgba(22,27,34,.85); border:1px solid #30363d; border-radius:8px;
    padding:8px 18px; text-align:center; z-index:10; backdrop-filter:blur(4px);
  }}
  .hud b {{ color:#58a6ff; }}
  .hud .red {{ color:#f85149; }} .hud .green {{ color:#3fb950; }}
</style>
</head>
<body>
<div class="hud">
  做空涨幅Top1 · 24h创30天新高 + reversal确认 + 盈利免禁仓 · 07-01 ~ 08-11<br>
  <b>{n}</b> 笔 · 胜率 <b>{wins/n*100:.1f}%</b><br>
  <span style="color:#8b949e">等额累计</span> <b class="{'green' if total>=0 else 'red'}">{total:+.2f}%</b>
  · <span style="color:#8b949e">10%仓位资金曲线</span> <b class="{'green' if total_eq>=0 else 'red'}">{total_eq:+.2f}%</b><br>
  <span style="color:#8b949e">滚轮/拖拽缩放 · 悬停查看时间</span>
</div>
<div id="chart"></div>
<script>
var pts = {data_js};
var times = pts.map(p => p.t);
var cum = pts.map(p => p.cum);
var eq = pts.map(p => p.eq);
var chart = echarts.init(document.getElementById('chart'), 'dark');
chart.setOption({{
  backgroundColor:'transparent',
  tooltip: {{
    trigger:'axis',
    confine:true,
    backgroundColor:'rgba(22,27,34,.95)',
    borderColor:'#30363d',
    textStyle:{{ color:'#c9d1d9', fontSize:12 }},
    formatter: function(ps) {{
      var p = pts[ps[0].dataIndex];
      return '<b>'+p.t+'</b><br>' +
        '合约: '+p.sym+'<br>' +
        '单笔盈亏: '+(p.pnl>=0?'<span style="color:#3fb950">+':'<span style="color:#f85149">')+p.pnl.toFixed(2)+'%</span> ('+p.reason+')<br>' +
        '等额累计: <b>'+p.cum.toFixed(2)+'%</b><br>' +
        '10%仓位资金曲线: <b>'+p.eq.toFixed(2)+'%</b><br>' +
        '平仓: '+p.ct;
    }}
  }},
  grid: {{ left:70, right:30, top:80, bottom:70 }},
  xAxis: {{
    type:'category', data:times, boundaryGap:false,
    axisLabel:{{ color:'#8b949e', fontSize:11, formatter:function(v){{ return v.slice(0,10); }} }},
    axisLine:{{ lineStyle:{{ color:'#30363d' }} }},
    axisTick:{{ show:false }}
  }},
  yAxis: {{
    type:'value', name:'累计盈亏 %', nameTextStyle:{{ color:'#8b949e' }},
    axisLabel:{{ color:'#8b949e', formatter:'{{value}}%' }},
    splitLine:{{ lineStyle:{{ color:'#21262d' }} }}
  }},
  dataZoom: [
    {{ type:'inside', xAxisIndex:0, zoomOnMouseWheel:true, moveOnMouseMove:true, moveOnMouseWheel:false }},
    {{ type:'slider', xAxisIndex:0, bottom:15, height:20, borderColor:'#30363d',
       backgroundColor:'#161b22', fillerColor:'rgba(56,139,253,.25)',
       textStyle:{{ color:'#8b949e' }} }}
  ],
  series: [
    {{
      name:'等额累计盈亏(每笔1份本金)', type:'line', data:cum, smooth:false, symbol:'circle', symbolSize:5,
      showSymbol:true,
      lineStyle:{{ width:2, color:'#58a6ff' }},
      itemStyle:{{ color:'#58a6ff' }},
      areaStyle:{{ color:new echarts.graphic.LinearGradient(0,0,0,1,[
        {{offset:0,color:'rgba(88,166,255,.35)'}},{{offset:1,color:'rgba(88,166,255,0)'}}]) }},
      markLine:{{
        silent:true, symbol:'none',
        lineStyle:{{ color:'#f85149', type:'dashed', width:1 }},
        label:{{ color:'#f85149', position:'insideEndTop', formatter:'0%' }},
        data:[{{ yAxis:0 }}]
      }},
      markPoint:{{
        data:[
          {{ type:'max', name:'最高', itemStyle:{{ color:'#3fb950' }}, label:{{ formatter:'峰值 {{c}}%' }} }},
          {{ type:'min', name:'最低', itemStyle:{{ color:'#f85149' }}, label:{{ formatter:'谷值 {{c}}%' }} }}
        ]
      }}
    }},
    {{
      name:'10%仓位资金曲线(复利)', type:'line', data:eq, smooth:false, symbol:'none',
      lineStyle:{{ width:2, color:'#3fb950' }},
      itemStyle:{{ color:'#3fb950' }},
      areaStyle:{{ color:new echarts.graphic.LinearGradient(0,0,0,1,[
        {{offset:0,color:'rgba(63,185,80,.25)'}},{{offset:1,color:'rgba(63,185,80,0)'}}]) }}
    }}
  ]
}});
window.addEventListener('resize', function(){{ chart.resize(); }});
</script>
</body>
</html>"""

with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)
print(f"生成 {OUT} | {n} 个数据点 | 末行累计 {total:+.2f}%")
