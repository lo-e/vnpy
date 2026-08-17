"""全单累计盈亏曲线(无 live 判定, 直接读回测 CSV 的 cumPnl)。带缩放 + 鼠标 x/y 十字线 + 悬停明细。
输出: workbuddy/data/change_20260101_20260812/_curve_final.html
"""
import csv
import json
import os

SRC = os.environ.get("CURVE_SRC", "workbuddy/data/change_20260101_20260812/_bt_r6pulse.csv")
OUT = os.environ.get("CURVE_OUT", "workbuddy/data/change_20260101_20260812/_curve_final.html")

rows = list(csv.DictReader(open(SRC, encoding="utf-8-sig")))
pts = [{
    "t": r["openTime"], "pnl": float(r["pnlPct"]), "sym": r["symbol"],
    "reason": r["reason"], "w": float(r.get("weight", 1.0) or 1.0),
    "cum": float(r["cumPnl"]),
} for r in rows]
pts.sort(key=lambda p: p["t"])

data_js = json.dumps(pts, ensure_ascii=False)
total = pts[-1]["cum"] if pts else 0
n = len(pts)
wtot = sum(p["w"] for p in pts)

html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>全单累计盈亏 · 01-01~08-12 ({n}笔)</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
  html,body {{ margin:0; padding:0; height:100%; background:#0d1117; }}
  #chart {{ width:100%; height:100%; }}
  .hud {{
    position:fixed; top:14px; left:50%; transform:translateX(-50%);
    color:#c9d1d9; font:13px/1.6 "Segoe UI", system-ui, sans-serif;
    background:rgba(22,27,34,.85); border:1px solid #30363d; border-radius:8px;
    padding:8px 18px; text-align:center; z-index:10; backdrop-filter:blur(4px);
  }}
  .hud b {{ color:#58a6ff; }}
</style>
</head>
<body>
<div class="hud">
  全单累计 · {n} 笔 / 加权本金 {wtot:.0f} 份<br>
  累计盈亏 <b>{total:+.2f}%</b><br>
  <span style="color:#8b949e">滚轮/拖拽缩放 · 悬停显示明细</span>
</div>
<div id="chart"></div>
<script>
var pts = {data_js};
var chart = echarts.init(document.getElementById('chart'), 'dark');
chart.setOption({{
  backgroundColor:'transparent',
  tooltip: {{
    trigger:'axis', confine:true,
    backgroundColor:'rgba(22,27,34,.95)', borderColor:'#30363d',
    textStyle:{{ color:'#c9d1d9', fontSize:12 }},
    axisPointer:{{ type:'cross', label:{{ backgroundColor:'#30363d', color:'#e6edf3',
      formatter:function(p){{ return p.axisDimension==='x' ? p.value : p.value.toFixed(2)+'%'; }} }} }},
    formatter: function(ps) {{
      var p = pts[ps[0].dataIndex];
      return '<b>'+p.t+'</b> ('+p.sym+')<br>' +
        '单笔: '+(p.pnl>=0?'<span style="color:#3fb950">+':'<span style="color:#f85149">')+p.pnl.toFixed(2)+'%</span> ('+p.reason+')'+
        (p.w>1?' <span style="color:#d29922">×'+p.w+'仓</span>':'')+'<br>' +
        '累计盈亏: <b>'+p.cum.toFixed(2)+'%</b>';
    }}
  }},
  grid: {{ left:75, right:35, top:95, bottom:70 }},
  xAxis: {{
    type:'category', data:pts.map(p=>p.t), boundaryGap:false,
    axisLabel:{{ color:'#8b949e', fontSize:11, formatter:function(v){{ return v.slice(0,10); }} }},
    axisLine:{{ lineStyle:{{ color:'#30363d' }} }}, axisTick:{{ show:false }}
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
  series: [{{
    name:'全单累计', type:'line', data:pts.map(p=>p.cum), symbol:'none',
    lineStyle:{{ width:2, color:'#58a6ff' }}, itemStyle:{{ color:'#58a6ff' }},
    areaStyle:{{ color:new echarts.graphic.LinearGradient(0,0,0,1,[
      {{offset:0,color:'#58a6ff33'}},{{offset:1,color:'#58a6ff00'}}]) }},
    markLine:{{ silent:true, symbol:'none', lineStyle:{{ color:'#f85149', type:'dashed', width:1 }},
      label:{{ color:'#f85149', position:'insideEndTop', formatter:'0%' }}, data:[{{ yAxis:0 }}] }}
  }}]
}});
window.addEventListener('resize', function(){{ chart.resize(); }});
</script>
</body>
</html>"""

with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)
print(f"输出: {OUT} | {n} 笔 / 加权本金{wtot:.0f} / 累计 {total:+.2f}%")
