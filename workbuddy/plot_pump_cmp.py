"""两个回测版本盈亏曲线对比(同区)。读CSV的 cumPnl, 图例名可配。"""
import csv
import json
import os

NAME_A = os.environ.get("NAME_A", "A版")
NAME_B = os.environ.get("NAME_B", "B版")
SRC_A = os.environ.get("SRC_A", "workbuddy/data/change_20260101_20260812/_bt_r6pulse.csv")
SRC_B = os.environ.get("SRC_B", "workbuddy/data/change_20260101_20260812/_bt_pump15c.csv")
OUT = os.environ.get("OUT", "workbuddy/data/change_20260101_20260812/_curve_pump_cmp.html")

def load(p):
    pts = []
    for r in csv.DictReader(open(p, encoding="utf-8-sig")):
        pts.append({"t": r["openTime"], "cum": float(r["cumPnl"]), "sym": r["symbol"], "pnl": float(r["pnlPct"]), "reason": r["reason"]})
    pts.sort(key=lambda x: x["t"])
    return pts

pa, pb = load(SRC_A), load(SRC_B)
ta = [p["t"] for p in pa]; tb = [p["t"] for p in pb]
ja, jb = json.dumps(pa, ensure_ascii=False), json.dumps(pb, ensure_ascii=False)
end_a, end_b = pa[-1]["cum"], pb[-1]["cum"]

NAME_A_JS = json.dumps(NAME_A, ensure_ascii=False)
NAME_B_JS = json.dumps(NAME_B, ensure_ascii=False)
html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>{NAME_A} vs {NAME_B} · 盈亏曲线对比</title>
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
  .blue {{ color:#58a6ff; }} .red {{ color:#f85149; }}
</style>
</head>
<body>
<div class="hud">
  01-01 ~ 08-12 · {len(pa)}笔 vs {len(pb)}笔<br>
  <span class="blue">■ {NAME_A}</span> <b class="blue">{end_a:+.1f}%</b>
  &nbsp;|&nbsp; <span class="red">■ {NAME_B}</span> <b class="red">{end_b:+.1f}%</b><br>
  <span style="color:#8b949e">滚轮/拖拽缩放 · 悬停明细</span>
</div>
<div id="chart"></div>
<script>
var pa = {ja}; var pb = {jb};
var nameA = {NAME_A_JS}; var nameB = {NAME_B_JS};
var chart = echarts.init(document.getElementById('chart'), 'dark');
chart.setOption({{
  backgroundColor:'transparent',
  tooltip: {{
    trigger:'axis', confine:true,
    backgroundColor:'rgba(22,27,34,.95)', borderColor:'#30363d',
    textStyle:{{ color:'#c9d1d9', fontSize:12 }},
    axisPointer:{{ type:'cross', label:{{ backgroundColor:'#30363d', color:'#e6edf3',
      formatter:function(p){{ return p.axisDimension==='x' ? p.value : p.value.toFixed(1)+'%'; }} }} }},
    formatter: function(ps) {{
      var s = '';
      for (var i=0;i<ps.length;i++){{
        var arr = ps[i].seriesName===nameA?pa:pb;
        var p = arr[ps[i].dataIndex];
        s += '<b>'+ps[i].seriesName+'</b> '+p.t+' ('+p.sym+')<br>'+
             '  单笔: '+p.pnl.toFixed(2)+'% ('+p.reason+') | 累计: '+p.cum.toFixed(2)+'%<br>';
      }}
      return s;
    }}
  }},
  grid: {{ left:75, right:35, top:95, bottom:70 }},
  xAxis: {{
    type:'category', data:pa.map(p=>p.t), boundaryGap:false,
    axisLabel:{{ color:'#8b949e', fontSize:11, formatter:function(v){{ return v.slice(0,10); }} }},
    axisLine:{{ lineStyle:{{ color:'#30363d' }} }}, axisTick:{{ show:false }}
  }},
  yAxis: {{
    type:'value', name:'累计盈亏 %', nameTextStyle:{{ color:'#8b949e' }},
    axisLabel:{{ color:'#8b949e', formatter:'{{value}}%' }},
    splitLine:{{ lineStyle:{{ color:'#21262d' }} }}
  }},
  legend: {{ data:[nameA, nameB], top:8, textStyle:{{ color:'#c9d1d9' }}, itemWidth:18, itemHeight:10 }},
  dataZoom: [
    {{ type:'inside', xAxisIndex:0, zoomOnMouseWheel:true, moveOnMouseMove:true, moveOnMouseWheel:false }},
    {{ type:'slider', xAxisIndex:0, bottom:15, height:20, borderColor:'#30363d',
       backgroundColor:'#161b22', fillerColor:'rgba(56,139,253,.25)', textStyle:{{ color:'#8b949e' }} }}
  ],
  series: [
    {{ name:nameA, type:'line', data:pa.map(p=>p.cum), symbol:'none',
       lineStyle:{{ width:2, color:'#58a6ff' }}, itemStyle:{{ color:'#58a6ff' }},
       areaStyle:{{ color:new echarts.graphic.LinearGradient(0,0,0,1,[{{offset:0,color:'#58a6ff33'}},{{offset:1,color:'#58a6ff00'}}]) }} }},
    {{ name:nameB, type:'line', data:pb.map(p=>p.cum), symbol:'none',
       lineStyle:{{ width:2, color:'#f85149' }}, itemStyle:{{ color:'#f85149' }} }}
  ]
}});
window.addEventListener('resize', function(){{ chart.resize(); }});
</script>
</body>
</html>"""

with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)
print(f"输出: {OUT} | {NAME_A} {end_a:+.1f}% vs {NAME_B} {end_b:+.1f}%")
