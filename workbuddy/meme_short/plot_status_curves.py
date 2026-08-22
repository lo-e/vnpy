"""sim / dropped / live 三个状态各自的累计盈亏曲线 HTML(带缩放 + 鼠标位置 x/y 十字线)。
读 judged CSV, 按状态分组, 各自按时间顺序累计(等额口径, 各自从 0 起)。
输出: workbuddy/data/change_20260101_20260812/_curve_{sim,dropped,live}.html
"""
import csv
import json

SRC = "data/change_20260101_20260812/_backtest_top1_short_judged.csv"
OUT_DIR = "data/change_20260101_20260812/"

rows = list(csv.DictReader(open(SRC, encoding="utf-8-sig")))
groups = {"sim": [], "dropped": [], "live": []}
for r in rows:
    groups[r["status"]].append({
        "t": r["openTime"],
        "pnl": float(r["pnlPct"]),
        "sym": r["symbol"],
        "ct": r["closeTime"],
        "reason": r["reason"],
    })
for g in groups.values():
    g.sort(key=lambda p: p["t"])

COLORS = {"sim": "#58a6ff", "dropped": "#f85149", "live": "#3fb950"}
NAMES = {"sim": "模拟阶段(sim)", "dropped": "剔除(dropped)", "live": "实盘验证(live)"}


def make_html(status, pts):
    data_js = json.dumps(pts, ensure_ascii=False)
    n = len(pts)
    total = pts[-1]["cum"] if pts else 0
    wins = sum(1 for p in pts if p["pnl"] > 0)
    color = COLORS[status]
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>{NAMES[status]} 盈亏曲线 · 06-01~08-11</title>
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
  .hud b {{ color:{color}; }}
  .hud .red {{ color:#f85149; }} .hud .green {{ color:#3fb950; }}
</style>
</head>
<body>
<div class="hud">
  {NAMES[status]} · 06-01 ~ 08-11<br>
  <b>{n}</b> 笔 · 胜率 <b>{wins/n*100:.1f}%</b> · 累计盈亏 <b class="{'green' if total>=0 else 'red'}">{total:+.2f}%</b><br>
  <span style="color:#8b949e">滚轮/拖拽缩放 · 悬停显示时间/盈亏</span>
</div>
<div id="chart"></div>
<script>
var pts = {data_js};
var times = pts.map(p => p.t);
var cum = pts.map(p => p.cum);
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
      return '<b>'+p.t+'</b><br>' +
        '合约: '+p.sym+'<br>' +
        '单笔盈亏: '+(p.pnl>=0?'<span style="color:#3fb950">+':'<span style="color:#f85149">')+p.pnl.toFixed(2)+'%</span> ('+p.reason+')<br>' +
        '累计盈亏: <b>'+p.cum.toFixed(2)+'%</b><br>' +
        '平仓: '+p.ct;
    }}
  }},
  grid: {{ left:70, right:30, top:80, bottom:70 }},
  xAxis: {{
    type:'category', data:times, boundaryGap:false,
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
    name:'{NAMES[status]}', type:'line', data:cum, smooth:false, symbol:'circle', symbolSize:5,
    showSymbol:true,
    lineStyle:{{ width:2, color:'{color}' }},
    itemStyle:{{ color:'{color}' }},
    areaStyle:{{ color:new echarts.graphic.LinearGradient(0,0,0,1,[
      {{offset:0,color:'{color}55'}},{{offset:1,color:'{color}00'}}]) }},
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
  }}]
}});
window.addEventListener('resize', function(){{ chart.resize(); }});
</script>
</body>
</html>"""


for status, pts in groups.items():
    cum = 0.0
    for p in pts:
        cum += p["pnl"]
        p["cum"] = round(cum, 4)
    html = make_html(status, pts)
    out = f"{OUT_DIR}_curve_{status}.html"
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"{status}: {len(pts)} 笔 → {out} | 末值 {pts[-1]['cum'] if pts else 0:+.2f}%")
