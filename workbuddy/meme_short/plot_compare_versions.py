# -*- coding: utf-8 -*-
"""三版对比曲线: final(无regime) / nosideways(剔SIDEWAYS) / bear(只BEAR) 同图。
沿用 plot_final_curve.py 的暗色 echarts 风格 + 滚轮缩放 + 十字光标 xy 显示 + HUD 显 maxDD/ddRatio。
额外: 样本外分界竖线 + 每版样本外(切点后)收益。
产物命名带 "_" 前缀(2026-09-13 超拍板)。
用法: python plot_compare_versions.py   (env 可覆盖 CMP_DIR / CMP_OUT / CMP_CUTOFF)
"""
import csv
import json
import os

WD = os.path.dirname(os.path.abspath(__file__))
DIR = os.environ.get("CMP_DIR", os.path.join(WD, "data", "change_20251201_20260912"))
OUT = os.environ.get("CMP_OUT", os.path.join(DIR, "_curve_3versions.html"))
CUT = os.environ.get("CMP_CUTOFF", "2026-08-29 00:00:00")

VER = [
    ("无 regime (定稿)", "_bt_mp9_final.csv", "#58a6ff"),
    ("剔 SIDEWAYS", "_bt_mp9_nosideways.csv", "#d29922"),
    ("只 BEAR", "_bt_mp9_bear.csv", "#a371f7"),
]


def load(fn):
    p = os.path.join(DIR, fn)
    if not os.path.exists(p):
        return None
    rows = list(csv.DictReader(open(p, encoding="utf-8-sig")))
    rows.sort(key=lambda r: r["openTime"])
    pts, ins, oos = [], 0.0, 0.0
    n_in = n_oos = 0
    for r in rows:
        w = float(r.get("weight", 1.0) or 1.0)
        pnl = float(r["pnlPct"])
        c = float(r["cumPnl"])
        pts.append([r["openTime"], round(c, 2)])
        if r["closeTime"] < CUT:
            ins += w * pnl
            n_in += 1
        else:
            oos += w * pnl
            n_oos += 1
    wins = sum(1 for r in rows if float(r["pnlPct"]) > 0)
    return {
        "pts": pts,
        "n": len(rows),
        "total": float(rows[-1]["cumPnl"]),
        "win": 100.0 * wins / len(rows) if rows else 0.0,
        "maxdd": float(rows[-1].get("maxDD", 0) or 0),
        "ddr": float(rows[-1].get("ddRatio", 0) or 0),
        "ins": ins, "n_in": n_in,
        "oos": oos, "n_oos": n_oos,
        "t0": rows[0]["openTime"], "t1": rows[-1]["openTime"],
    }


series, hud_rows, t0, t1 = [], [], None, None
for label, fn, color in VER:
    d = load(fn)
    if d is None:
        print("跳过(缺失):", fn)
        continue
    t0 = t0 or d["t0"]
    t1 = d["t1"]
    series.append({
        "name": label, "type": "line", "symbol": "none",
        "data": d["pts"], "lineStyle": {"width": 2, "color": color},
        "itemStyle": {"color": color},
    })
    hud_rows.append(
        f'<tr><td style="color:{color}">{label}</td><td>{d["n"]}</td>'
        f'<td>{d["win"]:.1f}%</td><td><b>{d["total"]:+.1f}</b></td>'
        f'<td style="color:#f85149">{d["maxdd"]:.1f}</td>'
        f'<td style="color:#d29922">{d["ddr"]:.3f}</td>'
        f'<td style="color:{"#3fb950" if d["oos"] >= 0 else "#f85149"}">'
        f'{d["oos"]:+.1f}</td></tr>')

if not series:
    raise SystemExit("没有可绘制的 CSV")

# 判定: 定稿 vs 剔SIDEWAYS 在样本外(切点后)的差值
base = load("_bt_mp9_final.csv")
nos = load("_bt_mp9_nosideways.csv")
verdict = ""
if base and nos:
    delta = nos["oos"] - base["oos"]
    verdict = (f'样本外区间 剔SIDEWAYS 相对 定稿 <b style="color:'
               f'{"#3fb950" if delta >= 0 else "#f85149"}">{delta:+.1f} pp</b>')

html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>三版对比 · {t0[:10]} ~ {t1[:10]}</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
  html,body {{ margin:0; padding:0; height:100%; background:#0d1117; }}
  #chart {{ width:100%; height:100%; }}
  .hud {{
    position:fixed; top:14px; left:50%; transform:translateX(-50%);
    color:#c9d1d9; font:12px/1.55 "Segoe UI", system-ui, sans-serif;
    background:rgba(22,27,34,.9); border:1px solid #30363d; border-radius:8px;
    padding:10px 20px; z-index:10; backdrop-filter:blur(4px);
  }}
  .hud table {{ border-collapse:collapse; }}
  .hud th {{ color:#8b949e; font-weight:400; padding:1px 9px; }}
  .hud td {{ padding:1px 9px; text-align:right; }}
  .hud td:first-child, .hud th:first-child {{ text-align:left; }}
  .hud .cap {{ color:#8b949e; text-align:center; }}
</style>
</head>
<body>
<div class="hud">
  <div class="cap">{t0[:10]} ~ {t1[:10]} · 样本外分界 <b style="color:#f85149">{CUT[:10]}</b> · 滚轮/拖拽缩放</div>
  <table>
    <tr><th>版本</th><th>笔数</th><th>胜率</th><th>加权pp</th><th>maxDD</th><th>ddRatio</th><th>样本外pp</th></tr>
    {''.join(hud_rows)}
  </table>
  <div class="cap">{verdict}</div>
</div>
<div id="chart"></div>
<script>
var series = {json.dumps(series, ensure_ascii=False)};
var chart = echarts.init(document.getElementById('chart'), 'dark');
series.forEach(function(s) {{
  if (s.name === '只 BEAR') {{
    s.markLine = {{ silent:true, symbol:'none', lineStyle:{{ color:'#f85149', type:'dashed', width:1 }},
      label:{{ color:'#f85149', position:'insideEndTop' }}, data:[{{ xAxis:'{CUT}' }}] }};
  }}
}});
chart.setOption({{
  backgroundColor:'transparent',
  legend: {{ top:106, textStyle:{{ color:'#c9d1d9' }}, data:series.map(function(s){{return s.name;}}) }},
  tooltip: {{
    trigger:'axis', confine:true,
    backgroundColor:'rgba(22,27,34,.95)', borderColor:'#30363d',
    textStyle:{{ color:'#c9d1d9', fontSize:12 }},
    axisPointer:{{ type:'cross', label:{{ backgroundColor:'#30363d', color:'#e6edf3',
      formatter:function(p){{ return p.axisDimension==='x' ? p.value : p.value.toFixed(2)+'%'; }} }} }},
    formatter: function(ps) {{
      var out = '<b>' + String(ps[0].axisValue).slice(0,16) + '</b>';
      ps.forEach(function(p) {{
        if (p.value == null) return;
        out += '<br>' + p.marker + p.seriesName + ': <b>' +
               Number(p.value[1]).toFixed(2) + '%</b>';
      }});
      return out;
    }}
  }},
  grid: {{ left:78, right:35, top:150, bottom:70 }},
  xAxis: {{
    type:'time', boundaryGap:false,
    axisLabel:{{ color:'#8b949e', fontSize:11 }},
    axisLine:{{ lineStyle:{{ color:'#30363d' }} }}, axisTick:{{ show:false }},
    splitLine:{{ show:false }}
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
  series: series
}});
window.addEventListener('resize', function(){{ chart.resize(); }});
</script>
</body>
</html>"""

with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)
print("输出:", OUT)
for label, fn, _c in VER:
    d = load(fn)
    if d:
        print("  %-16s %5d笔 胜率%5.2f%%  加权%+8.2f  样本内%+8.2f(%d笔)  样本外%+7.2f(%d笔)"
              % (label, d["n"], d["win"], d["total"], d["ins"], d["n_in"], d["oos"], d["n_oos"]))
