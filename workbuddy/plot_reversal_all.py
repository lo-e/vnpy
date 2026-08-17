"""96 个 Top1 合约 reversal_score 批量可视化。
每币时间窗: 首个信号前24h ~ 末个信号后24h(北京)。
输出: workbuddy/data/top1_reversal_score_html/{symbol}.html (复用 esports 模板: K线+▲▼+阈值滑块+tooltip)
"""
import os, json
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import reversal_indicators as ri
import pymongo

LOCAL = timezone(timedelta(hours=8))
STEP5 = 5 * 60 * 1000
DAY = 24 * 3600 * 1000
WARM_MS = 48 * 3600 * 1000  # 展示起点前额外预热 48h(Connors rank 50 仅需 ~4h, 富余)
OUT_DIR = "workbuddy/data/top1_reversal_score_html"

cli = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
db = cli["Workbuddy_5Min_Db"]

# 每币首/末信号 + 全部信号点(时间, 24h涨幅)
import csv
first_sig, last_sig = {}, {}
sig_map = {}   # sym -> [(t_ms, chg), ...] 按时间升序
with open("workbuddy/data/change_20260701_20260811/000000_top1_rise.csv", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        t = int(datetime.strptime(r["pointTimeLocal"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL).timestamp() * 1000)
        first_sig.setdefault(r["topSymbol"], t)
        last_sig[r["topSymbol"]] = t
        sig_map.setdefault(r["topSymbol"], []).append((t, float(r["topChangePct"])))
syms = sorted(first_sig.keys())
os.makedirs(OUT_DIR, exist_ok=True)
print(f"{len(syms)} 币, 输出目录 {OUT_DIR}")


def make_html(sym, pts, fs, ls, sigs):
    """esports 同款模板: K线 + bull▲/bear▼ + Top1信号★ + 阈值滑块(1-8) + 悬停 tooltip
    sigs: [(pts 索引, 24h涨幅), ...]"""
    data_js = json.dumps(pts, ensure_ascii=False)
    sig_js = json.dumps(sigs, ensure_ascii=False)
    fs_s = datetime.fromtimestamp(fs / 1000, tz=LOCAL).strftime("%m-%d %H:%M")
    ls_s = datetime.fromtimestamp(ls / 1000, tz=LOCAL).strftime("%m-%d %H:%M")
    n = len(pts)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>{sym.upper()} 5m 反转信号 · 信号前24h~后24h</title>
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
  #meas{{padding:3px 12px;border:1px solid #30363d;border-radius:6px;cursor:pointer;user-select:none;color:#8b949e;font-size:12px;transition:all .15s}}
  #meas.on{{background:rgba(227,179,65,.18);border-color:#e3b341;color:#e3b341}}
</style>
</head>
<body>
<div class="ctrl">
  <span style="color:#e6edf3;font-weight:600">{sym.upper()} 5m</span>
  <span style="color:#8b949e;font-size:12px">信号窗 {fs_s} ~ {ls_s}</span>
  <label>反转阈值 ≥</label>
  <input type="range" id="th" min="1" max="8" step="1" value="3">
  <b id="thv">3</b>
  <span class="tag g" id="cntB">bull 0</span>
  <span class="tag r" id="cntR">bear 0</span>
  <span id="meas">📏 测量</span>
</div>
<div id="chart"></div>
<script>
var pts = {data_js};
var sigs = {sig_js};            // [pts索引, 24h涨幅]
var sigIdx = new Set(sigs.map(s=>s[0]));
var sigChg = {{}}; sigs.forEach(s=>sigChg[s[0]]=s[1]);
var n = pts.length;
var chart = echarts.init(document.getElementById('chart'), 'dark');
var zr = chart.getZr();

// ---------- 测量工具: 拖拽框选区域 → 显示区间涨跌幅/时间跨度 ----------
var measureOn = false;
var mG = new echarts.graphic.Group();
zr.add(mG);
var m = {{on:false, x0:0, y0:0, idx0:-1, c0:0, px0:''}};
var measureEl = document.getElementById('meas');
measureEl.onclick = function(){{
  measureOn = !measureOn;
  measureEl.classList.toggle('on', measureOn);
  chart.setOption({{dataZoom:[{{type:'inside', xAxisIndex:0, disabled:measureOn}}]}});
  if(!measureOn) mG.removeAll();
}};
zr.on('mousedown', function(e){{
  if(!measureOn) return;
  var d = chart.convertFromPixel({{seriesIndex:0}}, [e.offsetX, e.offsetY]);
  var i0 = Math.max(0, Math.min(n-1, Math.round(d[0])));
  m = {{on:true, x0:e.offsetX, y0:e.offsetY, idx0:i0, c0:pts[i0].c, px0:pts[i0].t}};
  mG.removeAll();
}});
zr.on('mousemove', function(e){{
  if(!measureOn || !m.on) return;
  mG.removeAll();
  var x = Math.min(m.x0,e.offsetX), y = Math.min(m.y0,e.offsetY);
  var w = Math.abs(e.offsetX-m.x0), h = Math.abs(e.offsetY-m.y0);
  if(w<2||h<2) return;
  mG.add(new echarts.graphic.Rect({{
    shape:{{x:x,y:y,width:w,height:h}},
    style:{{fill:'rgba(227,179,65,.12)', stroke:'#e3b341', lineWidth:1}}
  }}));
  var d = chart.convertFromPixel({{seriesIndex:0}}, [e.offsetX, e.offsetY]);
  var idx1 = Math.round(d[0]);
  if(idx1>=0 && idx1<n && idx1!==m.idx0){{
    var p1 = pts[idx1];
    var chg = (p1.c - m.c0)/m.c0*100;
    var t0 = new Date(m.px0.replace(' ','T')), t1 = new Date(p1.t.replace(' ','T'));
    var hrs = Math.round((t1-t0)/3600000*10)/10;
    var label = '涨跌幅 '+(chg>=0?'+':'')+chg.toFixed(2)+'%  ('+m.c0+'→'+p1.c+')  |  '+
                hrs+'h  |  '+m.px0.slice(5,16)+' ~ '+p1.t.slice(5,16);
    mG.add(new echarts.graphic.Text({{
      style:{{text:label, fill:'#e3b341', fontSize:12,
              backgroundColor:'rgba(13,17,23,.88)', padding:[4,8], borderRadius:4}},
      position:[x+4, y+4]
    }}));
  }}
}});
zr.on('mouseup', function(){{ m.on=false; }});
zr.on('globalout', function(){{ m.on=false; }});

function range(){{
  var h = Math.max.apply(null,pts.map(p=>p.h)), l = Math.min.apply(null,pts.map(p=>p.l));
  return (h-l)*0.03;
}}
var pad = range();
function render(th){{
  var kdata = pts.map(p=>[p.o,p.c,p.l,p.h]);
  var bm=[], rm=[];
  for (var i=0;i<n;i++){{
    var p=pts[i];
    if(p.bull>=th) bm.push([i,p.l-pad]);
    if(p.bear>=th) rm.push([i,p.h+pad]);
  }}
  document.getElementById('cntB').textContent='bull '+bm.length;
  document.getElementById('cntR').textContent='bear '+rm.length;
  chart.setOption({{
    tooltip:{{
      trigger:'axis', confine:true, backgroundColor:'rgba(22,27,34,.95)', borderColor:'#30363d',
      textStyle:{{color:'#c9d1d9',fontSize:12}},
      formatter:function(ps){{
        var p=pts[ps[0].dataIndex], i=ps[0].dataIndex;
        var s='<b>'+p.t+'</b><br>O:'+p.o+' H:'+p.h+' L:'+p.l+' C:'+p.c+'<br>量: '+p.v+'<br>'+
          'bull得分: <span style="color:#3fb950">'+p.bull+'</span> | bear得分: <span style="color:#f85149">'+p.bear+'</span>';
        if(p.sig.length) s+='<br>信号: '+p.sig.join(' · ');
        if(sigIdx.has(i)) s+='<br><span style="color:#e3b341">★ Top1信号 · 24h涨幅 '+sigChg[i].toFixed(2)+'%</span>';
        return s;
      }}
    }},
    grid:{{left:70,right:30,top:60,bottom:70}},
    xAxis:{{
      type:'category', data:pts.map(p=>p.t), boundaryGap:true,
      axisLabel:{{color:'#8b949e',fontSize:11,formatter:function(v){{return v.slice(5,16)}}}},
      axisLine:{{lineStyle:{{color:'#30363d'}}}}, axisTick:{{show:false}}
    }},
    yAxis:{{type:'value', scale:true, axisLabel:{{color:'#8b949e'}}, splitLine:{{lineStyle:{{color:'#21262d'}}}}}},
    dataZoom:[
      {{type:'inside', xAxisIndex:0, zoomOnMouseWheel:true, moveOnMouseMove:true, moveOnMouseWheel:false}},
      {{type:'slider', xAxisIndex:0, bottom:15, height:20, borderColor:'#30363d', backgroundColor:'#161b22',
        fillerColor:'rgba(56,139,253,.25)', textStyle:{{color:'#8b949e'}}}}
    ],
    series:[
      {{name:'K线', type:'candlestick', data:kdata, itemStyle:{{color:'#3fb950',color0:'#f85149',borderColor:'#3fb950',borderColor0:'#f85149'}}, barWidth:'70%'}},
      {{name:'做多反转', type:'scatter', data:bm, symbol:'triangle', symbolSize:12, itemStyle:{{color:'#3fb950'}}, z:10,
        label:{{show:true,position:'bottom',color:'#3fb950',fontSize:10,formatter:'▲'}}}},
      {{name:'做空反转', type:'scatter', data:rm, symbol:'triangle', symbolSize:12, itemStyle:{{color:'#f85149'}}, z:10,
        label:{{show:true,position:'top',color:'#f85149',fontSize:10,formatter:'▼'}}}},
      {{name:'Top1信号', type:'scatter', data:sigs.map(s=>[s[0],(pts[s[0]].h+pts[s[0]].l)/2]),
        symbol:'diamond', symbolSize:15, itemStyle:{{color:'#e3b341',borderColor:'#ffffff',borderWidth:1}}, z:20,
        label:{{show:true,position:'top',color:'#e3b341',fontSize:11,formatter:'★'}}}}
    ]
  }}, true);
}}
var th=document.getElementById('th');
th.oninput=function(){{document.getElementById('thv').textContent=th.value;render(parseInt(th.value));}};
render(parseInt(th.value));
window.addEventListener('resize',function(){{chart.resize();}});
</script>
</body>
</html>"""


def work(sym):
    fs, ls = first_sig[sym], last_sig[sym]
    show_start = fs - DAY
    show_end = ls + DAY
    calc_start = show_start - WARM_MS
    kc = db[f"{sym.upper()}.BINANCE"]
    docs = list(kc.find({"openTime": {"$gte": calc_start, "$lte": show_end}}).sort("openTime", 1))
    if not docs:
        return sym, 0, "无数据"
    o = [d["open"] for d in docs]; h = [d["high"] for d in docs]
    l = [d["low"] for d in docs]; c = [d["close"] for d in docs]
    sc = ri.reversal_score(o, h, l, c)
    pts = []
    first_show_i = None
    for i, d in enumerate(docs):
        if d["openTime"] < show_start:
            continue
        if first_show_i is None:
            first_show_i = i
        pts.append({
            "t": datetime.fromtimestamp(d["openTime"] / 1000, tz=LOCAL).strftime("%Y-%m-%d %H:%M"),
            "o": d["open"], "h": d["high"], "l": d["low"], "c": d["close"], "v": d["volume"],
            "bull": sc[i]["bull"], "bear": sc[i]["bear"], "sig": sc[i]["signals"],
        })
    # Top1 信号点: 信号 T 对应已收盘 K(openTime=T-5min), 映射到 pts 索引
    ot_idx = {d["openTime"]: i for i, d in enumerate(docs)}
    sigs = []
    for t_ms, chg in sig_map.get(sym, []):
        pi = ot_idx.get(t_ms - STEP5)
        if pi is not None and first_show_i is not None:
            pi -= first_show_i
            if 0 <= pi < len(pts):
                sigs.append([pi, chg])
    html = make_html(sym, pts, fs, ls, sigs)
    with open(os.path.join(OUT_DIR, f"{sym}.html"), "w", encoding="utf-8") as f:
        f.write(html)
    return sym, len(pts), "ok"


MAXW = int(os.environ.get("BW", "3"))
results = []
with ThreadPoolExecutor(max_workers=MAXW) as ex:
    futs = {ex.submit(work, s): s for s in syms}
    for fut in as_completed(futs):
        results.append(fut.result())
results.sort()
bad = [r for r in results if r[2] != "ok"]
print(f"完成 {len(results)} 币, 失败 {len(bad)}")
for r in results[:3] + results[-3:]:
    print(" ", r)
if bad:
    print("失败:", bad)
total_k = sum(r[1] for r in results)
print(f"总展示 K 数: {total_k}")
