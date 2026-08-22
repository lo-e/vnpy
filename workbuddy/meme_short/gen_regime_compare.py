# -*- coding: utf-8 -*-
"""生成月度 regime 对比 HTML: 策略月pnl(柱) vs BTC月涨跌(线), 含两窗口对比"""
import csv, pymongo, datetime, json
from datetime import timezone, timedelta
from collections import defaultdict
LOCAL = timezone(timedelta(hours=8))
db = pymongo.MongoClient("mongodb://127.0.0.1:27017")["Workbuddy_5Min_Db"]
kc = db["BTCUSDT.BINANCE"]

def monthly(path):
    rows = list(csv.DictReader(open(path, encoding="utf-8-sig")))
    sp, sn = defaultdict(float), defaultdict(int)
    for r in rows:
        k = r["openTime"][:7]
        sp[k] += float(r["pnlPct"]) * float(r["weight"])
        sn[k] += 1
    return sp, sn

def btc_monthly(months):
    prices, out = {}, {}
    for m in sorted(set(months)):
        y, mo = map(int, m.split("-"))
        ny, nm = (y + 1, 1) if mo == 12 else (y, mo + 1)
        end = int(datetime.datetime(ny, nm, 1, tzinfo=LOCAL).timestamp() * 1000) - 1
        k = kc.find_one({"openTime": {"$lte": end}}, sort=[("openTime", -1)])
        if k:
            prices[m] = k["close"]
    prev = None
    for m in sorted(prices):
        out[m] = (prices[m] - prev) / prev * 100 if prev else 0.0
        prev = prices[m]
    return out

sp24, sn24 = monthly("data/change_20240101_20251201/_bt_2024window_v2.csv")
sp26, sn26 = monthly("data/change_20251201_20260816/_bt_mp9_final.csv")
btc24 = btc_monthly(sp24.keys())
btc26 = btc_monthly(sp26.keys())

def js(d):
    return json.dumps(d, ensure_ascii=False)

months24 = sorted(btc24)
months26 = sorted(btc26)
html = """<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<title>策略月度 Regime 对比 (2024牛市窗口 vs 2026熊市窗口)</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
<style>
body{margin:0;padding:20px;background:#0d1117;color:#c9d1d9;font-family:-apple-system,'Segoe UI',sans-serif}
h1{font-size:20px;color:#f0f6fc}h2{font-size:15px;color:#8b949e;margin-top:24px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px;margin-bottom:16px}
#c24,#c26{width:100%;height:420px}
table{border-collapse:collapse;width:100%;font-size:12px;margin-top:8px}
th,td{padding:4px 8px;border:1px solid #30363d;text-align:right}
th{background:#21262d;color:#f0f6fc;position:sticky;top:0}
td:nth-child(1){text-align:left}
.pos{color:#f85149}.neg{color:#3fb950}
.sum{background:#21262d;font-weight:bold}
</style></head><body>
<h1>妖币做空策略 · 月度 Regime 对比</h1>
<div class="card"><div id="c24"></div></div>
<div class="card"><div id="c26"></div></div>
<h2>月度明细 · 2024牛市窗口</h2><div style="max-height:320px;overflow:auto"><table id="t24"></table></div>
<h2>月度明细 · 2026熊市窗口(定稿)</h2><div style="max-height:320px;overflow:auto"><table id="t26"></table></div>
<script>
function bars(el, months, sp, sn, btc, title, winColor){
  var c = echarts.init(document.getElementById(el));
  c.setOption({
    title:{text:title,textStyle:{color:'#f0f6fc',fontSize:14},left:8},
    tooltip:{trigger:'axis',axisPointer:{type:'shadow'},backgroundColor:'#161b22',borderColor:'#30363d',textStyle:{color:'#c9d1d9'}},
    legend:{data:['策略月pnl%','BTC月涨跌%'],textStyle:{color:'#8b949e'},top:28},
    grid:{left:60,right:60,top:70,bottom:40},
    xAxis:{type:'category',data:months,axisLabel:{color:'#8b949e',fontSize:10}},
    yAxis:[
      {type:'value',name:'策略月pnl%',axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#21262d'}}},
      {type:'value',name:'BTC%',axisLabel:{color:'#8b949e'},splitLine:{show:false}}
    ],
    series:[
      {name:'策略月pnl%',type:'bar',data:months.map(function(m){return sp[m]||0}),
       itemStyle:{color:function(p){return p.value>=0?'#f85149':'#3fb950'}}},
      {name:'BTC月涨跌%',type:'line',yAxisIndex:1,data:months.map(function(m){return btc[m]}),
       lineStyle:{color:'#58a6ff',width:2},itemStyle:{color:'#58a6ff'},symbolSize:5}
    ]
  });
}
function table(el, months, sp, sn, btc){
  var rows = ['<tr><th>月份</th><th>笔数</th><th>策略月pnl%</th><th>BTC月涨跌%</th><th>regime</th></tr>'];
  var tot=0;
  months.forEach(function(m){
    var s=(sp[m]||0), b=(btc[m]||0);
    tot+=s;
    var rg = b>5?'📈牛':(b<-5?'📉熊':'⚖️震荡');
    rows.push('<tr><td>'+m+'</td><td>'+(sn[m]||0)+'</td><td class="'+(s>=0?'pos':'neg')+'">'+(s>=0?'+':'')+s.toFixed(1)+'</td><td class="'+(b>=0?'pos':'neg')+'">'+(b>=0?'+':'')+b.toFixed(1)+'</td><td>'+rg+'</td></tr>');
  });
  rows.push('<tr class="sum"><td>累计</td><td>'+months.reduce(function(a,m){return a+(sn[m]||0)},0)+'</td><td class="'+(tot>=0?'pos':'neg')+'">'+(tot>=0?'+':'')+tot.toFixed(1)+'</td><td></td><td></td></tr>');
  document.getElementById(el).innerHTML=rows.join('');
}
bars('c24',__M24__,__SP24__,__SN24__,__B24__,'2024牛市窗口 · 策略月pnl vs BTC月涨跌 (相关系数-0.06)');
bars('c26',__M26__,__SP26__,__SN26__,__B26__,'2026熊市窗口(定稿) · 策略月pnl vs BTC月涨跌');
table('t24',__M24__,__SP24__,__SN24__,__B24__);
table('t26',__M26__,__SP26__,__SN26__,__B26__);
</script></body></html>"""
html = (html.replace("__M24__", js(months24)).replace("__SP24__", js(sp24))
            .replace("__SN24__", js(sn24)).replace("__B24__", js(btc24))
            .replace("__M26__", js(months26)).replace("__SP26__", js(sp26))
            .replace("__SN26__", js(sn26)).replace("__B26__", js(btc26)))
out = "data/change_20240101_20251201/_regime_compare.html"
open(out, "w", encoding="utf-8").write(html)
print("输出:", out, len(html), "bytes")
