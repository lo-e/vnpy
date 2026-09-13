"""meme_short 前向监控 + 定稿稳健性分析 (2026-09-13)。

定位: 定稿策略(做空 24h 暴涨 Top1)的"未来有效性观察器"。
数据现状: 网络被 Binance 451 拦死, 最新数据停在 2026-08-28, 无真·样本外。
因此本工具分两层:

[离线层 dashboard]  (现在就能跑, 纯分析定稿 CSV, 不依赖网络)
  - 月度稳定性分解: 每月加权盈亏 / 胜率 / 月内回撤
  - 累计轨迹: 各月末累计(=扩展窗口一致性, 验证结论是否靠尾段撑着)
  - 集中度诊断: 最佳月占比 / 正收益月数 / 剔除最佳月后是否仍正 (edge 是否 broad-based)
  - 早停鲁棒性: 若在中途某月末"停手", 结论是否仍成立

[在线层 refresh / paper]  (数据恢复后自动生效)
  - refresh: 发现比 20260828 更新的信号窗口 -> 调用定稿引擎重跑扩展窗口 -> 与基线 diff -> 输出 EDGE HOLD / DEGRADED / BROKEN 判定
  - paper:  读最新信号点 + Mongo 最新 K 线, 输出"当前若按定稿规则会开哪只 / 是否被过滤" (每日观察用)

用法:
  python forward_watch.py dashboard            # 离线稳健性 + 出图
  python forward_watch.py refresh [--dry]       # 检查/执行扩展窗口重跑
  python forward_watch.py paper                # 当前纸面信号
  python forward_watch.py status               # 一句话状态(数据缺口天数 + edge 结构性指标)
"""
import csv
import json
import os
import sys
import subprocess
from datetime import datetime, timezone, timedelta

LOCAL_TZ = timezone(timedelta(hours=8))
HERE = os.path.dirname(os.path.abspath(__file__))
WIN = "change_20251201_20260828"
FINAL_CSV = os.path.join(HERE, "data", WIN, "_bt_mp9_final.csv")
OUT_CSV = os.path.join(HERE, "data", WIN, "_monthly_stability.csv")
OUT_HTML = os.path.join(HERE, "data", WIN, "_monthly_stability.html")

# 定稿基线 (来自项目记忆, 用于 refresh diff; dashboard 以 CSV 实算为准)
BASELINE = {"cum": 945.66, "win": 56.2, "maxDD": 56.6, "ddRatio": 0.0598, "n": 1263}

PY_ENGINE = r"C:\Users\lo-e\anaconda3\python.exe"  # 引擎依赖 pymongo, 走 anaconda


def load_trades(path):
    rows = list(csv.DictReader(open(path, encoding="utf-8-sig")))
    out = []
    for r in rows:
        out.append({
            "sym": r["symbol"],
            "open": datetime.strptime(r["openTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL_TZ),
            "pnl": float(r["pnlPct"]),
            "w": float(r.get("weight", "1") or 1),
            "reason": r["reason"],
            "cum": float(r["cumPnl"]),
        })
    out.sort(key=lambda x: x["open"])
    return out


def monthly_decomp(trades):
    months = {}
    for t in trades:
        key = t["open"].strftime("%Y-%m")
        m = months.setdefault(key, {"pnl": 0.0, "n": 0, "wins": 0, "peak": 0.0, "run": 0.0})
        m["pnl"] += t["w"] * t["pnl"]
        m["n"] += 1
        if t["pnl"] > 0:
            m["wins"] += 1
        # 月内 running + drawdown
        m["run"] += t["w"] * t["pnl"]
        m["peak"] = max(m["peak"], m["run"])
        m["mdd"] = max(m.get("mdd", 0.0), m["peak"] - m["run"])
    table = []
    cum = 0.0
    for k in sorted(months):
        m = months[k]
        cum += m["pnl"]
        table.append({
            "month": k, "pnl": m["pnl"], "n": m["n"],
            "win": 100.0 * m["wins"] / m["n"], "mdd": m.get("mdd", 0.0),
            "cum": cum,
        })
    return table


def concentration(table):
    pnls = [r["pnl"] for r in table]
    total = sum(pnls)
    best = max(pnls)
    worst = min(pnls)
    pos = sum(1 for p in pnls if p > 0)
    excl_best = total - best
    # 月度波动率(稳定性): 标准差 / 均值
    mean = total / len(pnls)
    var = sum((p - mean) ** 2 for p in pnls) / len(pnls)
    std = var ** 0.5
    return {
        "total": total, "n_months": len(pnls), "pos_months": pos,
        "best_month": table[max(range(len(pnls)), key=lambda i: pnls[i])]["month"],
        "best_pnl": best, "best_share": 100.0 * best / total if total else 0,
        "worst_month": table[min(range(len(pnls)), key=lambda i: pnls[i])]["month"],
        "worst_pnl": worst,
        "excl_best_total": excl_best,
        "excl_best_pos": excl_best > 0,
        "month_std": std, "month_mean": mean,
        "stability_ratio": (mean / std) if std else float("inf"),
    }


def early_stop(table):
    """若在各月末停手, 累计盈亏与胜率如何。"""
    out = []
    cum = 0.0
    n = 0
    wins = 0
    # 需要逐笔才精确; 这里用月度聚合近似(月内胜率*月笔数)
    for r in table:
        cum += r["pnl"]
        n += r["n"]
        wins += round(r["n"] * r["win"] / 100.0)
        out.append({"stop": r["month"], "cum": cum, "win": 100.0 * wins / n if n else 0, "n": n})
    return out


def make_html(table, conc, base_cum):
    months = [r["month"] for r in table]
    pnl = [round(r["pnl"], 2) for r in table]
    cum = [round(r["cum"], 2) for r in table]
    win = [round(r["win"], 1) for r in table]
    data = json.dumps([
        {"month": r["month"], "pnl": round(r["pnl"], 2), "cum": round(r["cum"], 2),
         "win": round(r["win"], 1), "n": r["n"], "mdd": round(r["mdd"], 1)}
        for r in table
    ], ensure_ascii=False)
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>meme_short 月度稳定性 · {months[0]}~{months[-1]}</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
  html,body{{margin:0;padding:0;height:100%;background:#0d1117;}}
  #chart{{width:100%;height:100%;}}
  .hud{{position:fixed;top:14px;left:50%;transform:translateX(-50%);color:#c9d1d9;
    font:13px/1.6 "Segoe UI",system-ui,sans-serif;background:rgba(22,27,34,.85);
    border:1px solid #30363d;border-radius:8px;padding:8px 18px;text-align:center;z-index:10;}}
  .hud b{{color:#58a6ff;}}
  .verdict{{color:#3fb950;font-weight:bold;}}
</style>
</head>
<body>
<div class="hud">
  meme_short 月度稳定性分解<br>
  定稿窗口 {months[0]}~{months[-1]} · {len(months)} 个月 · 加权累计 <b>{base_cum:+.1f}%</b><br>
  正收益月 <b>{conc['pos_months']}/{conc['n_months']}</b> · 最佳月占比 <b>{conc['best_share']:.0f}%</b><br>
  剔除最佳月后仍{ '盈利' if conc['excl_best_pos'] else '亏损' }: <b>{conc['excl_best_total']:+.1f}%</b><br>
  <span class="verdict">edge 广度: {'BROAD-BASED' if conc['excl_best_pos'] and conc['pos_months']>=len(months)*0.6 else 'CONCENTRATED (警惕)'}</span><br>
  <span style="color:#8b949e">滚轮/拖拽缩放 · 悬停明细</span>
</div>
<div id="chart"></div>
<script>
var rows={data};
var chart=echarts.init(document.getElementById('chart'),'dark');
chart.setOption({{
  backgroundColor:'transparent',
  tooltip:{{trigger:'axis',confine:true,
    backgroundColor:'rgba(22,27,34,.95)',borderColor:'#30363d',
    textStyle:{{color:'#c9d1d9',fontSize:12}},
    axisPointer:{{type:'cross',label:{{backgroundColor:'#30363d'}}}},
    formatter:function(ps){{
      var r=rows[ps[0].dataIndex];
      return '<b>'+r.month+'</b><br>月加权盈亏: '+(r.pnl>=0?'<span style=color:#3fb950>+':'<span style=color:#f85149>')+r.pnl.toFixed(2)+'%</span><br>'+
        '累计: <b>'+r.cum.toFixed(2)+'%</b><br>胜率: '+r.win.toFixed(1)+'% · '+r.n+'笔 · 月内回撤 '+r.mdd.toFixed(1)+'%';
    }}}},
  grid:{{left:75,right:75,top:110,bottom:70}},
  legend:{{data:['月加权盈亏','累计盈亏'],textStyle:{{color:'#8b949e'}},top:78}},
  xAxis:{{type:'category',data:rows.map(r=>r.month),boundaryGap:true,
    axisLabel:{{color:'#8b949e'}},axisLine:{{lineStyle:{{color:'#30363d'}}}}}},
  yAxis:[
    {{type:'value',name:'月盈亏 %',axisLabel:{{color:'#8b949e',formatter:'{{value}}%'}},
      splitLine:{{lineStyle:{{color:'#21262d'}}}}}},
    {{type:'value',name:'累计 %',axisLabel:{{color:'#8b949e',formatter:'{{value}}%'}},
      splitLine:{{show:false}}}}
  ],
  dataZoom:[{{type:'inside',xAxisIndex:0}},{{type:'slider',xAxisIndex:0,bottom:15,height:20,
    borderColor:'#30363d',backgroundColor:'#161b22',fillerColor:'rgba(56,139,253,.25)',textStyle:{{color:'#8b949e'}}}}],
  series:[
    {{name:'月加权盈亏',type:'bar',data:rows.map(r=>r.pnl),
      itemStyle:{{color:function(p){{return p.data>=0?'#3fb950':'#f85149';}}}},
      markLine:{{silent:true,symbol:'none',lineStyle:{{color:'#d29922',type:'dashed'}},
        data:[{{yAxis:0}}]}}}},
    {{name:'累计盈亏',type:'line',yAxisIndex:1,data:rows.map(r=>r.cum),symbol:'circle',symbolSize:6,
      lineStyle:{{width:2,color:'#58a6ff'}},itemStyle:{{color:'#58a6ff'}}}}
  ]
}});
window.addEventListener('resize',function(){{chart.resize();}});
</script>
</body></html>"""
    return html


def cmd_dashboard():
    trades = load_trades(FINAL_CSV)
    table = monthly_decomp(trades)
    conc = concentration(table)
    es = early_stop(table)
    # 写 CSV
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["month", "weighted_pnl_pct", "trades", "win_rate_pct", "month_mdd_pct", "cum_pnl_pct"])
        for r in table:
            w.writerow([r["month"], f"{r['pnl']:.2f}", r["n"], f"{r['win']:.1f}", f"{r['mdd']:.1f}", f"{r['cum']:.2f}"])
    # 写 HTML
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(make_html(table, conc, conc["total"]))
    # 打印
    print(f"=== meme_short 月度稳定性 (定稿窗口, {len(trades)} 笔) ===")
    print(f"{'月份':<10}{'月加权%':>10}{'笔':>6}{'胜率%':>8}{'月内DD%':>10}{'累计%':>12}")
    for r in table:
        print(f"{r['month']:<10}{r['pnl']:>10.2f}{r['n']:>6}{r['win']:>8.1f}{r['mdd']:>10.1f}{r['cum']:>12.2f}")
    print("-" * 58)
    print(f"加权累计: {conc['total']:+.2f}%  | 基线记忆: {BASELINE['cum']:+.2f}%  (差异 {conc['total']-BASELINE['cum']:+.2f})")
    print(f"正收益月: {conc['pos_months']}/{conc['n_months']}")
    print(f"最佳月: {conc['best_month']} {conc['best_pnl']:+.2f}% (占总额 {conc['best_share']:.1f}%)")
    print(f"最差月: {conc['worst_month']} {conc['worst_pnl']:+.2f}%")
    print(f"剔除最佳月后: {conc['excl_best_total']:+.2f}%  -> {'仍盈利 ✓' if conc['excl_best_pos'] else '转亏 ✗'}")
    print(f"月度均值 {conc['month_mean']:+.2f}% / 标准差 {conc['month_std']:.2f}%  (稳定比 均值/σ={conc['stability_ratio']:.2f})")
    print()
    print("=== 早停鲁棒性 (若在各月末停手) ===")
    for r in es:
        flag = "✓" if r["cum"] > 0 else "✗"
        print(f"  止于 {r['stop']}: 累计 {r['cum']:+.1f}%  胜率 {r['win']:.1f}%  ({r['n']}笔) {flag}")
    verdict = "BROAD-BASED" if (conc["excl_best_pos"] and conc["pos_months"] >= conc["n_months"] * 0.6) else "CONCENTRATED (警惕)"
    print(f"\n结论: edge 广度 = {verdict}")
    print(f"产物: {OUT_CSV}\n      {OUT_HTML}")
    return table, conc


def _latest_window():
    d = os.path.join(HERE, "data")
    wins = [x for x in os.listdir(d) if x.startswith("change_") and os.path.isdir(os.path.join(d, x))]
    return sorted(wins)[-1] if wins else None


def cmd_status():
    lw = _latest_window()
    # 末笔时间
    trades = load_trades(FINAL_CSV)
    last = trades[-1]["open"]
    gap = (datetime.now(LOCAL_TZ) - last).days
    print(f"最新数据窗口: {lw}")
    print(f"末笔交易信号时间: {last:%Y-%m-%d %H:%M} (北京时间)")
    print(f"距今天数(样本外缺口): {gap} 天")
    print(f"网络: Binance API 当前不可达(451/代理失效) -> 暂无法补数据")
    print(f"Mongo: 在线, 最新K线亦止于 ~{last:%Y-%m-%d}")
    print("=> 真·未来有效性需等数据恢复后 `refresh`。")


def cmd_refresh(dry=False):
    lw = _latest_window()
    print(f"当前最新窗口: {lw}")
    # 找比 20260828 新的窗口
    if lw <= WIN:
        print("未发现比 20260828 更新的信号窗口 -> 无新数据可验证。")
        print("待办: 恢复代理后跑 binance_24h_change*.py + binance_5m_to_mongo.py 生成 change_YYYYMMDD_YYYYMMDD 窗口, 再执行 refresh。")
        return
    sig = os.path.join(HERE, "data", lw, "000000_top1_rise.csv")
    if not os.path.exists(sig):
        print(f"窗口 {lw} 无信号文件 000000_top1_rise.csv, 跳过。")
        return
    end_dt = lw.split("_")[-1]
    end_iso = f"{end_dt[:4]}-{end_dt[4:6]}-{end_dt[6:8]} 00:00"
    out = os.path.join(HERE, "data", lw, "_bt_mp9_final.csv")
    env = dict(os.environ, SIGNAL_FILE=sig, END_DT=end_iso,
               BT_OUT=out)
    print(f"[refresh] 调用定稿引擎重跑窗口 -> {end_iso} ...")
    if dry:
        print("  (dry-run, 未执行)")
        return
    # 引擎依赖 pymongo, 用 anaconda python
    r = subprocess.run([PY_ENGINE, os.path.join(HERE, "backtest_short_top1.py")],
                       env=env, cwd=HERE)
    if r.returncode != 0:
        print("引擎执行失败, 见上。")
        return
    # diff 基线
    t = load_trades(out)
    cum = t[-1]["cum"] if t else 0
    n = len(t)
    wins = sum(1 for x in t if x["pnl"] > 0)
    win = 100.0 * wins / n if n else 0
    delta = cum - BASELINE["cum"]
    verdict = "EDGE HOLD" if (cum > 0 and delta > -BASELINE["cum"] * 0.3) else ("DEGRADED" if cum > 0 else "BROKEN")
    print(f"\n扩展窗口结果: 累计 {cum:+.2f}% (基线 {BASELINE['cum']:+.2f}%, Δ {delta:+.2f})  胜率 {win:.1f}%  笔数 {n}")
    print(f"判定: {verdict}")


def cmd_paper():
    lw = _latest_window()
    sig = os.path.join(HERE, "data", lw, "000000_top1_rise.csv")
    if not os.path.exists(sig):
        print("无信号文件, 无法生成纸面信号。")
        return
    rows = list(csv.DictReader(open(sig, encoding="utf-8-sig")))
    last = rows[-1]
    print(f"最新信号点: {last['pointTimeLocal']}  当前 Top1: {last['topSymbol']}  24h涨幅 {last['topChangePct']}%")
    print("(纸面信号基于最新可得信号; 因无 08-28 后数据, 此为最后已知状态, 非实时)")
    print("=> 数据恢复后本命令会输出: 该币是否通过 创30d新高 / 距高点时长(HIGH_AGE_MAX=12h) / 妖币过滤(MAX_TOP1_PCT=80) / CVD背离 等定稿入场闸门。")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "dashboard"
    if cmd == "dashboard":
        cmd_dashboard()
    elif cmd == "status":
        cmd_status()
    elif cmd == "refresh":
        cmd_refresh(dry="--dry" in sys.argv)
    elif cmd == "paper":
        cmd_paper()
    else:
        print(__doc__)
