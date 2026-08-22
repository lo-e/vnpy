"""做空 24h 涨幅 Top1 策略回测 (2025-12-01 00:00 -> 2026-08-16 00:00 信号期)。
最简版本: 无 reversal/无动态仓位/无复杂止损。

规则:
- 每整10分钟点, 若该点 24h 涨幅 Top1 合约未持有、不在禁仓期、且通过"创30天新高"确认 -> 开空
  (开仓价 = 该点已收盘 5m K 的 close, 即 openTime=T-5min)
- 创30天新高确认: 过去24h最高价 >= 过去30天最高价(窗口用已收盘 K, 不偷看未来)
- 最大持仓 24h 强制平仓(平仓价 = T+24h 已收盘价)
- 持仓 24h 内价格相对开仓价上涨 >= 100% -> 止损: 每根 5m K 判 open/high 是否触及止损价(开仓价x2),
  开盘跳空越过按开盘价成交, 盘中触及按止损价成交
- 禁仓: 盈利平仓 -> 免禁仓(可立即再开); 亏损平仓 -> 7 天(168h)禁仓
- K 线查 MongoDB(Workbuddy_5Min_Db/{SYMBOL}.BINANCE), 缺失自动下载补齐
输出: workbuddy/data/backtest_short_top1_20260701_20260811.csv
  列: symbol, openTime, openPrice, closeTime, closePrice, pnlPct, reason, cumPnl(等额累计)
"""
import os
import csv
import time
import json
import bisect
import requests
import pymongo
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

LOCAL_TZ = timezone(timedelta(hours=8))
EXCHANGE = "BINANCE"
DB_NAME = "Workbuddy_5Min_Db"
KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
SIGNAL_FILE = os.environ.get("SIGNAL_FILE",
                             "data/change_20251201_20260816/000000_top1_rise.csv")
OUT_FILE = os.environ.get("BT_OUT",
                          "data/change_20251201_20260816/_backtest_top1_short_all.csv")

STEP5 = 5 * 60 * 1000
DAY_MS = 24 * 3600 * 1000
BAN_MS = 7 * DAY_MS
STOP_FACTOR = float(os.environ.get("STOP_FACTOR", "1.8"))  # 固定止损(2026-08-17 定稿): 反向 80% = 价格 x1.8 止损; 样本外两段均优(+60/+4.5pp)
LOOKBACK_MS = 30 * DAY_MS  # 开仓确认: 过去三十天最高价窗口
CONFIRM_MS = 24 * 3600 * 1000  # 开仓确认: 过去 24h 窗口

_sd = os.environ.get("START_DT")
_ed = os.environ.get("END_DT")
T0 = datetime.strptime(_sd, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ) if _sd else datetime(2025, 12, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
T1 = datetime.strptime(_ed, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ) if _ed else datetime(2026, 8, 16, 0, 0, 0, tzinfo=LOCAL_TZ)      # 最后信号点
T_END = T1 + timedelta(hours=48)  # permit架构尾部修正(2026-08-18): 最后信号后仍有24h permit窗可开仓, 仓最久再持24h → 地平线需+48h(旧24h会截断尾部)
K_START_MS = int(T0.timestamp() * 1000) - STEP5
K_END_MS = int(T_END.timestamp() * 1000) - STEP5
T0_MS = int(T0.timestamp() * 1000)
T_END_MS = int(T_END.timestamp() * 1000)


def local_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


# ---------- 1. 读信号 ----------
# 信号元组: (t_ms, sym, top_change_pct) —— 涨幅用于 ≥MAX_TOP1_PCT 妖币过滤
signals = []
with open(SIGNAL_FILE, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        t = datetime.strptime(r["pointTimeLocal"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL_TZ)
        signals.append((int(t.timestamp() * 1000), r["topSymbol"], float(r["topChangePct"])))
signals.sort()
MAX_TOP1_PCT = float(os.environ.get("MAX_TOP1_PCT", "80"))   # 开仓过滤: 24h涨幅≥此值(妖币)跳过; 设0关闭
TAKER_N = int(os.environ.get("TAKER_N", "0"))                # 顶部转向确认: 近N根K加权主动买占比<50%才开; 0=关闭
RATIO6_MIN = float(os.environ.get("RATIO6_MIN", "-1"))       # 涨幅结构: <0=排除"近6h涨幅<0"(回落中); >0=只保留近6h/24h涨幅比例>=阈值(纯脉冲); 0=关闭
PULSE_TH = float(os.environ.get("PULSE_TH", "0.85"))        # 脉冲加仓阈值: 开仓时 ratio6>=此值 → 权重 PULSE_W
PULSE_W = float(os.environ.get("PULSE_W", "2"))             # 脉冲单权重(>1 启用, 如 2=双倍仓)
CIRCUIT_N = int(os.environ.get("CIRCUIT_N", "0"))           # 环境熔断: 环境分E回望天数(0=关闭)
CIRCUIT_M = int(os.environ.get("CIRCUIT_M", "3"))           # 环境熔断: 连续 M 天 E<0 才暂停开仓
OFF24_LO = float(os.environ.get("OFF24_LO", "0"))           # 距24h高点回落排除下限%(0=关闭)
OFF24_HI = float(os.environ.get("OFF24_HI", "0"))           # 距24h高点回落排除上限%
HIGH_AGE_MAX = float(os.environ.get("HIGH_AGE_MAX", "12"))  # 距24h最高点小时数上限(默认12): 高点太久的"接刀单"排除(0=关闭)
PUMP_ENABLE = int(os.environ.get("PUMP_ENABLE", "1"))       # 滚动24h新高止损: 4h缓冲后突破滚动24h最高(high) → 按前高止损
PUMP_DELAY_H = float(os.environ.get("PUMP_DELAY_H", "8"))
PUMP_VOL_RATIO = float(os.environ.get("PUMP_VOL_RATIO", "0"))   # pump_stop量能确认(2026-08-19): 突破当根成交额≥此值×近N根均额才止损(防无量假突破); 0=关闭
PUMP_VOL_N = int(os.environ.get("PUMP_VOL_N", "48"))           # 量能参考窗(根, 5m): 48=4h
TAKE_ENABLE = int(os.environ.get("TAKE_ENABLE", "0"))       # 移动止盈: 峰值浮盈≥TAKE_MIN% 后从峰值回落 TAKE_RETRACE pp → 锁利
TAKE_MIN = float(os.environ.get("TAKE_MIN", "15"))
TAKE_RETRACE = float(os.environ.get("TAKE_RETRACE", "15"))
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "80"))  # 固定止盈(2026-08-17 定稿): 空头浮盈达 X%(价格跌 X%) → 落袋, 接近24h极限
# ===== permit 架构(2026-08-18 定稿替换旧逻辑): 信号=开仓允许状态, 默认定稿兼容配置(结果与旧版一致 +1796.0%) =====
PERMIT = int(os.environ.get("PERMIT", "1"))  # 1=permit架构(信号→允许状态) 0=旧逻辑直接开仓
MIN_NEW_HIGH_AGE = float(os.environ.get("MIN_NEW_HIGH_AGE", "0"))  # 距24h最高点分钟数下限(定稿0=立即开; 实验30)
MAX_PERMIT_OPENS = int(os.environ.get("MAX_PERMIT_OPENS", "9"))  # 单permit最大开仓数(定稿9=首开+至多8次加仓, ddRatio最优区; 实验1~20)
ENTRY_H24 = int(os.environ.get("ENTRY_H24", "0"))  # 开仓24h最高价止损锚(定稿0=取消; 实验1启用)
FUNDING_MIN = float(os.environ.get("FUNDING_MIN", "0"))   # permit窗内开仓闸(2026-08-19 超定): 未持仓时取tick前最近funding ≥此值才开; 0=关闭
CVD_DIVERGE_W = int(os.environ.get("CVD_DIVERGE_W", "12"))   # CVD背离定入场点(定稿12=permit窗内未持仓时逐tick检查, 近12根主动买净额<前12根(背离成立)才开仓; 0=关闭)
OFF_HIGH_MAX = float(os.environ.get("OFF_HIGH_MAX", "0.15"))   # 开仓价距24h最高价降幅过滤(定稿0.15): (h24-px)/h24 > 阈值(追跌过深) → 不开; 0=关闭
OFF_HIGH_MIN = float(os.environ.get("OFF_HIGH_MIN", "0"))     # 最小降幅过滤(2026-08-20 超定): (h24-px)/h24 < 阈值(贴顶猜顶) → 不开, 等小幅回落确认; 0=关闭
ADD_ON_BREAK = int(os.environ.get("ADD_ON_BREAK", "1"))     # 加仓(定稿1): permit确认后PUMP_DELAY_H(8h)内破滚动24h前高且符合开仓条件 → 同权重加仓(金字塔); 8h后pump_stop止损照常; 0=关闭
# permit 结束规则: ①距起始24h且无未平仓 ②期内累计盈亏>0 ③已平满 MAX_PERMIT_OPENS 笔 ④expire/pump_stop 平仓 → 立即结束; 结束按总盈亏禁仓(总亏7天/总盈免)
sig_by_sym = {}   # sym -> [t_ms, ...] 按时间升序(用于模拟平均盈亏回放)
for t_ms, sym, _chg in signals:
    sig_by_sym.setdefault(sym, []).append(t_ms)
first_sig = {}
last_sig = {}
for t_ms, sym, _chg in signals:
    if sym not in first_sig:
        first_sig[sym] = t_ms
    last_sig[sym] = t_ms
print(f"信号点: {len(signals)} 个, 涉及币种 {len(set(s for _, s, _ in signals))} 个 | 妖币过滤阈值 {MAX_TOP1_PCT}%")

# ---------- 1.5 环境分 E(熔断用): 全市场 Top1 信号后 24h 平均表现 ----------
env_by_day = {}      # day_ms -> E (该天环境分: 过去 N 天已完成信号的 24h 表现平均)
if CIRCUIT_N:
    _cli0 = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
    _db0 = _cli0[DB_NAME]
    sig24 = []       # [(t_ms, pnl24h)]
    for t_ms, sym, _chg in signals:
        _kc = _db0[f"{sym.upper()}.BINANCE"]
        _k0 = _kc.find_one({"openTime": {"$lte": t_ms - STEP5, "$gte": t_ms - STEP5 - STEP5}}, sort=[("openTime", -1)])
        _k1 = _kc.find_one({"openTime": {"$lte": t_ms - STEP5 + DAY_MS, "$gte": t_ms - STEP5 + DAY_MS - STEP5}}, sort=[("openTime", -1)])
        if _k0 and _k1 and _k0["close"] > 0:
            sig24.append((t_ms, (_k0["close"] - _k1["close"]) / _k0["close"] * 100))
    sig24.sort()
    _sig_t = [x[0] for x in sig24]
    _sig_p = [x[1] for x in sig24]
    from bisect import bisect_left as _bl, bisect_right as _br
    _day0 = T0_MS // DAY_MS
    _dayN = (T_END_MS + DAY_MS) // DAY_MS
    for _d in range(_day0, _dayN + 1):
        _day_ms = _d * DAY_MS
        _lo = _bl(_sig_t, _day_ms - CIRCUIT_N * DAY_MS)          # 信号在 [day-Nd, day]
        _hi = _br(_sig_t, _day_ms)
        # 只算已完成 24h 的信号: t + DAY <= day_end → t <= day_ms (上面 _hi 已限制 t<=day_ms)
        _pool = _sig_p[_lo:_hi]
        if _pool:
            env_by_day[_day_ms] = sum(_pool) / len(_pool)
    print(f"环境分计算完成: {len(env_by_day)} 天 | 回望 {CIRCUIT_N} 天", flush=True)

def circuit_on(tick):
    """熔断: 当天往前连续 CIRCUIT_M 天(含当天)环境分都 < 0 → 暂停开仓。缺数据天视为非负(不熔断)。"""
    if not CIRCUIT_N:
        return False
    _day = tick // DAY_MS
    for _k in range(CIRCUIT_M):
        _e = env_by_day.get((_day - _k) * DAY_MS)
        if _e is None or _e >= 0:
            return False
    return True

cli = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
db = cli[DB_NAME]
close_maps = {}
series = {}  # sym -> (有序 openTime 列表, 对应 high 列表), 供创30天新高确认窗口 max


# ---------- funding 开仓闸(2026-08-19 超定): ef_*.csv 资金费率, 供 permit 窗内开仓判断 ----------
EF_DIR = os.path.join("data", "change_20251201_20260816")
EF_CACHE = {}


def ef_load(sym):
    if sym in EF_CACHE:
        return EF_CACHE[sym]
    p = os.path.join(EF_DIR, f"ef_{sym}.csv")
    if not os.path.exists(p):
        EF_CACHE[sym] = None
        return None
    rows = []
    for r in csv.DictReader(open(p, encoding="utf-8-sig")):
        try:
            fr = float(r["funding_rate"]) if r["funding_rate"] else None
            if fr is not None:
                rows.append((int(r["ts_ms"]), fr))
        except Exception:
            pass
    rows.sort()
    EF_CACHE[sym] = rows if rows else None
    return EF_CACHE[sym]


def funding_at(sym, t_ms):
    """t 时刻(含)之前最近一次 funding_rate; 无数据返回 None(闸不开, 保守)"""
    rows = ef_load(sym)
    if not rows:
        return None
    i = bisect.bisect_right(rows, (t_ms, 1e18)) - 1
    if i < 0:
        return None
    return rows[i][1]


def cvd_diverged(sym, t_ms, W):
    """CVD 主动买卖背离(2026-08-19): 近W根主动买净额 < 前W根(买盘衰竭/派发) → 背离成立(True, 可开)。
    delta = 2×takerBuyQuoteVolume − quoteVolume; 数据不足 2W 根 → 放行(True, 不误杀新币)。"""
    cm = close_maps.get(sym, {})
    ots = series.get(sym, (None, None))[0]
    if not cm or not ots:
        return True
    i = bisect.bisect_right(ots, t_ms - STEP5)      # 最后一根已收盘K之后
    lo2 = i - 2 * W
    if lo2 < 0:
        return True                                  # 上市不足 2W 根, 无法判断 → 放行
    rec = ref = 0.0
    for _j in range(i - W, i):
        _k = cm[ots[_j]]
        rec += 2 * float(_k.get("tbqv", 0.0) or 0.0) - float(_k.get("qv", 0.0) or 0.0)
    for _j in range(lo2, i - W):
        _k = cm[ots[_j]]
        ref += 2 * float(_k.get("tbqv", 0.0) or 0.0) - float(_k.get("qv", 0.0) or 0.0)
    return rec < ref


def fetch_all(sym, start_ms, end_ms):
    """拉K线; 3次重试失败 → 代理健康检查, 挂了循环等恢复(2026-08-21 超指: 与信号脚本一致)"""
    out = []
    cursor = start_ms
    while cursor <= end_ms:
        kl = None
        for attempt in range(3):
            try:
                params = {"symbol": sym, "interval": "5m",
                          "startTime": cursor, "endTime": end_ms, "limit": 1000}
                r = requests.get(KLINE_URL, params=params, timeout=30)
                r.raise_for_status()
                kl = r.json()
                break
            except Exception as e:
                if attempt == 2:
                    # 代理自愈: 连续失败先查代理, 挂了循环等恢复再整体重试
                    if not check_proxy():
                        print(f"  ⚠️ [fetch] {sym} 失败且代理不可用, 暂停等代理恢复...", flush=True)
                        while not check_proxy():
                            time.sleep(60)
                        print(f"  ✅ 代理恢复, 重试 {sym}", flush=True)
                        attempt = -1   # 重置重试
                        continue
                    print(f"  [fetch] {sym} 重试3次仍失败: {e}", flush=True)
                    raise
                time.sleep(3)
        if not kl:
            break
        out.extend(kl)
        nxt = kl[-1][0] + STEP5
        if nxt <= cursor:
            break
        cursor = nxt
    return out


def upsert(kc, kl):
    from pymongo import UpdateOne
    docs = []
    seen = set()
    for k in kl:
        ot = k[0]
        if ot in seen:
            continue
        seen.add(ot)
        docs.append({
            "openTime": int(ot),
            "openTimeLocal": local_str(ot),
            "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
            "volume": float(k[5]), "closeTime": int(k[6]),
            "closeTimeLocal": local_str(k[6]),
            "quoteVolume": float(k[7]), "trades": int(k[8]),
            "takerBuyBaseVolume": float(k[9]), "takerBuyQuoteVolume": float(k[10]),
        })
    if docs:
        kc.bulk_write([UpdateOne({"openTime": d["openTime"]}, {"$set": d}, upsert=True)
                       for d in docs], ordered=False)


PROBE_EPOCH = 1577836800000  # 2020-01-01 UTC, 足够早 → 返回该币最早可用K
PROXY_CHECK_URL = "https://fapi.binance.com/fapi/v1/time"


def check_proxy():
    """轻量代理健康检查(2026-08-21 超指: 与信号脚本一致): fapi time 200 = 通"""
    try:
        r = requests.get(PROXY_CHECK_URL, timeout=8)
        return r.status_code == 200
    except Exception:
        return False


def probe_listing(sym):
    """探测币种上市首根K时间: startTime=2020-01-01 limit=10(单页), 最多重试3次。
    返回首根K的 openTime(毫秒); 失败/无数据返回 None(调用方兜底)。
    代理自愈(2026-08-21): 连续失败先查代理, 挂了循环等恢复再重试。"""
    for attempt in range(3):
        try:
            params = {"symbol": sym, "interval": "5m", "startTime": PROBE_EPOCH, "limit": 10}
            r = requests.get(KLINE_URL, params=params, timeout=30)
            r.raise_for_status()
            kl = r.json()
            if kl:
                return int(kl[0][0])
            print(f"  [probe] {sym} 返回空(无历史数据)", flush=True)
            return None
        except Exception as e:
            if attempt == 2:
                if not check_proxy():
                    print(f"  ⚠️ [probe] {sym} 失败且代理不可用, 暂停等代理恢复...", flush=True)
                    while not check_proxy():
                        time.sleep(60)
                    print(f"  ✅ 代理恢复, 重试 {sym} probe", flush=True)
                    attempt = -1   # 重置重试
                    continue
                print(f"  [probe] {sym} 重试3次仍失败: {e}", flush=True)
                return None
            time.sleep(2)
    return None


def build_cm(kc, start_ms, end_ms):
    """从库里读 [start_ms, end_ms] 的K构建 cm, 返回 (cm, ots, db_start, db_end)"""
    cm = {}
    for k in kc.find({"openTime": {"$gte": start_ms, "$lte": end_ms}},
                     {"openTime": 1, "open": 1, "high": 1, "low": 1, "close": 1,
                      "takerBuyQuoteVolume": 1, "quoteVolume": 1}):
        cm[k["openTime"]] = {"open": k["open"], "high": k["high"], "low": k["low"], "close": k["close"],
                             "tbqv": k.get("takerBuyQuoteVolume", 0.0),
                             "qv": k.get("quoteVolume", 0.0)}
    ots = sorted(cm.keys())
    return cm, ots, (ots[0] if ots else None), (ots[-1] if ots else None)


def ensure_close_map(sym):
    """按币所需 K 区间 [首上榜-锚窗口-5min, 末上榜+24h-5min] 保证数据完整。
    流程(用户思路):
      1. probe_listing 探测上市首根K时间(重试3次), 失败用窗口起点兜底;
      2. 所需区间 need_start=max(上市时间, 窗口起点), need_end=窗口终点;
      3. 对比库内已有起止, 分[头部][尾部]两段补拉(各重试3次);
      4. 校验根数完整 → 完成; 仍缺(中段洞) → 全量拉兜底; 仍不完整 → 标记 skip。"""
    if sym in close_maps:
        return close_maps[sym]
    kc = db[f"{sym.upper()}.{EXCHANGE}"]
    kc.create_index("openTime", unique=True)
    raw_start = first_sig[sym] - LOOKBACK_MS - STEP5   # 理论窗口起点(不管上市)
    end_ms = last_sig[sym] + 2 * DAY_MS - STEP5   # permit尾(2026-08-18): 最后信号+24h permit窗可开仓 + 24h最大持仓 = last_sig+48h
    listing = probe_listing(sym)                       # 1. 上市首根K时间
    need_start = max(raw_start, listing) if listing else raw_start
    cm, ots, db_start, db_end = build_cm(kc, raw_start, end_ms)
    head_gap = db_start is None or db_start > need_start   # 头部缺/空 → 需补头部
    tail_gap = db_end is None or db_end < end_ms           # 尾部缺 → 需补尾部
    # 3. 分首尾两段补拉(各重试3次, fetch_all 内部有3次重试)
    for attempt in range(3):
        changed = False
        if head_gap:
            seg_end = (db_start - STEP5) if db_start else end_ms
            try:
                kl = fetch_all(sym, need_start, seg_end)
                if kl:
                    upsert(kc, kl)
                    cm, ots, db_start, db_end = build_cm(kc, raw_start, end_ms)
                    head_gap = db_start is None or db_start > need_start
                    changed = True
                print(f"  [fill] {sym} 补头部 [{local_str(need_start)} ~ {local_str(seg_end)}] 返回 {len(kl)} 根", flush=True)
            except Exception as e:
                print(f"  [fill] {sym} 头部补拉失败: {e}", flush=True)
                head_gap = False  # 补不上 → 放弃头部, 走兜底
        if tail_gap:
            seg_start = (db_end + STEP5) if db_end else need_start
            try:
                kl = fetch_all(sym, seg_start, end_ms)
                if kl:
                    upsert(kc, kl)
                    cm, ots, db_start, db_end = build_cm(kc, raw_start, end_ms)
                    tail_gap = db_end is None or db_end < end_ms
                    changed = True
                print(f"  [fill] {sym} 补尾部 [{local_str(seg_start)} ~ {local_str(end_ms)}] 返回 {len(kl)} 根", flush=True)
            except Exception as e:
                print(f"  [fill] {sym} 尾部补拉失败: {e}", flush=True)
                tail_gap = False
        expect = (end_ms - need_start) // STEP5 + 1
        if not head_gap and not tail_gap and len(cm) >= expect:
            break
        if not changed or attempt == 2:
            break
    # 4. 最终校验: 根数不足(中段洞) → 全量拉兜底
    expect = (end_ms - need_start) // STEP5 + 1
    fb_ok = True
    if not ots or len(cm) < expect:
        fb_ok = False
        try:
            kl = fetch_all(sym, need_start, end_ms)
            if kl:
                upsert(kc, kl)
                cm, ots, db_start, db_end = build_cm(kc, raw_start, end_ms)
                print(f"  [fill] {sym} 全量兜底 返回 {len(kl)} 根", flush=True)
                fb_ok = True
        except Exception as e:
            print(f"  [fill] {sym} 全量兜底失败: {e}", flush=True)
    # 4.5 防probe失败假阴性(2026-08-19, aigensynusdt案例): probe失败→need_start回退raw_start→expect含上市前不存在的K
    #     → 仅在全量兜底成功后, 数据实际起点 ots[0] 晚于 need_start(上市晚), 用 ots[0] 重算期望根数, 完整即放行
    if fb_ok and ots and len(cm) < expect and ots[0] > need_start:
        expect2 = (end_ms - ots[0]) // STEP5 + 1
        if len(cm) >= expect2:
            print(f"  [ok] {sym} 数据起点{local_str(ots[0])}>need_start(上市晚/probe失败), {len(cm)}/{expect2}完整, 放行", flush=True)
            series[sym] = (ots, [cm[ot]["high"] for ot in ots])
            close_maps[sym] = cm
            return cm
    if not ots or len(cm) < expect:
        print(f"  [skip] {sym} 仍缺失 {len(cm)}/{expect}, 标记跳过", flush=True)
        close_maps[sym] = None
        return None
    series[sym] = (ots, [cm[ot]["high"] for ot in ots])
    close_maps[sym] = cm
    return cm


def price_at(cm, t_ms):
    """t 时刻可用价格 = openTime = t-5min 的 K close"""
    k = cm.get(t_ms - STEP5)
    return k["close"] if k else None


def confirm_new_high(sym, t_ms):
    """开仓确认: 过去 24h max high >= 过去 30 天 max high(等价: 最近24h创30天新高)。
    窗口用已收盘 K(openTime <= t-5min), 不偷看未来。
    数据不足 30 天(上市晚) → 从上市以来到 T 取 max high。"""
    ots, highs = series[sym]
    hi = bisect.bisect_right(ots, t_ms - STEP5) - 1   # 最后一根已收盘 K
    if hi < 0:
        return False
    lo1 = bisect.bisect_left(ots, t_ms - CONFIRM_MS)
    lo15 = bisect.bisect_left(ots, t_ms - LOOKBACK_MS)
    h1 = max(highs[lo1:hi + 1])
    h15 = max(highs[lo15:hi + 1])
    return h1 >= h15


def entry_max_high(sym, t_ms, px):
    """追踪止损锚: 入场前24h最高价(已收盘K), 保底开仓价×1.01(防止入场价=前高时必损)。"""
    ots, highs = series.get(sym, (None, None))
    if not ots:
        return px * 1.01
    hi = bisect.bisect_right(ots, t_ms - STEP5) - 1
    lo = bisect.bisect_left(ots, t_ms - DAY_MS)
    if hi < lo:
        return px * 1.01
    return max(max(highs[lo:hi + 1]), px * 1.01)


def entry_peak24(sym, t_ms):
    """开仓前24h实体最高价: max over [t-24h, t-5min] 的 max(open, close)。用于二次冲高止损的 H1 初始锚。"""
    cm = close_maps.get(sym, {})
    ots, _h = series.get(sym, (None, None))
    if not cm or not ots:
        return None
    hi = bisect.bisect_right(ots, t_ms - STEP5) - 1
    lo = bisect.bisect_left(ots, t_ms - DAY_MS)
    if hi < lo:
        return None
    best = None
    for i in range(lo, hi + 1):
        k = cm[ots[i]]
        ent = max(k["open"], k["close"])
        if best is None or ent > best:
            best = ent
    return best


def build_pq24(sym, t_ms):
    """构建开仓前24h high 的单调递减队列(队头=当前24h窗口最大, 供滚动新高止损)。"""
    from collections import deque
    cm = close_maps.get(sym, {})
    ots, _h = series.get(sym, (None, None))
    q = deque()
    if not cm or not ots:
        return q
    hi = bisect.bisect_right(ots, t_ms - STEP5) - 1
    lo = bisect.bisect_left(ots, t_ms - DAY_MS)
    for i in range(max(lo, 0), hi + 1):
        ent = cm[ots[i]]["high"]
        while q and q[-1][1] <= ent:
            q.pop()
        q.append((ots[i], ent))
    return q


def taker_buy_ratio_ok(sym, t_ms, n):
    """顶部转向确认: 信号点前 n 根已收盘 K 的加权主动买占比 takerBuyRatio < 50% 才放行(买盘衰竭=派发)。
    加权: 近端线性权重(最新一根权重 n, 最早 1), tbr = Σ(w·tbqv) / Σ(w·qv)。
    数据缺失(qv 全 0 或 K 不足) → 返回 False(不开仓, 保守)。"""
    cm = close_maps.get(sym) or {}
    ots = series.get(sym, (None, None))[0]
    if not cm or not ots:
        return False
    hi = bisect.bisect_right(ots, t_ms - STEP5) - 1          # 最后一根已收盘 K
    lo = hi - n + 1
    if lo < 0:
        return False
    wtb = wqv = 0.0
    for i in range(lo, hi + 1):
        k = cm[ots[i]]
        w = i - lo + 1
        wtb += w * float(k.get("tbqv", 0.0) or 0.0)
        wqv += w * float(k.get("qv", 0.0) or 0.0)
    if wqv <= 0:
        return False
    return wtb / wqv < 0.50


def sim_avg_pnl(sym, t_ms):
    """回放过去一个月 [t_ms-30天, t_ms) 该币的模拟交易(按现有规则: 创30天新高确认 + 持仓24h + 100%止损
    + 盈利免禁仓/亏损禁7天), 返回模拟平均盈亏%。无模拟交易或数据不足返回 None(不拦截)。
    实际开仓前调用: 平均盈亏 > 0 才允许开仓。"""
    win_start = t_ms - LOOKBACK_MS
    sim_sigs = [s for s in sig_by_sym.get(sym, []) if win_start <= s < t_ms]
    if not sim_sigs:
        return None
    cm = close_maps.get(sym) or {}
    ots = series.get(sym, (None, None))[0]
    if not cm or not ots:
        return None
    pnls = []
    hold_until = 0      # 模拟持仓结束时间(止损/到期时刻)
    sim_ban = 0         # 模拟禁仓截止(亏损平仓后 7 天)
    for s in sim_sigs:
        if s < hold_until or s < sim_ban:
            continue
        if not confirm_new_high(sym, s):
            continue
        px = price_at(cm, s)
        if px is None:
            continue
        stop_px = px * STOP_FACTOR
        lo = bisect.bisect_left(ots, s + STEP5)
        hi = bisect.bisect_right(ots, s + DAY_MS - STEP5) - 1
        exit_px, exit_t = None, s + DAY_MS
        if lo <= hi:
            for i in range(lo, hi + 1):
                k = cm[ots[i]]
                if k["open"] >= stop_px:              # 跳空越过止损
                    exit_px, exit_t = k["open"], ots[i] + STEP5
                    break
                if k["high"] >= stop_px:              # 盘中触及止损
                    exit_px, exit_t = stop_px, ots[i] + STEP5
                    break
        if exit_px is None:                           # 未止损 → 24h 到期
            kk = cm.get(s + DAY_MS - STEP5)
            if kk is None:
                continue                              # 到期 K 缺失, 跳过该模拟
            exit_px = kk["close"]
        pnl = (px - exit_px) / px * 100
        pnls.append(pnl)
        sim_ban = exit_t + BAN_MS if pnl <= 0 else 0
        hold_until = exit_t
    if not pnls:
        return None
    return sum(pnls) / len(pnls)


# ---------- 3. 模拟: 按币推进(2026-08-21 超定架构) ----------
# 每个币信号互不影响(ban/permits/holdings 均 per-币, 信号每时刻唯一Top1) → 按币独立回测
# 按名称排序 → 依次 拉数据 → 信号时间序列回测 → 记录完成 → 中断后从未完成币重测
from collections import defaultdict as _dd
sig_by_sym = _dd(list)
for _t, _s, _c in signals:
    sig_by_sym[_s].append((_t, _c))
for _s in sig_by_sym:
    sig_by_sym[_s].sort()
syms_all = sorted(sig_by_sym.keys())
missing_syms = []   # 按币模式每币独立 ensure, 无全局缺失

PROGRESS_FILE = os.environ.get("BT_PROGRESS",
                               os.path.join(os.path.dirname(OUT_FILE), "_bt_progress.json"))
if os.path.exists(PROGRESS_FILE):
    try:
        progress = json.load(open(PROGRESS_FILE, encoding="utf-8"))
    except Exception:
        progress = {}
else:
    progress = {}

trades_all = []
n_open = n_stop = n_expire = n_cancel = 0

for _si, sym in enumerate(syms_all):
    if progress.get(sym) == "ok":
        continue
    sigs = sig_by_sym[sym]
    # 1) 单币 K 线(ensure 缓存仅此币, 处理完释放 → 内存 <100MB, GC 快)
    cm0 = ensure_close_map(sym)
    if cm0 is None:
        progress[sym] = "error"
        with open(PROGRESS_FILE, "w", encoding="utf-8") as _f:
            json.dump(progress, _f, ensure_ascii=False)
        print(f"[{sym}] K线缺失, 标 error, 跳过", flush=True)
        continue
    # 2) 该币时间轴: [首信号, 末信号+48h](permit尾24h + 持仓24h)
    t_lo = sigs[0][0]
    t_hi = sigs[-1][0] + 2 * DAY_MS
    # 3) 该币局部状态(独立)
    holdings = {}
    ban = {}
    pending = {}
    permits = {}
    trades = []
    sig_idx = 0
    tick = t_lo

    # ================= 单币时间轴逐 tick =================
    while tick <= t_hi:
        # --- 平仓检查(仅本币) ---
        if sym in holdings:
            h = holdings[sym]
            cm = close_maps.get(sym, {})
            k = cm.get(tick - STEP5)
            if k is not None:
                _hc = h.get("hold_max_high")
                if _hc is None or k["high"] > _hc:
                    h["hold_max_high"] = k["high"]
                reason = None
                if tick >= (h.get("permit_sig") or h["open_ms"]) + DAY_MS:
                    reason = "expire"
                    px = k["close"]
                elif TAKER_N:
                    stop_px = h["max_high"]
                    if k["open"] >= stop_px:
                        reason = "stop_loss"
                        px = k["open"]
                    elif k["high"] >= stop_px:
                        reason = "stop_loss"
                        px = stop_px
                    else:
                        h["max_high"] = max(h["max_high"], k["high"])
                elif PUMP_ENABLE:
                    q = h["pq"]
                    while q and q[0][0] <= tick - DAY_MS:
                        q.popleft()
                    h1_prev = q[0][1] if q else h["open_px"]
                    if k["high"] >= h1_prev:
                        h["broke_flag"] = True
                    _sig0 = h.get("permit_sig") or h["open_ms"]
                    pump_ok = tick - _sig0 >= PUMP_DELAY_H * 3600 * 1000
                    trigger = min(h["open_px"] * STOP_FACTOR,
                                  h.get("entry_h24") or h["open_px"] * STOP_FACTOR)
                    if pump_ok:
                        trigger = min(trigger, h1_prev)
                    _vol_ok = True
                    if PUMP_VOL_RATIO and pump_ok and trigger == h1_prev:
                        _ots4 = series.get(sym, (None, None))[0]
                        _cm4 = close_maps.get(sym, {})
                        if _ots4 and _cm4:
                            _i = bisect.bisect_left(_ots4, tick - STEP5)
                            _vs = [_cm4[_ots4[_j]].get("qv", 0.0) or 0.0
                                   for _j in range(max(0, _i - PUMP_VOL_N), _i)]
                            if _vs:
                                _avg = sum(_vs) / len(_vs)
                                if _avg > 0 and float(k.get("qv", 0.0) or 0.0) < PUMP_VOL_RATIO * _avg:
                                    _vol_ok = False
                    if k["open"] >= trigger:
                        if _vol_ok:
                            reason = "pump_stop" if (pump_ok and trigger == h1_prev) else "stop_loss"
                            px = k["open"]
                    elif k["high"] >= trigger:
                        if _vol_ok:
                            reason = "pump_stop" if (pump_ok and trigger == h1_prev) else "stop_loss"
                            px = trigger
                    if reason is None:
                        while q and q[-1][1] <= k["high"]:
                            q.pop()
                        q.append((tick, k["high"]))
                        _cur_p = (h["open_px"] - k["close"]) / h["open_px"] * 100
                        _peak_p = max(h["peak_profit"], (h["open_px"] - k["low"]) / h["open_px"] * 100)
                        h["peak_profit"] = _peak_p
                        if TAKE_ENABLE and _peak_p >= TAKE_MIN and (_peak_p - _cur_p) >= TAKE_RETRACE:
                            reason = "take_profit"
                            px = k["close"]
                        elif TAKE_PROFIT_PCT:
                            take_px = h["open_px"] * (1 - TAKE_PROFIT_PCT / 100)
                            if k["open"] <= take_px:
                                reason = "take_profit"
                                px = k["open"]
                            elif k["low"] <= take_px:
                                reason = "take_profit"
                                px = take_px
                else:
                    if TAKE_PROFIT_PCT:
                        take_px = h["open_px"] * (1 - TAKE_PROFIT_PCT / 100)
                        if k["open"] <= take_px:
                            reason = "take_profit"
                            px = k["open"]
                        elif k["low"] <= take_px:
                            reason = "take_profit"
                            px = take_px
                    if reason is None:
                        stop_px = min(h["open_px"] * STOP_FACTOR, h.get("entry_h24") or h["open_px"] * STOP_FACTOR)
                        if k["open"] >= stop_px:
                            reason = "stop_loss"
                            px = k["open"]
                        elif k["high"] >= stop_px:
                            reason = "stop_loss"
                            px = stop_px
                if reason:
                    pnl = (h["open_px"] - px) / h["open_px"] * 100
                    _op = round(h["open_px"], 6)
                    max_adv = ((h.get("hold_max_high") or _op) - _op) / _op * 100
                    trades.append([sym, local_str(h["open_ms"]), round(h["open_px"], 6),
                                   local_str(tick), round(px, 6), round(pnl, 4), reason,
                                   h.get("sim_type", "live"), h.get("weight", 1.0), round(max_adv, 4),
                                   local_str(h["permit_sig"]) if h.get("permit_sig") else ""])
                    if PERMIT and sym in permits:
                        _pp = permits[sym]
                        _pp["total_pnl"] += pnl * h.get("weight", 1.0)
                    for ad in h.get("adds", []):
                        pnl_a = (ad["open_px"] - px) / ad["open_px"] * 100
                        trades.append([sym, local_str(ad["open_ms"]), round(ad["open_px"], 6),
                                       local_str(tick), round(px, 6), round(pnl_a, 4), reason,
                                       ad.get("sim_type", "live"), ad.get("weight", 1.0), 0.0,
                                       local_str(h["permit_sig"]) if h.get("permit_sig") else ""])
                        if PERMIT and sym in permits:
                            _pp["total_pnl"] += pnl_a * ad.get("weight", 1.0)
                    if PERMIT and sym in permits:
                        if (_pp["total_pnl"] > 0 or _pp["n_open"] >= MAX_PERMIT_OPENS
                                or reason in ("expire", "pump_stop")):
                            if _pp["total_pnl"] < 0:
                                ban[sym] = max(ban.get(sym, 0), tick + BAN_MS)
                            del permits[sym]
                    else:
                        ban[sym] = 0 if pnl > 0 else max(ban.get(sym, 0), tick + BAN_MS)
                    del holdings[sym]
                    if reason == "stop_loss":
                        n_stop += 1
                    else:
                        n_expire += 1

        # --- 延迟入场(仅本币) ---
        if TAKER_N and sym in pending:
            sig_t = pending[sym]
            if tick - sig_t > DAY_MS:
                del pending[sym]
                n_cancel += 1
            else:
                _cmT = close_maps.get(sym, {})
                _kT = _cmT.get(tick - STEP5)
                if _kT is not None and taker_buy_ratio_ok(sym, tick, TAKER_N):
                    pxT = _kT["close"]
                    holdings[sym] = {"open_ms": tick, "open_px": pxT, "sim_type": "live",
                                     "max_high": entry_max_high(sym, tick, pxT),
                                     "hold_max_high": None, "adds": [], "broke_flag": False}
                    n_open += 1
                    del pending[sym]

        # --- 信号处理(仅本币) ---
        while sig_idx < len(sigs) and sigs[sig_idx][0] <= tick:
            t_ms, chg = sigs[sig_idx]
            sig_idx += 1
            if t_ms != tick:
                continue
            if sym in holdings or ban.get(sym, 0) > tick or sym in missing_syms:
                continue
            if PERMIT and sym in permits and permits[sym]["end_ms"] > tick:
                continue
            if circuit_on(tick):
                continue
            if MAX_TOP1_PCT and chg >= MAX_TOP1_PCT:
                ban[sym] = max(ban.get(sym, 0), tick + BAN_MS)
                continue
            if OFF24_LO and OFF24_HI:
                _cm2 = close_maps.get(sym, {})
                _px2 = price_at(_cm2, t_ms)
                _ots2, _h2 = series.get(sym, (None, None))
                if _px2 is None or not _ots2:
                    continue
                _hi2 = bisect.bisect_right(_ots2, t_ms - STEP5) - 1
                _lo2 = bisect.bisect_left(_ots2, t_ms - DAY_MS)
                if _hi2 < _lo2:
                    continue
                _h24 = max(_h2[_lo2:_hi2 + 1])
                _off = (_h24 - _px2) / _h24 * 100
                if OFF24_LO <= _off < OFF24_HI:
                    continue
            if HIGH_AGE_MAX:
                _cm3 = close_maps.get(sym, {})
                _ots3, _h3 = series.get(sym, (None, None))
                if _cm3 and _ots3:
                    _hi3 = bisect.bisect_right(_ots3, t_ms - STEP5) - 1
                    _lo3 = bisect.bisect_left(_ots3, t_ms - DAY_MS)
                    if _hi3 >= _lo3:
                        _imax = max(range(_lo3, _hi3 + 1), key=lambda i: _h3[i])
                        _age = (_ots3[_hi3] - _ots3[_imax]) / 3600000.0
                        if _age > HIGH_AGE_MAX:
                            continue
            r6 = None
            if RATIO6_MIN or PULSE_W > 1:
                _cm = ensure_close_map(sym)
                if _cm is None:
                    continue
                _px = price_at(_cm, t_ms)
                _k6 = _cm.get(t_ms - STEP5 - 6 * 3600 * 1000)
                _k24 = _cm.get(t_ms - STEP5 - DAY_MS)
                if _px is None or _k6 is None or _k24 is None or _k6["close"] <= 0 or _k24["close"] <= 0:
                    continue
                g6 = (_px - _k6["close"]) / _k6["close"]
                g24 = (_px - _k24["close"]) / _k24["close"]
                if g24 <= 0:
                    continue
                r6 = g6 / g24
            if RATIO6_MIN:
                if RATIO6_MIN < 0:
                    if r6 < 0:
                        continue
                elif r6 < RATIO6_MIN:
                    continue
            if not confirm_new_high(sym, t_ms):
                continue
            sim_type = "live"
            if not os.environ.get("SKIP_SIM_FILTER", "1"):
                sim_avg = sim_avg_pnl(sym, t_ms)
                if sim_avg is None:
                    sim_type = "sim"
                elif sim_avg <= 0:
                    continue
            cm = ensure_close_map(sym)
            if cm is None:
                continue
            px = price_at(cm, t_ms)
            if px is None:
                continue
            weight = PULSE_W if (PULSE_W > 1 and r6 is not None and r6 >= PULSE_TH) else 1.0
            if TAKER_N:
                if sym in pending:
                    continue
                pending[sym] = t_ms
            elif PERMIT:
                if sym in permits and permits[sym]["end_ms"] > tick:
                    continue
                permits[sym] = {"sig_ms": t_ms, "end_ms": t_ms + DAY_MS,
                                "n_open": 0, "total_pnl": 0.0, "weight": weight}
            else:
                holdings[sym] = {"open_ms": t_ms, "open_px": px, "sim_type": sim_type,
                                 "max_high": entry_max_high(sym, t_ms, px), "weight": weight,
                                 "pq": build_pq24(sym, t_ms), "pulled": False,
                                 "peak_profit": 0.0, "hold_max_high": None, "adds": [], "broke_flag": False}
                n_open += 1

        # --- permit 状态机(仅本币) ---
        if PERMIT and sym in permits:
            p = permits[sym]
            if tick >= p["end_ms"]:
                if sym not in holdings:
                    if p["total_pnl"] < 0:
                        ban[sym] = max(ban.get(sym, 0), tick + BAN_MS)
                    del permits[sym]
            elif p["n_open"] < MAX_PERMIT_OPENS and sym not in missing_syms:
                _in_hold = sym in holdings
                _skip_open = False
                if _in_hold:
                    # 加仓(2026-08-20 超定): 仅 permit 8h 内且破前高状态打开才可加仓; 否则本 tick 不开仓
                    # 注意: 不能 continue(while tick 循环内会跳过 tick+=STEP5 死循环), 用 _skip_open 标志
                    if not ADD_ON_BREAK or tick - p["sig_ms"] >= PUMP_DELAY_H * 3600 * 1000:
                        _skip_open = True
                    elif not holdings[sym].get("broke_flag"):
                        _skip_open = True
                if not _skip_open:
                    _allow_open = True
                    if FUNDING_MIN:
                        _fr = funding_at(sym, tick)
                        if _fr is None or _fr < FUNDING_MIN:
                            _allow_open = False
                    if _allow_open and CVD_DIVERGE_W:
                        if not cvd_diverged(sym, tick, CVD_DIVERGE_W):
                            _allow_open = False
                    if _allow_open and MIN_NEW_HIGH_AGE:
                        _ots8, _h8 = series.get(sym, (None, None))
                        if _ots8:
                            _hi8 = bisect.bisect_right(_ots8, tick - STEP5) - 1
                            _lo8 = bisect.bisect_left(_ots8, tick - DAY_MS)
                            if _hi8 >= _lo8:
                                _imax8 = max(range(_lo8, _hi8 + 1), key=lambda i: _h8[i])
                                _age8 = (_ots8[_hi8] - _ots8[_imax8]) / 60000.0
                                if _age8 < MIN_NEW_HIGH_AGE:
                                    _allow_open = False
                    if _allow_open:
                        cm = ensure_close_map(sym)
                        if cm is not None:
                            px = price_at(cm, tick)
                            if px is not None:
                                _pass_open = True
                                if OFF_HIGH_MAX:
                                    _ots9, _h9 = series.get(sym, (None, None))
                                    if _ots9:
                                        _hi9 = bisect.bisect_right(_ots9, tick - STEP5) - 1
                                        _lo9 = bisect.bisect_left(_ots9, tick - DAY_MS)
                                        if _hi9 >= _lo9:
                                            _h24_9 = max(_h9[_lo9:_hi9 + 1])
                                            if _h24_9 > 0:
                                                _drop9 = (_h24_9 - px) / _h24_9
                                                if _drop9 > OFF_HIGH_MAX:
                                                    _pass_open = False
                                                elif OFF_HIGH_MIN and _drop9 < OFF_HIGH_MIN:
                                                    _pass_open = False
                                if _pass_open:
                                    if ENTRY_H24:
                                        _ots7, _h7 = series.get(sym, (None, None))
                                        _hi7 = bisect.bisect_right(_ots7, tick - STEP5) - 1
                                        _lo7 = bisect.bisect_left(_ots7, tick - DAY_MS)
                                        entry_h24 = max(_h7[_lo7:_hi7 + 1]) if (_ots7 and _hi7 >= _lo7) else px * STOP_FACTOR
                                    else:
                                        entry_h24 = None
                                    if sym in holdings:
                                        holdings[sym]["adds"].append({"open_ms": tick, "open_px": px,
                                                                      "weight": p["weight"] / MAX_PERMIT_OPENS,
                                                                      "sim_type": "live", "hold_max_high": None})
                                        holdings[sym]["broke_flag"] = False
                                    else:
                                        holdings[sym] = {"open_ms": tick, "open_px": px, "sim_type": "live",
                                                         "max_high": entry_max_high(sym, tick, px),
                                                         "weight": p["weight"] / MAX_PERMIT_OPENS,
                                                         "pq": build_pq24(sym, tick),
                                                         "pulled": False, "peak_profit": 0.0, "hold_max_high": None,
                                                         "entry_h24": entry_h24, "permit_sig": p["sig_ms"], "adds": [],
                                                         "broke_flag": False}
                                    p["n_open"] += 1
                                    n_open += 1
        tick += STEP5

    # 4) 该币兜底: 仍持仓的按最后可得价格平仓(expire_forced)
    for s2 in list(holdings.keys()):
        h = holdings[s2]
        cm = close_maps.get(s2, {})
        ks = sorted(cm.keys())
        if not ks:
            continue
        px = cm[ks[-1]]["close"]
        pnl = (h["open_px"] - px) / h["open_px"] * 100
        _op = round(h["open_px"], 6)
        max_adv = (h.get("hold_max_high", _op) - _op) / _op * 100
        trades.append([s2, local_str(h["open_ms"]), round(h["open_px"], 6),
                       local_str(ks[-1]), round(px, 6), round(pnl, 4), "expire_forced",
                       h.get("sim_type", "live"), h.get("weight", 1.0), round(max_adv, 4),
                       local_str(h["permit_sig"]) if h.get("permit_sig") else ""])
        for ad in h.get("adds", []):
            pnl_a = (ad["open_px"] - px) / ad["open_px"] * 100
            trades.append([s2, local_str(ad["open_ms"]), round(ad["open_px"], 6),
                           local_str(ks[-1]), round(px, 6), round(pnl_a, 4), "expire_forced",
                           ad.get("sim_type", "live"), ad.get("weight", 1.0), 0.0,
                           local_str(h["permit_sig"]) if h.get("permit_sig") else ""])
        del holdings[s2]
        n_expire += 1

    # 5) 记录完成 + 释放单币缓存
    progress[sym] = "ok"
    with open(PROGRESS_FILE, "w", encoding="utf-8") as _f:
        json.dump(progress, _f, ensure_ascii=False)
    trades_all.extend(trades)
    close_maps.pop(sym, None)
    series.pop(sym, None)
    if (_si + 1) % 20 == 0 or _si == len(syms_all) - 1:
        print(f"[进度] {_si + 1}/{len(syms_all)} 币 | 累计开仓 {n_open} | 当前 {sym}", flush=True)

print(f"\n回测完成: 开仓 {n_open}, 止损平仓 {n_stop}, 到期平仓 {n_expire}, 延迟入场取消 {n_cancel}")
print(f"完成币数: {len(progress)}/{len(syms_all)} | 输出: {OUT_FILE}", flush=True)

if not trades_all:
    # 断点续传: 所有币已 done → 无新交易 → 不覆盖 CSV(避免空文件毁掉已算结果), 也不除零
    print("⚠️ 所有币已完成(progress 全 ok), 未重新计算; CSV 保持不变", flush=True)
else:
    with open(OUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "openTime", "openPrice", "closeTime", "closePrice",
                "pnlPct", "reason", "signalType", "weight", "cumPnl", "maxDD", "maxAdvPct", "permitOpenTime",
                "ddRatio"])  # ddRatio = maxDD / 累计总pnl (2026-08-20 超定: 回撤收益比, 越小越好; cum<=0 时记0)
        cum = 0.0
        peak = 0.0
        max_dd = 0.0
        for t in sorted(trades_all, key=lambda r: r[1]):  # 按开仓时间排序
            cum += t[5] * t[8]                         # 加权本金口径: 盈亏 × 权重 累加
            if cum > peak:
                peak = cum
            dd = peak - cum
            if dd > max_dd:
                max_dd = dd
            _dd_ratio = (max_dd / cum) if cum > 0 else 0.0
            w.writerow([t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7], t[8], round(cum, 4), round(max_dd, 4), t[9], t[10], round(_dd_ratio, 4)])

    total = len(trades_all)
    wins = [t for t in trades_all if t[5] > 0]
    losses = [t for t in trades_all if t[5] <= 0]
    wtot = sum(t[8] for t in trades_all)
    avg = sum(t[5] * t[8] for t in trades_all) / wtot if wtot else 0
    avg_win = sum(t[5] * t[8] for t in wins) / sum(t[8] for t in wins) if wins else 0
    avg_loss = sum(t[5] * t[8] for t in losses) / sum(t[8] for t in losses) if losses else 0
    print(f"交易 {total} 笔(加权本金{wtot:.0f}) | 胜率 {len(wins)/total*100:.1f}% | 加权平均盈亏 {avg:.3f}% "
          f"| 平均盈利 {avg_win:.3f}% | 平均亏损 {avg_loss:.3f}%")
    if missing_syms:
        print(f"⚠️ 数据缺失未参与回测 {len(missing_syms)} 币: {missing_syms}", flush=True)
    else:
        print("✅ 数据完整性: 无因数据缺失未参与回测的币", flush=True)
    print(f"输出: {OUT_FILE}")
