"""做空 24h 涨幅 Top1 策略回测 (2026-07-01 00:00 -> 2026-08-11 23:30 信号期)。
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
                             "workbuddy/data/change_20260101_20260812/000000_top1_rise.csv")
OUT_FILE = os.environ.get("BT_OUT",
                          "workbuddy/data/change_20260101_20260812/_backtest_top1_short_all.csv")

STEP5 = 5 * 60 * 1000
DAY_MS = 24 * 3600 * 1000
BAN_MS = 7 * DAY_MS
STOP_FACTOR = float(os.environ.get("STOP_FACTOR", "1.8"))  # 固定止损(2026-08-17 定稿): 反向 80% = 价格 x1.8 止损; 样本外两段均优(+60/+4.5pp)
LOOKBACK_MS = 30 * DAY_MS  # 开仓确认: 过去三十天最高价窗口
CONFIRM_MS = 24 * 3600 * 1000  # 开仓确认: 过去 24h 窗口

_sd = os.environ.get("START_DT")
_ed = os.environ.get("END_DT")
T0 = datetime.strptime(_sd, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ) if _sd else datetime(2026, 1, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
T1 = datetime.strptime(_ed, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ) if _ed else datetime(2026, 8, 12, 0, 0, 0, tzinfo=LOCAL_TZ)      # 最后信号点
T_END = T1 + timedelta(hours=24)                           # 最后持仓到期
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
RATIO6_MIN = float(os.environ.get("RATIO6_MIN", "0"))       # 涨幅结构: <0=排除"近6h涨幅<0"(回落中); >0=只保留近6h/24h涨幅比例>=阈值(纯脉冲); 0=关闭
PULSE_TH = float(os.environ.get("PULSE_TH", "0.85"))        # 脉冲加仓阈值: 开仓时 ratio6>=此值 → 权重 PULSE_W
PULSE_W = float(os.environ.get("PULSE_W", "1"))             # 脉冲单权重(>1 启用, 如 2=双倍仓)
CIRCUIT_N = int(os.environ.get("CIRCUIT_N", "0"))           # 环境熔断: 环境分E回望天数(0=关闭)
CIRCUIT_M = int(os.environ.get("CIRCUIT_M", "3"))           # 环境熔断: 连续 M 天 E<0 才暂停开仓
OFF24_LO = float(os.environ.get("OFF24_LO", "0"))           # 距24h高点回落排除下限%(0=关闭)
OFF24_HI = float(os.environ.get("OFF24_HI", "0"))           # 距24h高点回落排除上限%
HIGH_AGE_MAX = float(os.environ.get("HIGH_AGE_MAX", "12"))  # 距24h最高点小时数上限(默认12): 高点太久的"接刀单"排除(0=关闭)
PUMP_ENABLE = int(os.environ.get("PUMP_ENABLE", "0"))       # 滚动24h新高止损: 4h缓冲后突破滚动24h最高(high) → 按前高止损
PUMP_DELAY_H = float(os.environ.get("PUMP_DELAY_H", "4"))
TAKE_ENABLE = int(os.environ.get("TAKE_ENABLE", "0"))       # 移动止盈: 峰值浮盈≥TAKE_MIN% 后从峰值回落 TAKE_RETRACE pp → 锁利
TAKE_MIN = float(os.environ.get("TAKE_MIN", "15"))
TAKE_RETRACE = float(os.environ.get("TAKE_RETRACE", "15"))
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "80"))  # 固定止盈(2026-08-17 定稿): 空头浮盈达 X%(价格跌 X%) → 落袋, 接近24h极限
# ===== permit 架构(2026-08-18 定稿替换旧逻辑): 信号=开仓允许状态, 默认定稿兼容配置(结果与旧版一致 +1796.0%) =====
PERMIT = int(os.environ.get("PERMIT", "1"))  # 1=permit架构(信号→允许状态) 0=旧逻辑直接开仓
MIN_NEW_HIGH_AGE = float(os.environ.get("MIN_NEW_HIGH_AGE", "0"))  # 距24h最高点分钟数下限(定稿0=立即开; 实验30)
MAX_PERMIT_OPENS = int(os.environ.get("MAX_PERMIT_OPENS", "1"))  # 单permit最大开仓数(定稿1=兼容旧逻辑; 实验3)
ENTRY_H24 = int(os.environ.get("ENTRY_H24", "0"))  # 开仓24h最高价止损锚(定稿0=取消; 实验1启用)
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


def fetch_all(sym, start_ms, end_ms):
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


def probe_listing(sym):
    """探测币种上市首根K时间: startTime=2020-01-01 limit=10(单页), 最多重试3次。
    返回首根K的 openTime(毫秒); 失败/无数据返回 None(调用方兜底)。"""
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
    end_ms = last_sig[sym] + DAY_MS - STEP5
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
    if not ots or len(cm) < expect:
        try:
            kl = fetch_all(sym, need_start, end_ms)
            if kl:
                upsert(kc, kl)
                cm, ots, db_start, db_end = build_cm(kc, raw_start, end_ms)
                print(f"  [fill] {sym} 全量兜底 返回 {len(kl)} 根", flush=True)
        except Exception as e:
            print(f"  [fill] {sym} 全量兜底失败: {e}", flush=True)
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


# ---------- 2. 预拉所有上榜币连续 5m K (3 并发) ----------
syms_all = sorted(set(s for _, s, _ in signals))
ok_syms = set()


def _ensure(sym):
    return ensure_close_map(sym) is not None


with ThreadPoolExecutor(max_workers=3) as ex:
    futs = {ex.submit(_ensure, s): s for s in syms_all}
    for fut in as_completed(futs):
        s = futs[fut]
        try:
            if fut.result():
                ok_syms.add(s)
        except Exception as e:
            print(f"[预拉失败] {s}: {e}", flush=True)
missing_syms = [s for s in syms_all if s not in ok_syms]
print(f"K 预拉完成: {len(ok_syms)}/{len(syms_all)} 币, 缺失 {len(missing_syms)}: {missing_syms[:10]}", flush=True)

# ---------- 3. 模拟 ----------
holdings = {}   # sym -> {open_ms, open_px}
ban = {}        # sym -> ban_until_ms
pending = {}    # sym -> signal_ms  (延迟入场候选: 24h 内等 tbr<50%)
permits = {}    # PERMIT=1: sym -> {sig_ms, end_ms, n_open, total_pnl, weight}
trades = []
sig_idx = 0
n_open = n_stop = n_expire = n_cancel = 0

tick = T0_MS
while tick <= T_END_MS:
    if tick % (1000 * STEP5) == 0:
        print(f"  [tick] {local_str(tick)} 持仓 {len(holdings)} 开仓 {n_open}", flush=True)
    # 平仓检查(先于开仓)
    for sym in list(holdings.keys()):
        h = holdings[sym]
        cm = close_maps.get(sym, {})
        k = cm.get(tick - STEP5)
        if k is None:
            continue
        # 追踪持仓期最高 high(供 maxAdvPct 输出): 含开仓当根 K, 区间=[开仓时刻, 平仓前最后一根]
        # maxAdvPct = 实际值(带符号): (最高high - 开仓价)/开仓价*100, 负值 = 空头浮盈
        _hc = h.get("hold_max_high")
        if _hc is None or k["high"] > _hc:
            h["hold_max_high"] = k["high"]
        reason = None
        if tick >= h["open_ms"] + DAY_MS:
            reason = "expire"
            px = k["close"]
        elif TAKER_N:
            # tbr 入场模式 → 追踪止损: 止损价 = 开仓以来最高价(初始=入场前24h最高, 保底开仓价×1.01)
            stop_px = h["max_high"]
            if k["open"] >= stop_px:      # 跳空越过 → 按开盘价成交
                reason = "stop_loss"
                px = k["open"]
            elif k["high"] >= stop_px:    # 触及持仓期最高点 → 按该点成交(创新高即损)
                reason = "stop_loss"
                px = stop_px
            else:
                h["max_high"] = max(h["max_high"], k["high"])   # 未触发 → 上移追踪锚
        elif PUMP_ENABLE:
            # 并行止损(先到先得, 2026-08-18): pump_stop(8h后前高) / 固定止损(open×SF) / 24h锚(ENTRY_H24) 取最低触发价
            q = h["pq"]
            while q and q[0][0] <= tick - DAY_MS:      # 滑出24h窗口外旧K
                q.popleft()
            h1_prev = q[0][1] if q else h["open_px"]   # 当前"过去24h"最高high(不含当前K)
            pump_ok = tick - h["open_ms"] >= PUMP_DELAY_H * 3600 * 1000
            trigger = min(h["open_px"] * STOP_FACTOR,
                          h.get("entry_h24") or h["open_px"] * STOP_FACTOR)
            if pump_ok:
                trigger = min(trigger, h1_prev)
            if k["open"] >= trigger:                   # 跳空越过 → 按开盘价成交
                reason = "pump_stop" if (pump_ok and trigger == h1_prev) else "stop_loss"
                px = k["open"]
            elif k["high"] >= trigger:                 # 盘中触及最低触发价 → 按该价成交
                reason = "pump_stop" if (pump_ok and trigger == h1_prev) else "stop_loss"
                px = trigger
            else:
                while q and q[-1][1] <= k["high"]:     # 未触发 → 加入当前K(单调递减)
                    q.pop()
                q.append((tick, k["high"]))
                # 移动止盈: 峰值浮盈≥TAKE_MIN 后从峰值回落 TAKE_RETRACE pp → 锁利平仓
                _cur_p = (h["open_px"] - k["close"]) / h["open_px"] * 100
                _peak_p = max(h["peak_profit"], (h["open_px"] - k["low"]) / h["open_px"] * 100)
                h["peak_profit"] = _peak_p
                if TAKE_ENABLE and _peak_p >= TAKE_MIN and (_peak_p - _cur_p) >= TAKE_RETRACE:
                    reason = "take_profit"
                    px = k["close"]
                elif TAKE_PROFIT_PCT:
                    # 固定止盈: 空头浮盈达 TAKE_PROFIT_PCT%(价格跌 X%) → 落袋(对称止损: 盘中触及按止盈价)
                    take_px = h["open_px"] * (1 - TAKE_PROFIT_PCT / 100)
                    if k["open"] <= take_px:      # 跳空低开越过 → 按开盘价成交
                        reason = "take_profit"
                        px = k["open"]
                    elif k["low"] <= take_px:     # 盘中触及 → 按止盈价成交
                        reason = "take_profit"
                        px = take_px
        else:
            # PUMP_ENABLE=0: 全期兜底止损(与 ENTRY_H24 锚并行, 先到原则); 固定止盈优先
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
            _op = round(h["open_px"], 6)   # 旧版口径: 分母用 round 后的开仓价(=CSV openPrice)
            max_adv = ((h.get("hold_max_high") or _op) - _op) / _op * 100
            trades.append([sym, local_str(h["open_ms"]), round(h["open_px"], 6),
                           local_str(tick), round(px, 6), round(pnl, 4), reason,
                           h.get("sim_type", "live"), h.get("weight", 1.0), round(max_adv, 4)])
            # 禁仓: PERMIT 模式平仓不禁仓(累计到 permit, 窗口结束统一判总盈亏); 旧逻辑平仓即判
            if PERMIT and sym in permits:
                _pp = permits[sym]
                _pp["total_pnl"] += pnl * h.get("weight", 1.0)
                if (_pp["total_pnl"] > 0 or _pp["n_open"] >= MAX_PERMIT_OPENS
                        or reason in ("expire", "pump_stop")):
                    # 结束规则②盈利收手 ③平满N笔 ④expire/pump_stop: 总亏禁7天, 总盈免禁
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
    # 延迟入场: 候选信号 24h 内等 tbr<50% 触发真正入场, 超时取消
    if TAKER_N:
        for sym in list(pending.keys()):
            sig_t = pending[sym]
            if tick - sig_t > DAY_MS:            # 24h 未触发 → 取消
                del pending[sym]
                n_cancel += 1
                continue
            cm = close_maps.get(sym, {})
            k = cm.get(tick - STEP5)
            if k is None:
                continue
            if not taker_buy_ratio_ok(sym, tick, TAKER_N):
                continue                          # tbr 未跌破 50%, 继续等
            px = k["close"]                       # 触发入场: 当前已收盘价
            if px is None:
                continue
            holdings[sym] = {"open_ms": tick, "open_px": px, "sim_type": "live",
                             "max_high": entry_max_high(sym, tick, px),
                             "hold_max_high": None}
            n_open += 1
            del pending[sym]
    # 信号检查: 满足现有规则 → 记为候选(等待 tbr<50% 延迟入场; TAKER_N=0 时直接开仓)
    while sig_idx < len(signals) and signals[sig_idx][0] <= tick:
        t_ms, sym, chg = signals[sig_idx]
        sig_idx += 1
        if t_ms != tick:
            continue
        if sym in holdings or ban.get(sym, 0) > tick or sym in missing_syms:
            continue
        if circuit_on(tick):                    # 环境熔断: 连续 M 天环境分<0, 暂停开仓
            continue
        if MAX_TOP1_PCT and chg >= MAX_TOP1_PCT:    # 妖币: 不开仓 + 该币禁仓7天(与亏损禁仓取截止最大)
            ban[sym] = max(ban.get(sym, 0), tick + BAN_MS)
            continue
        if OFF24_LO and OFF24_HI:
            # 排除"距24h高点回落 [OFF24_LO, OFF24_HI)%"的单(回调中做空接反弹)
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
            # 排除"距24h最高点时间过长"的接刀单: 24h窗口内最高high的K距当前已收盘K超过 N 小时
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
            # 涨幅结构: 近6h涨幅/24h涨幅比例 r6
            _cm = close_maps.get(sym, {})
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
                    continue        # 排除"回落中"(近6h在跌=接刀)
            elif r6 < RATIO6_MIN:
                continue            # 只保留纯脉冲(近6h贡献占比>=阈值)
        if not confirm_new_high(sym, t_ms):     # 开仓确认: 最近24h创30天新高
            continue
        # 模拟过滤: 过去一个月该币模拟平均盈亏 > 0 才开仓; 无模拟数据 → 模拟阶段(放行); SKIP_SIM_FILTER=1 跳过
        sim_type = "live"    # 实盘阶段: 模拟验证通过
        if not os.environ.get("SKIP_SIM_FILTER"):
            sim_avg = sim_avg_pnl(sym, t_ms)
            if sim_avg is None:
                sim_type = "sim"    # 模拟阶段: 过去一个月无模拟交易(空窗放行)
            elif sim_avg <= 0:
                continue            # 模拟验证未通过, 拦截
        cm = ensure_close_map(sym)   # 预拉已 ensure, 命中缓存
        if cm is None:
            continue
        px = price_at(cm, t_ms)
        if px is None:
            continue
        weight = PULSE_W if (PULSE_W > 1 and r6 is not None and r6 >= PULSE_TH) else 1.0
        if TAKER_N:
            if sym in pending:
                continue              # 已有候选, 不重复
            pending[sym] = t_ms       # 记为候选, 等 tbr<50% 延迟入场
        elif PERMIT:
            # permit 架构(定稿): 信号 → 开仓允许状态(24h), 期内独立开仓(≤MAX_PERMIT_OPENS, 不受禁仓限制)
            if sym in permits and permits[sym]["end_ms"] > tick:
                continue              # 已有活跃 permit
            permits[sym] = {"sig_ms": t_ms, "end_ms": t_ms + DAY_MS,
                            "n_open": 0, "total_pnl": 0.0, "weight": weight}
        else:
            holdings[sym] = {"open_ms": t_ms, "open_px": px, "sim_type": sim_type,
                             "max_high": entry_max_high(sym, t_ms, px), "weight": weight,
                             "pq": build_pq24(sym, t_ms), "pulled": False,
                             "peak_profit": 0.0, "hold_max_high": None}
            n_open += 1
    if PERMIT:
        # permit 状态机(定稿): 结束规则①距起始24h且无未平仓 ③平满MAX_PERMIT_OPENS笔; 期内开仓(age30m/24h锚可选)
        for sym in list(permits.keys()):
            p = permits[sym]
            if tick >= p["end_ms"]:
                if sym not in holdings:
                    if p["total_pnl"] < 0:          # 窗口结束: 总亏禁7天, 总盈免禁
                        ban[sym] = max(ban.get(sym, 0), tick + BAN_MS)
                    del permits[sym]
                continue
            if p["n_open"] >= MAX_PERMIT_OPENS or sym in holdings or sym in missing_syms:
                continue
            if MIN_NEW_HIGH_AGE:
                # 入场条件: 距24h最高点时间 > N 分钟才开(刚创新高等待确认)
                _ots8, _h8 = series.get(sym, (None, None))
                if _ots8:
                    _hi8 = bisect.bisect_right(_ots8, tick - STEP5) - 1
                    _lo8 = bisect.bisect_left(_ots8, tick - DAY_MS)
                    if _hi8 >= _lo8:
                        _imax8 = max(range(_lo8, _hi8 + 1), key=lambda i: _h8[i])
                        _age8 = (_ots8[_hi8] - _ots8[_imax8]) / 60000.0
                        if _age8 < MIN_NEW_HIGH_AGE:
                            continue
            cm = ensure_close_map(sym)
            if cm is None:
                continue
            px = price_at(cm, tick)
            if px is None:
                continue
            if ENTRY_H24:
                _ots7, _h7 = series.get(sym, (None, None))
                _hi7 = bisect.bisect_right(_ots7, tick - STEP5) - 1
                _lo7 = bisect.bisect_left(_ots7, tick - DAY_MS)
                entry_h24 = max(_h7[_lo7:_hi7 + 1]) if (_ots7 and _hi7 >= _lo7) else px * STOP_FACTOR
            else:
                entry_h24 = None
            holdings[sym] = {"open_ms": tick, "open_px": px, "sim_type": "live",
                             "max_high": entry_max_high(sym, tick, px),
                             "weight": p["weight"], "pq": build_pq24(sym, tick),
                             "pulled": False, "peak_profit": 0.0, "hold_max_high": None,
                             "entry_h24": entry_h24, "permit_sig": p["sig_ms"]}
            p["n_open"] += 1
            n_open += 1
    tick += STEP5

# 兜底: 模拟结束仍持仓的按最后可得价格平仓(标记 expire_forced)
for sym in list(holdings.keys()):
    h = holdings[sym]
    cm = close_maps.get(sym, {})
    ks = sorted(cm.keys())
    if not ks:
        continue
    px = cm[ks[-1]]["close"]
    pnl = (h["open_px"] - px) / h["open_px"] * 100
    _op = round(h["open_px"], 6)   # 旧版口径: 分母用 round 后的开仓价(=CSV openPrice)
    max_adv = (h.get("hold_max_high", _op) - _op) / _op * 100
    trades.append([sym, local_str(h["open_ms"]), round(h["open_px"], 6),
                   local_str(ks[-1]), round(px, 6), round(pnl, 4), "expire_forced",
                   h.get("sim_type", "live"), h.get("weight", 1.0), round(max_adv, 4)])
    del holdings[sym]
    n_expire += 1

print(f"\n回测完成: 开仓 {n_open}, 止损平仓 {n_stop}, 到期平仓 {n_expire}, 延迟入场取消 {n_cancel}")
if holdings:
    print("未平持仓:")
    for s, h in sorted(holdings.items()):
        end = last_sig[s] + DAY_MS - STEP5
        print(f"  {s} 开仓 {local_str(h['open_ms'])} 开价 {h['open_px']} | 最后上榜 {local_str(last_sig[s])} | K尾部 {local_str(end)}", flush=True)

with open(OUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["symbol", "openTime", "openPrice", "closeTime", "closePrice",
                "pnlPct", "reason", "signalType", "weight", "cumPnl", "maxDD", "maxAdvPct"])
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in sorted(trades, key=lambda r: r[1]):  # 按开仓时间排序
        cum += t[5] * t[8]                         # 加权本金口径: 盈亏 × 权重 累加
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
        w.writerow([t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7], t[8], round(cum, 4), round(max_dd, 4), t[9]])

total = len(trades)
wins = [t for t in trades if t[5] > 0]
losses = [t for t in trades if t[5] <= 0]
wtot = sum(t[8] for t in trades)
avg = sum(t[5] * t[8] for t in trades) / wtot if wtot else 0
avg_win = sum(t[5] * t[8] for t in wins) / sum(t[8] for t in wins) if wins else 0
avg_loss = sum(t[5] * t[8] for t in losses) / sum(t[8] for t in losses) if losses else 0
print(f"交易 {total} 笔(加权本金{wtot:.0f}) | 胜率 {len(wins)/total*100:.1f}% | 加权平均盈亏 {avg:.3f}% "
      f"| 平均盈利 {avg_win:.3f}% | 平均亏损 {avg_loss:.3f}%")
if missing_syms:
    print(f"⚠️ 数据缺失未参与回测 {len(missing_syms)} 币: {missing_syms}", flush=True)
else:
    print("✅ 数据完整性: 无因数据缺失未参与回测的币", flush=True)
print(f"输出: {OUT_FILE}")
