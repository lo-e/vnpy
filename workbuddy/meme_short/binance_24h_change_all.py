"""全量: 所有币安 USDT 永续合约 5m 整10分钟点 24h 涨跌幅 (结果只落 CSV)。

v2 优化: 计算阶段一次性取全量 K 进内存(不再逐点 find_one); 3 并发拉取; 断点续传(done.json)。
口径: close[T] = openTime = T-5min 的 5m K close; change% = (close[T]-close[T-24h])/close[T-24h]*100
时间点: 2026-07-01 00:00(北京) -> 当前最近的整 10 分钟点。
输出:   workbuddy/data/change_20260701_{end}/{symbol_lower}.csv
"""
import os
import json
import csv
import time
import requests
import pymongo
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import UpdateOne

EXCHANGE = "BINANCE"
DB_NAME = "Workbuddy_5Min_Db"
KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
SYMBOLS_FILE = "data/symbols_usdt_perp.json"
MAX_WORKERS = int(os.environ.get("BW", "3"))

LOCAL_TZ = timezone(timedelta(hours=8))  # 北京时间 UTC+8
STEP_5M_MS = 5 * 60 * 1000

START_DT = os.environ.get("START_DT", "2026-07-01 00:00")
END_DT = os.environ.get("END_DT", "")  # 空则动态到当前最近整10分
p_start = datetime.strptime(START_DT, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ)
if END_DT:
    p_end = datetime.strptime(END_DT, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ)
else:
    now_local = datetime.now(LOCAL_TZ)
    p_end = now_local - timedelta(minutes=now_local.minute % 10,
                                  seconds=now_local.second,
                                  microseconds=now_local.microsecond)
OUT_DIR = os.path.join("data", f"change_{p_start:%Y%m%d}_{p_end:%Y%m%d}")
os.makedirs(OUT_DIR, exist_ok=True)
DONE_FILE = os.environ.get("DONE_FILE", os.path.join(OUT_DIR, "_done.json"))  # 断点续传(默认进周期目录)

points = []
t = p_start
while t <= p_end:
    points.append(t)
    t += timedelta(minutes=10)

with open(SYMBOLS_FILE, encoding="utf-8") as f:
    SYMBOLS = json.load(f)


def auto_sync_symbols():
    """启动时自动校验/更新币种清单(只增不减, 2026-08-17 用户要求):
    exchangeInfo 拉当前 TRADING 的 USDT 永续 → 新币加入 json 并落盘, 下架币保留不删(防历史窗口重跑漏币)。
    拉取失败 → 打印警告并沿用现有清单, 不阻断任务。"""
    try:
        r = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=30)
        r.raise_for_status()
        info = r.json()
        cur = {s["symbol"] for s in info["symbols"]
               if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
               and s.get("status") == "TRADING"}
    except Exception as e:
        print(f"  [sync-symbols] exchangeInfo 拉取失败: {e}, 沿用现有清单", flush=True)
        return
    old = set(SYMBOLS)
    new_add = sorted(cur - old)    # 新币: 清单里没有的
    delisted = sorted(old - cur)   # 下架/停交易: 清单里有但现在不在(保留不删)
    if new_add:
        SYMBOLS.extend(new_add)
        with open(SYMBOLS_FILE, "w", encoding="utf-8") as f:
            json.dump(SYMBOLS, f, ensure_ascii=False, indent=1)
        print(f"  [sync-symbols] 新增 {len(new_add)} 币 → {SYMBOLS_FILE}: {new_add[:10]}{'...' if len(new_add) > 10 else ''}", flush=True)
    if delisted:
        print(f"  [sync-symbols] ⚠️ {len(delisted)} 币当前不在 TRADING 清单(保留不删, 兼容历史窗口): {delisted[:10]}{'...' if len(delisted) > 10 else ''}", flush=True)
    if not new_add and not delisted:
        print(f"  [sync-symbols] 清单与 exchangeInfo 一致, 无变化 ({len(SYMBOLS)} 币)", flush=True)


auto_sync_symbols()

print(f"时间点 {len(points)} 个 ({p_start:%Y-%m-%d %H:%M} -> {p_end:%H:%M}), 币种 {len(SYMBOLS)}, 并发 {MAX_WORKERS}", flush=True)
print(f"输出目录: {OUT_DIR}", flush=True)


def ms_to_local(ms):
    """统一规范: 存 tz-aware 北京 datetime(+08:00), pymongo 自动转 UTC 存储, 物理时刻正确"""
    return datetime.fromtimestamp(ms / 1000, tz=LOCAL_TZ)


def ms_to_utc_naive(ms):
    """读回比较统一用 naive UTC"""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def local_str(dt_local):
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")


def load_done():
    if os.path.exists(DONE_FILE):
        try:
            with open(DONE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_done(done):
    with open(DONE_FILE, "w", encoding="utf-8") as f:
        json.dump(done, f, ensure_ascii=False)


def fetch_all(sym, start_ms, end_ms):
    out = []
    cursor = start_ms
    while cursor <= end_ms:
        params = {"symbol": sym, "interval": "5m",
                  "startTime": cursor, "endTime": end_ms, "limit": 1000}
        r = requests.get(KLINE_URL, params=params, timeout=30)
        r.raise_for_status()
        kl = r.json()
        if not kl:
            break
        out.extend(kl)
        nxt = kl[-1][0] + STEP_5M_MS
        if nxt <= cursor:
            break
        cursor = nxt
    return out


PROXY_CHECK_URL = "https://fapi.binance.com/fapi/v1/time"


def probe_listing(sym):
    """探测上市首根 K 时间(2026-08-21 超定): startTime=2020-01-01 limit=1, 重试3次。
    币安对 startTime 早于上市 → 返回上市首根。失败返回 None。"""
    for _ in range(3):
        try:
            r = requests.get(KLINE_URL, params={"symbol": sym, "interval": "5m",
                                                "startTime": 1577836800000, "limit": 1}, timeout=30)
            r.raise_for_status()
            kl = r.json()
            return kl[0][0] if kl else None
        except Exception:
            time.sleep(3)
    return None


def check_proxy():
    """轻量代理健康检查: fapi time 接口 200 = 通"""
    try:
        r = requests.get(PROXY_CHECK_URL, timeout=8)
        return r.status_code == 200
    except Exception:
        return False


fail_streak = 0  # 连续失败计数(2026-08-21 超定): ≥5 检查代理, 挂了暂停等待


def wait_rate_or_proxy(e):
    """429 → 读 Retry-After 动态等待; 其他连续失败 → 代理健康检查, 挂了等恢复"""
    global fail_streak
    wait = 5.0
    if isinstance(e, requests.HTTPError) and e.response is not None:
        ra = e.response.headers.get("Retry-After")
        if ra:
            try:
                wait = float(ra)
            except ValueError:
                pass
    if wait > 60:
        wait = 60.0
    print(f"  ⏸ 限流/失败, 等待 {wait:.0f}s 后重试", flush=True)
    time.sleep(wait)
    fail_streak += 1
    if fail_streak >= 5:
        print("  ⚠️ 连续失败5次, 检查代理...", flush=True)
        while not check_proxy():
            print("  代理不可用, 60s后重试...", flush=True)
            time.sleep(60)
        print("  ✅ 代理恢复, 继续任务", flush=True)
        fail_streak = 0


cli = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
db = cli[DB_NAME]

# 全局时间网格(所有币相同)
needed_ots = set()
for p in points:
    needed_ots.add(p - timedelta(minutes=5))
    needed_ots.add(p - timedelta(hours=24, minutes=5))
needed_ms = {int(x.timestamp() * 1000) for x in needed_ots}
needed_dt = [ms_to_local(m) for m in needed_ms]
START_MS = int(min(needed_ots).timestamp() * 1000)
END_MS = int(max(needed_ots).timestamp() * 1000)
sorted_ots = sorted(needed_ots)


def upsert_needed(kc, kl):
    """全量入库(拉到的 5m K 全存, 不只存 needed_ms 计算点)。
    原实现只存每10分钟1根(needed_ms), 导致库里"筛子状"缺一半,
    回测每次都要为半入库补拉(整窗重拉/兜底)。2026-08-17 用户发现并修复。"""
    docs = []
    seen = set()
    for k in kl:
        ot_ms = k[0]
        if ot_ms in seen:
            continue
        seen.add(ot_ms)
        docs.append({
            "openTime": int(ot_ms),
            "openTimeLocal": local_str(ms_to_local(int(ot_ms))),
            "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
            "volume": float(k[5]), "closeTime": int(k[6]),
            "closeTimeLocal": local_str(ms_to_local(int(k[6]))),
            "quoteVolume": float(k[7]), "trades": int(k[8]),
            "takerBuyBaseVolume": float(k[9]), "takerBuyQuoteVolume": float(k[10]),
        })
    if docs:
        kc.bulk_write([UpdateOne({"openTime": d["openTime"]}, {"$set": d}, upsert=True)
                       for d in docs], ordered=False)
    return len(docs)


def process_symbol(sym):
    coll_name = f"{sym}.{EXCHANGE}"
    kc = db[coll_name]
    kc.create_index("openTime", unique=True)
    out_csv = os.path.join(OUT_DIR, f"{sym.lower()}.csv")

    # 0) probe 上市时间 → 实际所需区间(2026-08-21 超定): 上市前无数据, 点标 error 但不影响 ok
    #    probe 失败 → 查代理: 挂了循环等待恢复(2026-08-21 修正: 此前probe失败未接入代理自愈, 代理挂时156币被标error)
    listing_ms = None
    while listing_ms is None:
        listing_ms = probe_listing(sym)
        if listing_ms is None:
            if not check_proxy():
                print(f"  ⚠️ {sym} probe失败且代理不可用, 暂停等代理恢复...", flush=True)
                while not check_proxy():
                    time.sleep(60)
                print(f"  ✅ 代理恢复, 重试 {sym} probe", flush=True)
            else:
                print(f"  [probe] {sym} 重试(代理通但探测失败)...", flush=True)
                time.sleep(5)
    need_start_ms = max(START_MS, listing_ms)  # 上市首根时刻起(不提前: 提前的 ot 无K会假阳性缺1根, 2026-08-21 修正)
    need_ots = [ot for ot in sorted_ots if int(ot.timestamp() * 1000) >= need_start_ms]
    print(f"  [probe] {sym} 上市 {local_str(ms_to_local(listing_ms))} | 所需点 {len(need_ots)}/{len(sorted_ots)}", flush=True)

    # 1) 查缺失 -> 拉取补齐(最多重试 3 次, 只补上市后区间)
    #    内存修复(2026-08-21): distinct/find 全加 [need_start, END] 区间过滤, 避免老币全表40万+条拉全量导致内存爆炸→系统卡顿→写文件被锁
    rng_f = {"openTime": {"$gte": need_start_ms - STEP_5M_MS, "$lte": END_MS}}
    have_ms = set(kc.distinct("openTime", rng_f))
    for attempt in range(4):  # 0=查库, 1~3=拉取重试
        missing_ms = [int(ot.timestamp() * 1000) for ot in need_ots if int(ot.timestamp() * 1000) not in have_ms]
        if not missing_ms:
            break
        if attempt >= 3:
            break
        # 按缺失点连续性分组拉取(早期缺段+末端新增段都覆盖, 库内已有段跳过)
        segs = []
        for m in sorted(missing_ms):
            if segs and m <= segs[-1][1] + STEP_5M_MS * 2:
                segs[-1][1] = max(segs[-1][1], m)
            else:
                segs.append([m, m])
        try:
            for s, e in segs:
                fstart, fend = max(s - STEP_5M_MS, need_start_ms), e + STEP_5M_MS
                kl = fetch_all(sym, fstart, fend)
                print(f"  [fetch] {sym} 第{attempt + 1}次 返回 {len(kl)} 根 "
                      f"[{local_str(ms_to_local(fstart))} ~ {local_str(ms_to_local(fend))}]", flush=True)
                upsert_needed(kc, kl)
        except Exception as e:
            print(f"  [fetch] {sym} 第{attempt + 1}次失败: {e}", flush=True)
            wait_rate_or_proxy(e)
        have_ms = set(kc.distinct("openTime", rng_f))
    missing_left = [ot for ot in need_ots if int(ot.timestamp() * 1000) not in have_ms]
    if missing_left:
        # 全段兜底(2026-08-21 超定): 补首尾重试3次仍不全 → 拉目标起止范围全段
        try:
            kl = fetch_all(sym, need_start_ms, END_MS)
            print(f"  [full] {sym} 全段兜底 返回 {len(kl)} 根", flush=True)
            upsert_needed(kc, kl)
        except Exception as e:
            print(f"  [full] {sym} 全段兜底失败: {e}", flush=True)
            wait_rate_or_proxy(e)
        have_ms = set(kc.distinct("openTime", rng_f))
    missing_left = [ot for ot in need_ots if int(ot.timestamp() * 1000) not in have_ms]
    if missing_left:
        print(f"  [skip] {sym} 补首尾+全段兜底后仍缺失 {len(missing_left)}/{len(need_ots)} 根, 标 error 待下轮重试", flush=True)

    # 2) 一次取全量进内存(range 查询替代 10万项 $in, 2026-08-21 内存修复)
    kc_docs = {d["openTime"]: d for d in kc.find(rng_f)}

    def kget(ot_local):
        return kc_docs.get(int(ot_local.timestamp() * 1000))

    # 3) 计算 + 写 CSV(上市前/上市不足24h 点标 error 但不计 err, 上市后可用点完整即 ok)
    rows = []
    ok = err = pre = 0
    for p in points:
        ot1 = p - timedelta(minutes=5)
        ot2 = p - timedelta(hours=24, minutes=5)
        if int(ot1.timestamp() * 1000) < need_start_ms:
            rows.append([local_str(p), None, None, None, "error", "上市前无数据"])
            pre += 1
            continue
        if int(ot2.timestamp() * 1000) < need_start_ms:
            # 上市不足24h: T-24h 在上市前, 无法算 24h 涨跌幅(必然边界点, 不计 err)
            rows.append([local_str(p), None, None, None, "error", "上市不足24h"])
            pre += 1
            continue
        k1 = kget(ot1)
        k2 = kget(ot2)
        if k1 is None or k2 is None:
            miss = [f"{x:%Y-%m-%d %H:%M}" for x in (ot1, ot2) if kget(x) is None]
            rows.append([local_str(p), None, None, None, "error", f"数据缺失: {miss}"])
            err += 1
            continue
        change = (k1["close"] - k2["close"]) / k2["close"] * 100
        rows.append([local_str(p), k1["close"], k2["close"], round(change, 6), "ok", ""])
        ok += 1

    # 写 CSV(2026-08-21 修复): ①重试3次×3s 抗瞬时锁 ②先写 .tmp 再 os.replace 原子替换(避免覆盖被锁文件失败)
    for w_attempt in range(3):
        try:
            tmp_csv = out_csv + ".tmp"
            with open(tmp_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["pointTimeLocal", "closeT", "closeT24h", "changePct", "status", "errorDetail"])
                w.writerows(rows)
            os.replace(tmp_csv, out_csv)
            break
        except PermissionError as e:
            if w_attempt < 2:
                print(f"  [write] {sym} Permission denied 第{w_attempt + 1}次, 3s后重试", flush=True)
                time.sleep(3)
            else:
                raise e
    # 显式释放大对象(2026-08-21 内存修复): 3并发下 GC 不可靠, 峰值内存=最重币的 kc_docs+rows
    del kc_docs, rows, have_ms, rng_f
    return ok, err, pre


done = load_done()
ROUNDS = 3  # 2026-08-21 超定: 整批最多三轮, 三轮后仍有缺失暂停等用户确认
total_ok = sum(done.get(s) == "ok" for s in SYMBOLS)
for round_i in range(1, ROUNDS + 1):
    todo = [s for s in SYMBOLS if done.get(s) != "ok"]
    if not todo:
        print(f"全部币数据 ok, 无需第 {round_i} 轮", flush=True)
        break
    print(f"\n===== 第 {round_i}/{ROUNDS} 轮: 待处理 {len(todo)} 币 (已 ok {total_ok}) =====", flush=True)
    failed_round = []
    done_cnt = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(process_symbol, s): s for s in todo}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                ok, err, pre = fut.result()
                total_ok += ok
                st = "ok" if err == 0 else "error"   # 上市前无数据点(pre)不计 err, 上市后完整即 ok(2026-08-21 超定)
                done[sym] = st
                if st == "error":
                    failed_round.append(sym)
            except Exception as e:
                print(f"[{sym}] 异常: {e}", flush=True)
                done[sym] = "error"
                failed_round.append(sym)
            done_cnt += 1
            save_done(done)
            if done_cnt % 10 == 0:
                print(f"[{done_cnt}/{len(todo)}] 完成, 本轮累计 ok={total_ok} 失败={len(failed_round)}", flush=True)
    print(f"===== 第 {round_i} 轮完成: 本轮失败 {len(failed_round)} =====", flush=True)

final_failed = [s for s in SYMBOLS if done.get(s) != "ok"]
if final_failed:
    print(f"\n⚠️⚠️ 三轮后仍有 {len(final_failed)} 币数据缺失, 暂停等待用户确认:", flush=True)
    for s in sorted(final_failed):
        print(f"  - {s}", flush=True)
    print("(不自动重试。请确认: 继续补拉 / 人工处理 / 接受缺失跳过)", flush=True)
else:
    print(f"\n✅ 全部 {len(SYMBOLS)} 币数据 ok", flush=True)
print(f"输出目录: {OUT_DIR}", flush=True)
