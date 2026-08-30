"""TradingView 社区主流「压力/支撑位」指标 —— 纯 numpy 改写。

对应 TradingView 社区常见的几类自动 S/R 指标，输入均为等长 numpy 数组
(open/high/low/close/qvol)，输出与输入对齐(索引 i 对应第 i 根 K)。

派系对照(社区命名 -> 本模块函数):
  1. Pivot Points (Standard 内置, 含 Classic/Camarilla/Woodie/Fibonacci)
     -> pivot_points_classic / _camarilla / _woodie / _fib
  2. Williams Fractal (社区大量 S/R 指标的基础: HarryCTC/BigBeluga/LuxAlgo)
     -> williams_fractal
  3. Pivot Points High Low / Swing 摆动点 (TV 内置; 社区 Auto S/R 的核心)
     -> swing_points
  4. Donchian Channel (滚动高低通道)
     -> donchian
  5. Volume Profile (POC/VAH/VAL, 实际成交密集区)
     -> volume_profile
"""
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view


# ----------------------------------------------------------------------------
# 1. Pivot Points 系列
#    按 period(默认 24 根=1 天, 对 1h 数据) 切块:
#    第 b 块显示的 pivot 用「第 b-1 块(前一周期)」的 H/L/C 计算 -> 无未来函数。
#    返回 dict: {pivot, r1..r3, s1..s3}
# ----------------------------------------------------------------------------
def _pivot_blocks(high, low, close, period):
    """切成块，返回每块的 (start_idx, H, L, C_prev)"""
    n = len(close)
    blocks = n // period
    out = []
    for b in range(1, blocks):  # b=0 无前块，跳过
        lo = (b - 1) * period
        hi = b * period
        H = high[lo:hi].max()
        L = low[lo:hi].min()
        C = close[hi - 1]            # 前一周期收盘
        start = b * period           # 该 pivot 投射到的当前块起点
        end = min((b + 1) * period, n)
        out.append((start, end, H, L, C))
    return out


def pivot_points_classic(high, low, close, period=24):
    n = len(close)
    res = {k: np.full(n, np.nan) for k in ["pivot", "r1", "r2", "r3", "s1", "s2", "s3"]}
    for start, end, H, L, C in _pivot_blocks(high, low, close, period):
        P = (H + L + C) / 3
        rng = H - L
        res["pivot"][start:end] = P
        res["r1"][start:end] = P + rng
        res["r2"][start:end] = P + 2 * rng
        res["r3"][start:end] = P + 3 * rng
        res["s1"][start:end] = P - rng
        res["s2"][start:end] = P - 2 * rng
        res["s3"][start:end] = P - 3 * rng
    return res


def pivot_points_camarilla(high, low, close, period=24):
    n = len(close)
    res = {k: np.full(n, np.nan) for k in ["pivot", "r1", "r2", "r3", "r4", "s1", "s2", "s3", "s4"]}
    for start, end, H, L, C in _pivot_blocks(high, low, close, period):
        rng = H - L
        res["pivot"][start:end] = (H + L + C) / 3
        res["r1"][start:end] = C + rng * 1.1 / 12
        res["r2"][start:end] = C + rng * 1.1 / 6
        res["r3"][start:end] = C + rng * 1.1 / 4
        res["r4"][start:end] = C + rng * 1.1 / 2
        res["s1"][start:end] = C - rng * 1.1 / 12
        res["s2"][start:end] = C - rng * 1.1 / 6
        res["s3"][start:end] = C - rng * 1.1 / 4
        res["s4"][start:end] = C - rng * 1.1 / 2
    return res


def pivot_points_woodie(high, low, close, period=24):
    n = len(close)
    res = {k: np.full(n, np.nan) for k in ["pivot", "r1", "r2", "r3", "s1", "s2", "s3"]}
    for start, end, H, L, _C in _pivot_blocks(high, low, close, period):
        # Woodie 用「当前周期」H/L + 2*前收; 这里前块 C 即前一周期收盘
        P = (H + L + 2 * _C) / 4
        res["pivot"][start:end] = P
        res["r1"][start:end] = 2 * P - L
        res["r2"][start:end] = P + H - L
        res["r3"][start:end] = H + 2 * (P - L)
        res["s1"][start:end] = 2 * P - H
        res["s2"][start:end] = P - H + L
        res["s3"][start:end] = L - 2 * (H - P)
    return res


def pivot_points_fib(high, low, close, period=24):
    n = len(close)
    res = {k: np.full(n, np.nan) for k in ["pivot", "r1", "r2", "r3", "s1", "s2", "s3"]}
    for start, end, H, L, C in _pivot_blocks(high, low, close, period):
        P = (H + L + C) / 3
        rng = H - L
        res["pivot"][start:end] = P
        res["r1"][start:end] = P + 0.382 * rng
        res["r2"][start:end] = P + 0.618 * rng
        res["r3"][start:end] = P + 1.000 * rng
        res["s1"][start:end] = P - 0.382 * rng
        res["s2"][start:end] = P - 0.618 * rng
        res["s3"][start:end] = P - 1.000 * rng
    return res


# ----------------------------------------------------------------------------
# 2. Williams Fractal (Bill Williams)
#    中心 bar 的高低必须「严格大于/小于」左右各 left/right 根 -> 分形点。
#    返回 (fractal_high, fractal_low) 布尔数组 + 价格数组。
# ----------------------------------------------------------------------------
def williams_fractal(high, low, left=2, right=2):
    n = len(high)
    fh = np.zeros(n, dtype=bool)
    fl = np.zeros(n, dtype=bool)
    L = left + right + 1
    if n < L:
        return fh, fl
    hw = sliding_window_view(high, L)
    lw = sliding_window_view(low, L)
    c = left  # 中心位置
    # 有效窗口 = 中心落在 [left, n-1-right], 对应窗口 index [0, n-L]
    valid = n - L + 1
    seg_h = hw[:valid]
    seg_l = lw[:valid]
    maxh = seg_h.max(axis=1)
    minl = seg_l.min(axis=1)
    # 唯一最大/最小 (严格大于/小于左右)
    fh_mid = (seg_h[:, c] == maxh) & (np.sum(seg_h == seg_h[:, c][:, None], axis=1) == 1)
    fl_mid = (seg_l[:, c] == minl) & (np.sum(seg_l == seg_l[:, c][:, None], axis=1) == 1)
    fh[left:n - right] = fh_mid
    fl[left:n - right] = fl_mid
    return fh, fl


# ----------------------------------------------------------------------------
# 3. Pivot Points High Low / Swing 摆动点
#    与 Fractal 同构, 但 left=right=N (默认 10), 用于识别「摆动高低点」,
#    社区绝大多数 Auto S/R / Supply-Demand 指标都基于它做聚类。
#    返回 dict: {sh, sl, sh_px, sl_px}  (布尔 + 价格)
# ----------------------------------------------------------------------------
def swing_points(high, low, N=10):
    n = len(high)
    sh = np.zeros(n, dtype=bool)
    sl = np.zeros(n, dtype=bool)
    L = 2 * N + 1
    if n < L:
        return {"sh": sh, "sl": sl, "sh_px": np.full(n, np.nan), "sl_px": np.full(n, np.nan)}
    hw = sliding_window_view(high, L)
    lw = sliding_window_view(low, L)
    c = N
    valid = n - L + 1
    seg_h = hw[:valid]
    seg_l = lw[:valid]
    maxh = seg_h.max(axis=1)
    minl = seg_l.min(axis=1)
    sh_mid = (seg_h[:, c] == maxh) & (np.sum(seg_h == seg_h[:, c][:, None], axis=1) == 1)
    sl_mid = (seg_l[:, c] == minl) & (np.sum(seg_l == seg_l[:, c][:, None], axis=1) == 1)
    sh[N:n - N] = sh_mid
    sl[N:n - N] = sl_mid
    sh_px = np.where(sh, high, np.nan)
    sl_px = np.where(sl, low, np.nan)
    return {"sh": sh, "sl": sl, "sh_px": sh_px, "sl_px": sl_px}


# ----------------------------------------------------------------------------
# 4. Donchian Channel (滚动 N 根 high.max / low.min)
#    返回 dict: {upper, lower, mid}
# ----------------------------------------------------------------------------
def donchian(high, low, window):
    n = len(high)
    if n < window:
        nan = np.full(n, np.nan)
        return {"upper": nan, "lower": nan, "mid": nan}
    hw = sliding_window_view(high, window)
    lw = sliding_window_view(low, window)
    upper = np.concatenate([np.full(window - 1, np.nan), hw.max(axis=1)])
    lower = np.concatenate([np.full(window - 1, np.nan), lw.min(axis=1)])
    return {"upper": upper, "lower": lower, "mid": (upper + lower) / 2}


# ----------------------------------------------------------------------------
# 5. Volume Profile (POC / VAH / VAL)
#    把整段价格分箱, 按 qvol 累加, 找成交最密集区。
#    返回 dict: {edges, vol, bin_centers, poc, vah, val, bins}
# ----------------------------------------------------------------------------
def volume_profile(close, qvol, bins=120, value_area_pct=0.70):
    lo = np.nanmin(close)
    hi = np.nanmax(close)
    edges = np.linspace(lo, hi, bins + 1)
    centers = (edges[:-1] + edges[1:]) / 2
    idx = np.digitize(close, edges) - 1
    idx = np.clip(idx, 0, bins - 1)
    vol = np.zeros(bins)
    for b in range(bins):
        vol[b] = np.nansum(qvol[idx == b])
    poc_i = int(np.argmax(vol))
    poc = centers[poc_i]
    # 从 POC 向两侧扩展, 直到累计成交 >= value_area_pct * total
    total = vol.sum()
    target = total * value_area_pct
    cum = vol[poc_i]
    lo_i, hi_i = poc_i, poc_i
    while cum < target and (lo_i > 0 or hi_i < bins - 1):
        # 比较外侧相邻箱
        left_v = vol[lo_i - 1] if lo_i > 0 else -1
        right_v = vol[hi_i + 1] if hi_i < bins - 1 else -1
        if right_v >= left_v and hi_i < bins - 1:
            hi_i += 1
            cum += vol[hi_i]
        elif lo_i > 0:
            lo_i += -1
            cum += vol[lo_i]
        else:
            break
    val = edges[lo_i]
    vah = edges[hi_i + 1]
    return {"edges": edges, "vol": vol, "bin_centers": centers,
            "poc": poc, "vah": vah, "val": val, "bins": bins}
