"""
reversal_indicators.py
========================
TradingView 短线反转指标的 Python 复刻（纯标准库，无 numpy/pandas 依赖）。

公式对齐 TradingView 官方内置函数 ta.* 的公开定义：
  RSI        : Wilder 平滑 (ta.rsi)
  Stochastic : %K = SMA(100*(C-LL)/(HH-LL), smooth); %D = SMA(%K, d) (ta.stoch)
  Stochastic RSI : 对 RSI 再做 Stochastic (ta.stoch 套 ta.rsi)
  Williams %R : -100*(HH-C)/(HH-LL) (ta.wpr)
  CCI        : (TP - SMA(TP)) / (0.015 * MeanDev) (ta.cci)
  Bollinger  : SMA ± mult * pop_std (ta.bb)
  MACD       : EMA12 - EMA26, signal = SMA9 (ta.macd)
  ATR        : Wilder RMA of TrueRange (ta.atr)
  Parabolic SAR : Wilder SAR (ta.sar)
  CMO        : Chande Momentum Oscillator (ta.cmo)
  Fisher     : Inverse-Fisher of normalized HL2 (ta.fisher)
  VWAP       : 成交量加权均价（支持滚动窗口）(ta.vwap 的滚动版)
  Keltner    : EMA(close) ± mult * ATR (ta.kc 等价)
  Z-Score    : 滚动标准化，均值回归核心 (ta not built-in, 自实现)
  Connors RSI: (RSI3 + RSI(streak,2) + RSI(rank,100)) / 3 (Connors 专利)
  SuperTrend : (ta.supertrend) 翻转即趋势反转信号
  Pivot      : 经典枢轴点 S/R（前一根 HLC 推算）
  Candlestick: 锤子 / 射击星 / 看涨吞没 / 看跌吞没 / 十字星

约定：
  - 所有函数输入为等长 list（float）。OHLC 分别为 open/high/low/close，volume 可选。
  - 返回 list 与输入等长，预热期填充 None（与 TV 行为一致，开头几根无值）。
  - "反转"含义：超买(overbought)后做空/超卖(oversold)后做多。短线通常指 1~15 分钟 / 1~5 根 K。

⚠️ 免责：本文件为技术指标的客观复刻，不构成任何交易建议。
"""

import math


# ----------------------------------------------------------------------------
# 基础滚动工具（None 安全的 SMA / EMA / RMA / 滚动极值 / 总体标准差）
# ----------------------------------------------------------------------------
def _sma(x, n):
    """简单移动平均，None 安全：窗口内出现 None 时不计入；满 n 个才出值。"""
    out = [None] * len(x)
    s = 0.0
    cnt = 0
    for i, v in enumerate(x):
        if v is not None:
            s += v
            cnt += 1
        if i >= n:
            old = x[i - n]
            if old is not None:
                s -= old
                cnt -= 1
        if cnt == n:
            out[i] = s / n
    return out


def _ema(x, n):
    """指数移动平均，以首 n 个的 SMA 作种子（输入要求无 None）。"""
    out = [None] * len(x)
    if len(x) < n:
        return out
    alpha = 2.0 / (n + 1)
    seed = sum(x[:n]) / n
    out[n - 1] = seed
    prev = seed
    for i in range(n, len(x)):
        prev = x[i] * alpha + prev * (1 - alpha)
        out[i] = prev
    return out


def _rma(x, n):
    """Wilder 平滑（RMA）：seed = 首 n 个 SMA，之后 prev=(prev*(n-1)+x)/n。"""
    out = [None] * len(x)
    if len(x) < n:
        return out
    seed = sum(x[:n]) / n
    out[n - 1] = seed
    prev = seed
    for i in range(n, len(x)):
        prev = (prev * (n - 1) + x[i]) / n
        out[i] = prev
    return out


def _rolling_min(x, n):
    out = [None] * len(x)
    for i in range(len(x)):
        if i >= n - 1:
            out[i] = min(x[i - n + 1:i + 1])
    return out


def _rolling_max(x, n):
    out = [None] * len(x)
    for i in range(len(x)):
        if i >= n - 1:
            out[i] = max(x[i - n + 1:i + 1])
    return out


def _pop_std(w):
    """总体标准差（TradingView ta.stdev 用总体标准差）。"""
    m = sum(w) / len(w)
    return math.sqrt(sum((v - m) ** 2 for v in w) / len(w))


# ----------------------------------------------------------------------------
# 振荡器类反转指标
# ----------------------------------------------------------------------------
def rsi(close, length=14):
    """Wilder RSI。超卖<30 / 超买>70 是常见的短线反转阈值。"""
    n = len(close)
    out = [None] * n
    if n < length + 1:
        return out
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        d = close[i] - close[i - 1]
        gains[i] = max(d, 0.0)
        losses[i] = max(-d, 0.0)
    ag = sum(gains[1:length + 1]) / length
    al = sum(losses[1:length + 1]) / length
    out[length] = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    for i in range(length + 1, n):
        ag = (ag * (length - 1) + gains[i]) / length
        al = (al * (length - 1) + losses[i]) / length
        out[i] = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    return out


def stochastic(high, low, close, k_len=14, d_len=3, smooth=3):
    """TV ta.stoch：%K = SMA(100*(C-LL)/(HH-LL), smooth)；%D = SMA(%K, d_len)。
    短线反转：%K 从 <20 上穿 %D = 超卖反弹；>80 下穿 = 超买回落。"""
    n = len(close)
    hh = _rolling_max(high, k_len)
    ll = _rolling_min(low, k_len)
    rawk = [None] * n
    for i in range(n):
        if hh[i] is None or ll[i] is None or hh[i] == ll[i]:
            continue
        rawk[i] = 100.0 * (close[i] - ll[i]) / (hh[i] - ll[i])
    k = _sma(rawk, smooth)
    d = _sma(k, d_len)
    return k, d


def stochastic_rsi(close, rsi_len=14, stoch_len=14, k_smooth=3, d_smooth=3):
    """Stochastic RSI：对 RSI 序列再做 Stochastic。比 RSI 更灵敏，适合超短反转。
    阈值同 Stochastic（<20 / >80）。"""
    r = rsi(close, rsi_len)
    n = len(close)
    numer, idx = [], []
    for i, v in enumerate(r):
        if v is not None:
            numer.append(v)
            idx.append(i)
    m = len(numer)
    rawk = [0.0] * m
    for i in range(m):
        lo = min(numer[max(0, i - stoch_len + 1):i + 1])
        hi = max(numer[max(0, i - stoch_len + 1):i + 1])
        rawk[i] = 0.0 if hi == lo else 100.0 * (numer[i] - lo) / (hi - lo)
    smk = _sma(rawk, k_smooth)
    smd = _sma(smk, d_smooth)
    k = [None] * n
    d = [None] * n
    for j, i in enumerate(idx):
        k[i] = smk[j]
        d[i] = smd[j]
    return k, d


def williams_r(high, low, close, length=14):
    """Williams %R。范围 [-100, 0]；<-80 超卖，>-20 超买。"""
    n = len(close)
    hh = _rolling_max(high, length)
    ll = _rolling_min(low, length)
    out = [None] * n
    for i in range(n):
        if hh[i] is None or ll[i] is None or hh[i] == ll[i]:
            continue
        out[i] = -100.0 * (hh[i] - close[i]) / (hh[i] - ll[i])
    return out


def cci(high, low, close, length=20):
    """Commodity Channel Index。<-100 超卖，>100 超买（典型反转区）。"""
    n = len(close)
    tp = [(high[i] + low[i] + close[i]) / 3.0 for i in range(n)]
    basis = _sma(tp, length)
    out = [None] * n
    for i in range(n):
        if basis[i] is None:
            continue
        window = tp[i - length + 1:i + 1]
        md = sum(abs(v - basis[i]) for v in window) / length
        out[i] = 0.0 if md == 0 else (tp[i] - basis[i]) / (0.015 * md)
    return out


def cmo(close, length=14):
    """Chande Momentum Oscillator。范围 [-100, 100]；极端 ±50/±100 视为反转区。"""
    n = len(close)
    out = [None] * n
    for i in range(length, n):
        up = 0.0
        dn = 0.0
        for j in range(i - length + 1, i + 1):
            d = close[j] - close[j - 1]
            if d > 0:
                up += d
            elif d < 0:
                dn += -d
        tot = up + dn
        out[i] = 0.0 if tot == 0 else 100.0 * (up - dn) / tot
    return out


# ----------------------------------------------------------------------------
# 通道 / 带类（均值回归）
# ----------------------------------------------------------------------------
def bollinger_bands(close, length=20, mult=2.0):
    """TV ta.bb：basis=SMA，band=basis±mult*pop_std。
    返回 (basis, upper, lower, pctB)。pctB<0 跌破下轨=超卖反转；>1 突破上轨=超买。"""
    n = len(close)
    basis = _sma(close, length)
    upper = [None] * n
    lower = [None] * n
    pctb = [None] * n
    for i in range(n):
        if basis[i] is None:
            continue
        w = close[i - length + 1:i + 1]
        sd = _pop_std(w)
        upper[i] = basis[i] + mult * sd
        lower[i] = basis[i] - mult * sd
        if sd != 0:
            pctb[i] = (close[i] - lower[i]) / (upper[i] - lower[i])
    return basis, upper, lower, pctb


def keltner_channel(high, low, close, length=20, mult=2.0):
    """Keltner：basis=EMA(close)，band=basis±mult*ATR。价格触下带=超卖反转区。"""
    n = len(close)
    basis = _ema(close, length)
    atr_v = atr(high, low, close, length)
    upper = [None] * n
    lower = [None] * n
    for i in range(n):
        if basis[i] is None or atr_v[i] is None:
            continue
        upper[i] = basis[i] + mult * atr_v[i]
        lower[i] = basis[i] - mult * atr_v[i]
    return basis, upper, lower


def zscore(close, length=20):
    """滚动 Z-Score（自实现，TV 无内置）。|Z|>2~3 视为偏离均值，回归预期强。
    这是均值回归策略最朴素的统计表达。"""
    n = len(close)
    out = [None] * n
    for i in range(n):
        if i >= length - 1:
            w = close[i - length + 1:i + 1]
            m = sum(w) / length
            sd = _pop_std(w)
            out[i] = 0.0 if sd == 0 else (close[i] - m) / sd
    return out


def vwap(high, low, close, volume, length=None):
    """VWAP。length=None 为累计 VWAP（日内）；给定 length 为滚动窗口 VWAP。
    返回 (vwap, deviation)：deviation = (close - vwap)/vwap，<-0.005~-.01 为短线超卖偏离。"""
    n = len(close)
    tp = [(high[i] + low[i] + close[i]) / 3.0 for i in range(n)]
    pv = [tp[i] * volume[i] for i in range(n)]
    out = [None] * n
    if length is None:
        cum_pv = 0.0
        cum_v = 0.0
        for i in range(n):
            cum_pv += pv[i]
            cum_v += volume[i]
            out[i] = cum_pv / cum_v if cum_v != 0 else None
    else:
        for i in range(n):
            if i >= length - 1:
                wv = sum(volume[i - length + 1:i + 1])
                wt = sum(pv[i - length + 1:i + 1])
                out[i] = wt / wv if wv != 0 else None
    dev = [None] * n
    for i in range(n):
        if out[i] not in (None, 0):
            dev[i] = (close[i] - out[i]) / out[i]
    return out, dev


# ----------------------------------------------------------------------------
# 趋势 / 动量类（其"翻转"本身就是反转信号）
# ----------------------------------------------------------------------------
def macd(close, fast=12, slow=26, signal=9):
    """TV ta.macd：macd=EMA12-EMA26，signal=SMA9，hist=macd-signal。
    短线反转常用：hist 由负转正（底背离后）=反弹；由正转负=回落。"""
    n = len(close)
    ef = _ema(close, fast)
    es = _ema(close, slow)
    macd_line = [None if (ef[i] is None or es[i] is None) else ef[i] - es[i]
                 for i in range(n)]
    sig = _sma(macd_line, signal)
    hist = [None if (macd_line[i] is None or sig[i] is None) else macd_line[i] - sig[i]
            for i in range(n)]
    return macd_line, sig, hist


def atr(high, low, close, length=14):
    """Wilder ATR（真实波幅的 RMA）。本身不是反转信号，但作为波动尺度喂给
    SAR / Keltner / SuperTrend / 通道突破止损。"""
    n = len(close)
    tr = [0.0] * n
    for i in range(1, n):
        tr[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
    out = [None] * n
    if n < length + 1:
        return out
    seed = sum(tr[1:length + 1]) / length
    out[length] = seed
    prev = seed
    for i in range(length + 1, n):
        prev = (prev * (length - 1) + tr[i]) / length
        out[i] = prev
    return out


def parabolic_sar(high, low, step=0.02, max_step=0.2):
    """Wilder Parabolic SAR。返回 (sar, trend)；trend 由 'up' 翻 'down'（或反之）
    即趋势反转信号——SAR 翻转是经典的趋势终结/反转提示。"""
    n = len(high)
    sar = [None] * n
    trend = [None] * n
    ep = [None] * n
    af = [None] * n
    if n < 2:
        return sar, trend
    trend[1] = 'up' if high[1] >= high[0] else 'down'
    if trend[1] == 'up':
        sar[1] = low[0]
        ep[1] = high[1]
    else:
        sar[1] = high[0]
        ep[1] = low[1]
    af[1] = step
    for i in range(2, n):
        pt, ps, pe, pa = trend[i - 1], sar[i - 1], ep[i - 1], af[i - 1]
        if pt == 'up':
            if low[i] < ps:
                trend[i] = 'down'
                sar[i] = pe
                ep[i] = low[i]
                af[i] = step
            else:
                new_ep = max(pe, high[i])
                new_af = pa + step if new_ep > pe else pa
                new_af = min(new_af, max_step)
                sar[i] = ps + pa * (pe - ps)
                sar[i] = min(sar[i], low[i - 1], low[i - 2] if i >= 2 else low[i - 1])
                ep[i] = new_ep
                af[i] = new_af
                trend[i] = 'up'
        else:
            if high[i] > ps:
                trend[i] = 'up'
                sar[i] = pe
                ep[i] = high[i]
                af[i] = step
            else:
                new_ep = min(pe, low[i])
                new_af = pa + step if new_ep < pe else pa
                new_af = min(new_af, max_step)
                sar[i] = ps + pa * (pe - ps)
                sar[i] = max(sar[i], high[i - 1], high[i - 2] if i >= 2 else high[i - 1])
                ep[i] = new_ep
                af[i] = new_af
                trend[i] = 'down'
    return sar, trend


def supertrend(high, low, close, length=10, mult=3.0):
    """TV ta.supertrend。返回 (st, trend)，trend: 1=多, -1=空。
    trend 由 1 翻 -1（或反之）= 趋势反转信号。常配合收盘价穿越 ST 使用。"""
    atr_v = atr(high, low, close, length)
    n = len(close)
    hl2 = [(high[i] + low[i]) / 2.0 for i in range(n)]
    upb = [None] * n
    lwb = [None] * n
    st = [None] * n
    trend = [None] * n
    for i in range(n):
        if atr_v[i] is None:
            continue
        up = hl2[i] - mult * atr_v[i]
        dn = hl2[i] + mult * atr_v[i]
        if i > 0 and upb[i - 1] is not None:
            upb[i] = up if up > upb[i - 1] else upb[i - 1]
            lwb[i] = dn if dn < lwb[i - 1] else lwb[i - 1]
        else:
            upb[i] = up
            lwb[i] = dn
        if st[i - 1] is None:
            if close[i] <= lwb[i]:
                trend[i] = -1
                st[i] = lwb[i]
            else:
                trend[i] = 1
                st[i] = upb[i]
        elif trend[i - 1] == 1:
            if close[i] < st[i - 1]:
                trend[i] = -1
                st[i] = lwb[i]
            else:
                trend[i] = 1
                st[i] = upb[i] if upb[i] > st[i - 1] else st[i - 1]
        else:
            if close[i] > st[i - 1]:
                trend[i] = 1
                st[i] = upb[i]
            else:
                trend[i] = -1
                st[i] = lwb[i] if lwb[i] < st[i - 1] else st[i - 1]
    return st, trend


# ----------------------------------------------------------------------------
# 复合 / 统计类
# ----------------------------------------------------------------------------
def connors_rsi(close, rsi_len=3, streak_len=2, rank_len=50):
    """Connors RSI = (RSI(close,3) + RSI(streak,2) + RSI(rank,100)) / 3。
    短线均值回归神器：核心是第 2~3 期短 RSI。CRS<10 强烈超卖（做多反转），
    >90 强烈超买（做空反转）。"""
    n = len(close)
    streak = [0] * n
    for i in range(1, n):
        if close[i] > close[i - 1]:
            streak[i] = streak[i - 1] + 1 if streak[i - 1] > 0 else 1
        elif close[i] < close[i - 1]:
            streak[i] = streak[i - 1] - 1 if streak[i - 1] < 0 else -1
        else:
            streak[i] = 0
    ret = [0.0] * n
    for i in range(1, n):
        ret[i] = (close[i] - close[i - 1]) / close[i - 1] if close[i - 1] != 0 else 0.0
    rank = [None] * n
    for i in range(n):
        if i >= rank_len:
            w = ret[i - rank_len + 1:i + 1]
            cnt = sum(1 for x in w if x < ret[i])
            rank[i] = 100.0 * cnt / rank_len
    r1 = rsi(close, rsi_len)
    r2 = rsi(streak, streak_len)
    # rank 前面有 None，取首个非 None 之后的连续段喂给 rsi，再映射回原索引
    first = next((i for i, v in enumerate(rank) if v is not None), None)
    r3 = [None] * n
    if first is not None:
        tail = rank[first:]
        r3_tail = rsi(tail, rank_len)
        for j, val in enumerate(r3_tail):
            r3[first + j] = val
    out = [None] * n
    for i in range(n):
        vals = [v for v in (r1[i], r2[i], r3[i]) if v is not None]
        if len(vals) == 3:
            out[i] = sum(vals) / 3.0
    return out


def fisher_transform(high, low, length=10):
    """Inverse Fisher Transform of normalized HL2。返回 (fish, signal)。
    极值 fish>2（超买）/ <-2（超卖）反转；signal 为其前一根，穿越即信号。"""
    n = len(high)
    x = [None] * n
    for i in range(n):
        if i >= length - 1:
            hh = max(high[i - length + 1:i + 1])
            ll = min(low[i - length + 1:i + 1])
            x[i] = 0.0 if hh == ll else 2.0 * ((high[i] + low[i]) / 2.0 - ll) / (hh - ll) - 1.0
    fish = [None] * n
    prev = None
    for i in range(n):
        if x[i] is None:
            continue
        v = max(-0.999, min(0.999, x[i]))
        cur = 0.5 * math.log((1 + v) / (1 - v)) + (prev if prev is not None else 0.0)
        fish[i] = cur
        prev = cur
    sig = [None] + fish[:-1]
    return fish, sig


def pivot_points(high, low, close):
    """经典枢轴点（用前一根 HLC 推算）。返回 (pp, r1, r2, r3, s1, s2, s3)。
    价格跌到 s1/s2/s3 附近出现支撑反弹=短线反转；涨到 r1/r2/r3 遇阻回落。"""
    n = len(close)
    pp = [None] * n
    r1 = r2 = r3 = s1 = s2 = s3 = [None] * n
    for i in range(1, n):
        p = (high[i - 1] + low[i - 1] + close[i - 1]) / 3.0
        pp[i] = p
        s1[i] = 2 * p - high[i - 1]
        r1[i] = 2 * p - low[i - 1]
        s2[i] = p - (high[i - 1] - low[i - 1])
        r2[i] = p + (high[i - 1] - low[i - 1])
        s3[i] = low[i - 1] - 2 * (high[i - 1] - p)
        r3[i] = high[i - 1] + 2 * (p - low[i - 1])
    return pp, r1, r2, r3, s1, s2, s3


def candlestick_patterns(open_, high, low, close):
    """基础反转蜡烛形态。返回 dict，每项为与输入等长的 list（True/False/None）。
    锤子(hammer)=潜在底部反转；射击星(shooting_star)=潜在顶部反转；
    看涨吞没(bull_engulfing)/看跌吞没(bear_engulfing)；十字星(doji)=犹豫/反转前兆。"""
    n = len(close)
    hammer = [None] * n
    shooting = [None] * n
    bull_eng = [None] * n
    bear_eng = [None] * n
    doji = [None] * n

    def body(i):
        return abs(close[i] - open_[i])

    def rng(i):
        return high[i] - low[i] if high[i] > low[i] else 1e-9

    for i in range(1, n):
        b = body(i)
        r = rng(i)
        upper = high[i] - max(open_[i], close[i])
        lower = min(open_[i], close[i]) - low[i]
        hammer[i] = (b < 0.3 * r) and (lower > 2 * b) and (upper < 0.1 * r)
        shooting[i] = (b < 0.3 * r) and (upper > 2 * b) and (lower < 0.1 * r)
        if (close[i] > open_[i] and close[i - 1] < open_[i - 1]
                and close[i] >= open_[i - 1] and open_[i] <= close[i - 1]
                and body(i) > body(i - 1)):
            bull_eng[i] = True
        if (close[i] < open_[i] and close[i - 1] > open_[i - 1]
                and open_[i] >= close[i - 1] and close[i] <= open_[i - 1]
                and body(i) > body(i - 1)):
            bear_eng[i] = True
        doji[i] = b < 0.1 * r
    return {
        "hammer": hammer,
        "shooting_star": shooting,
        "bull_engulfing": bull_eng,
        "bear_engulfing": bear_eng,
        "doji": doji,
    }


# ----------------------------------------------------------------------------
# 汇总：把多个指标的反转信号计数（bullish / bearish），用于快速筛反转时点
# ----------------------------------------------------------------------------
def reversal_score(open_, high, low, close, volume=None,
                   rsi_len=14, stoch_k=14, stoch_d=3, stoch_sm=3,
                   will_len=14, cci_len=14, bb_len=20, bb_mult=2.0,
                   connors_len=3, connors_rank_len=50,
                   sar_step=0.02, sar_max=0.2,
                   st_len=10, st_mult=3.0, z_len=20,
                   include_candle=True):
    """逐根 K 线统计"有多少指标发出反转信号"，给出 bullish/bearish 计数。
    多指标共振（score 高）比单指标更可靠——这正是防假信号的核心思路。

    默认阈值（短线反转常用）：
      RSI <30 -> bullish, >70 -> bearish
      Stochastic %K <20 -> bullish, >80 -> bearish
      Williams %R < -80 -> bullish, > -20 -> bearish
      CCI < -100 -> bullish, > 100 -> bearish
      Bollinger %B <0 -> bullish, >1 -> bearish
      Connors RSI <10 -> bullish, >90 -> bearish
      Z-Score < -2 -> bullish, > 2 -> bearish
      SAR / SuperTrend 翻转 -> 对应方向
    返回 list of dict: {'bull','bear','signals', 以及子类计数
        'bull_m'/'bear_m'(均值回归振荡器), 'bull_t'/'bear_t'(趋势反转 SAR/ST),
        'bull_c'/'bear_c'(形态)}。bull=bull_m+bull_t+bull_c, bear 同理。
    """
    n = len(close)
    r = rsi(close, rsi_len)
    sk, sd = stochastic(high, low, close, stoch_k, stoch_d, stoch_sm)
    wr = williams_r(high, low, close, will_len)
    cc = cci(high, low, close, cci_len)
    _, _, _, pctb = bollinger_bands(close, bb_len, bb_mult)
    cr = connors_rsi(close, connors_len, rank_len=connors_rank_len)
    z = zscore(close, z_len)
    sar, sar_trend = parabolic_sar(high, low, sar_step, sar_max)
    _, st_trend = supertrend(high, low, close, st_len, st_mult)
    candle = candlestick_patterns(open_, high, low, close) if include_candle else None

    out = []
    for i in range(n):
        bull = bear = 0
        # 子类计数: 均值回归(m)/趋势反转(t)/形态(c)
        bm = br = bt = btr = bc = bcr = 0
        sig = []
        if r[i] is not None:
            if r[i] < 30:
                bull += 1; bm += 1; sig.append("RSI超卖")
            elif r[i] > 70:
                bear += 1; br += 1; sig.append("RSI超买")
        if sk[i] is not None:
            if sk[i] < 20:
                bull += 1; bm += 1; sig.append("Stoch超卖")
            elif sk[i] > 80:
                bear += 1; br += 1; sig.append("Stoch超买")
        if wr[i] is not None:
            if wr[i] < -80:
                bull += 1; bm += 1; sig.append("W%R超卖")
            elif wr[i] > -20:
                bear += 1; br += 1; sig.append("W%R超买")
        if cc[i] is not None:
            if cc[i] < -100:
                bull += 1; bm += 1; sig.append("CCI超卖")
            elif cc[i] > 100:
                bear += 1; br += 1; sig.append("CCI超买")
        if pctb[i] is not None:
            if pctb[i] < 0:
                bull += 1; bm += 1; sig.append("布林破下轨")
            elif pctb[i] > 1:
                bear += 1; br += 1; sig.append("布林破上轨")
        if cr[i] is not None:
            if cr[i] < 10:
                bull += 1; bm += 1; sig.append("Connors超卖")
            elif cr[i] > 90:
                bear += 1; br += 1; sig.append("Connors超买")
        if z[i] is not None:
            if z[i] < -2:
                bull += 1; bm += 1; sig.append("Zscore偏离下")
            elif z[i] > 2:
                bear += 1; br += 1; sig.append("Zscore偏离上")
        if i > 0 and sar_trend[i] is not None and sar_trend[i - 1] is not None:
            if sar_trend[i - 1] == 'up' and sar_trend[i] == 'down':
                bear += 1; btr += 1; sig.append("SAR翻空")
            elif sar_trend[i - 1] == 'down' and sar_trend[i] == 'up':
                bull += 1; bt += 1; sig.append("SAR翻多")
        if i > 0 and st_trend[i] is not None and st_trend[i - 1] is not None:
            if st_trend[i - 1] == 1 and st_trend[i] == -1:
                bear += 1; btr += 1; sig.append("SuperTrend翻空")
            elif st_trend[i - 1] == -1 and st_trend[i] == 1:
                bull += 1; bt += 1; sig.append("SuperTrend翻多")
        if candle is not None:
            if candle["hammer"][i]:
                bull += 1; bc += 1; sig.append("锤子线")
            if candle["bull_engulfing"][i]:
                bull += 1; bc += 1; sig.append("看涨吞没")
            if candle["shooting_star"][i]:
                bear += 1; bcr += 1; sig.append("射击星")
            if candle["bear_engulfing"][i]:
                bear += 1; bcr += 1; sig.append("看跌吞没")
        # bull = bm+bt+bc ; bear = br+btr+bcr (保持总和兼容)
        out.append({"bull": bull, "bear": bear, "signals": sig,
                    "bull_m": bm, "bear_m": br, "bull_t": bt, "bear_t": btr,
                    "bull_c": bc, "bear_c": bcr})
    return out


if __name__ == "__main__":
    print("reversal_indicators loaded. 19 indicators + reversal_score() available.")
    print("Functions:", [n for n in dir() if not n.startswith('_') and callable(eval(n))])
