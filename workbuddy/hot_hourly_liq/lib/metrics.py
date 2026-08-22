"""指标计算：从 1h 聚合数组派生「热度 / 波动率 / 流动性」三类因子。

约定：所有返回数组与输入等长；首根因需要滞后量而置 NaN。
"""

import numpy as np


def compute_features(arr, vol_window=24):
    """对单 symbol 的 1h 数组(dict) 计算因子。

    返回 dict，含：
      ret1h       : 1h 收益率 (close[t]/close[t-1]-1)
      range1h     : 1h 实体+影线振幅 (high-low)/close[t-1]
      body1h      : |close-open|/open
      taker_ratio : taker买quote / quoteVolume  (买压，>0.5 偏多/FOMO)
      vol24h_q    : 滚动24根(=24h) quote成交额
      amihud      : |ret1h| / vol24h_q  (非流动性，越低越流动)
      liq_score   : 1/amihud 缩放后的流动性代理（仅排序用）
    """
    close = arr["c"]
    n = len(close)
    feat = {}
    ret = np.full(n, np.nan)
    ret[1:] = close[1:] / close[:-1] - 1.0
    feat["ret1h"] = ret

    rng = np.full(n, np.nan)
    rng[1:] = (arr["h"][1:] - arr["l"][1:]) / close[:-1]
    feat["range1h"] = rng

    body = np.full(n, np.nan)
    body[1:] = np.abs(arr["c"][1:] - arr["o"][1:]) / arr["o"][1:]
    feat["body1h"] = body

    tbq = arr["tbq"]
    qv = arr["qv"]
    taker_ratio = np.where(qv > 0, tbq / qv, np.nan)
    feat["taker_ratio"] = taker_ratio

    # 滚动24h成交额
    qv_pos = np.where(np.isfinite(qv), qv, 0.0)
    vol24h = np.convolve(qv_pos, np.ones(vol_window), mode="full")[:n]
    # 前 vol_window-1 根不足24h，标记为 NaN
    vol24h[:vol_window - 1] = np.nan
    feat["vol24h_q"] = vol24h

    # Amihud 非流动性: |ret| / dollar_volume(用24h滚动成交额)
    with np.errstate(divide="ignore", invalid="ignore"):
        amihud = np.abs(ret) / np.where(vol24h > 0, vol24h, np.nan)
    feat["amihud"] = amihud

    return feat


def percentile_rank(x):
    """截面百分位排名(0~1)，NaN 不参与排序、输出 NaN。"""
    out = np.full_like(x, np.nan, dtype="float64")
    mask = np.isfinite(x)
    if mask.sum() == 0:
        return out
    order = x[mask].argsort().argsort()  # 0..k-1
    out[mask] = order / (mask.sum() - 1)
    return out
