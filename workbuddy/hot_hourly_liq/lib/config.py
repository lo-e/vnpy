"""hot_hourly_liq —— 全局配置。

策略主题：追踪「市场热度高 + 小时K暴涨暴跌 + 流动性优秀」的币安 USDT 永续标的。
所有路径相对本文件所在目录（workbuddy/hot_hourly_liq/）。
"""

import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../hot_hourly_liq
DATA = os.path.join(ROOT, "data")
DIR_1H = os.path.join(DATA, "1h")
DIR_SCREENER = os.path.join(DATA, "screener")
DIR_BT = os.path.join(DATA, "backtest")

# MongoDB
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_5M = "Workbuddy_5Min_Db"
EXCHANGE = "BINANCE"

# 研究窗口（UTC 毫秒）。数据实际覆盖到 2026-08-15/16。
WINDOW_START = "2025-01-01"
WINDOW_END = "2026-08-16"

# 流动性宇宙规模：按窗口内「日成交额中位数」取前 N 名 = 最流动（也是成交额口径下最热）的标的。
# 只交易/追踪流动性足够好的名字，避免滑点吞噬 alpha。
TOP_N_UNIVERSE = 120
# 次级流动性地板（日成交额中位数，USD）。命中 TOP_N 或越过地板都纳入。
LIQ_FLOOR_USD = 30_000_000

# screener 每日输出 Top-N 榜单
SCREENER_TOP_N = 30

# 回测参数网格（数据决定方向，不预先假设）
HOLD_HOURS_LIST = [4, 8, 24]
STOP_PCT = 0.08          # 对称止损 8%
TAKE_PCT = None          # 仅用固定持有周期出场（研究用，保持简单）
SLIPPAGE_PCT = 0.0005    # 单边滑点（流动性好，给很紧）
FEE_PCT = 0.0004         # 单边手续费（taker）


def to_ms(s):
    from datetime import datetime, timezone
    if isinstance(s, (int, float)):
        return int(s)
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


START_MS = to_ms(WINDOW_START)
END_MS = to_ms(WINDOW_END)


def ensure_dirs():
    for d in (DATA, DIR_1H, DIR_SCREENER, DIR_BT):
        os.makedirs(d, exist_ok=True)
