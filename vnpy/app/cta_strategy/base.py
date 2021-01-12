"""
Defines constants and objects used in CtaStrategy App.
"""

from dataclasses import dataclass, field
from enum import Enum
from datetime import timedelta

from vnpy.trader.constant import Direction, Offset, Exchange, Interval

APP_NAME = "CtaStrategy"
STOPORDER_PREFIX = "STOP"

""" modify by loe """
import re

class StopOrderStatus(Enum):
    WAITING = "等待中"
    CANCELLED = "已撤销"
    TRIGGERED = "已触发"


class EngineType(Enum):
    LIVE = "实盘"
    BACKTESTING = "回测"


class BacktestingMode(Enum):
    BAR = 1
    TICK = 2


@dataclass
class StopOrder:
    vt_symbol: str
    direction: Direction
    offset: Offset
    price: float
    volume: float
    stop_orderid: str
    strategy_name: str
    lock: bool = False
    vt_orderids: list = field(default_factory=list)
    status: StopOrderStatus = StopOrderStatus.WAITING


EVENT_CTA_LOG = "eCtaLog"
EVENT_CTA_STRATEGY = "eCtaStrategy"
EVENT_CTA_STOPORDER = "eCtaStopOrder"

""" modify by loe """
# 数据库名称
TICK_DB_NAME = 'VnTrader_Tick_Db'
DAILY_DB_NAME = 'VnTrader_Daily_Db'
MINUTE_DB_NAME = 'VnTrader_1Min_Db'
HOUR_DB_NAME = 'VnTrader_1Hour_Db'
POSITION_DB_NAME = 'VnTrader_Position_Db'
SETTING_DB_NAME = 'VnTrader_Setting_Db'
TURTLE_PORTFOLIO_DB_NAME = 'VnTrader_Turtle_Portfolio_Db'
DOMINANT_DB_NAME = 'Dominant_db'
ORDER_DB_NAME = 'VnTrader_Order_Db'
TRADE_DB_NAME = 'VnTrader_Trade_Db'

TRANSFORM_SYMBOL_LIST = {'SM':'2', 'TA':'2', 'ZC':'2', 'CF':'2', 'CJ':'2', 'CY':'2', 'OI':'2', 'RM':'2', 'SF':'2', 'SR':'2', 'FG':'2', 'RI':'2', 'SA':'2', 'AP':'2', 'JR':2, 'LR':2, 'MA':2}      # 交易合约和RQData合约需要转换，TA905 -> TA1905

def MinuteDataBaseName(duration:int):
    return re.sub("\d", f'{duration}', MINUTE_DB_NAME)

def HourDataBaseName(duration:int):
    return re.sub("\d", f'{duration}', HOUR_DB_NAME)


EXCHANGE_SYMBOL_DICT = {Exchange.CFFEX:['IF', 'IC', 'IH'],
                        Exchange.SHFE:['AL', 'RB', 'HC', 'RU'],
                        Exchange.CZCE:['SM', 'ZC', 'TA'],
                        Exchange.DCE:['I', 'JM', 'J'],
                        Exchange.BYBIT:['BTCUSDT', 'ETHUSDT']}

INTERVAL_DELTA_MAP = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}
