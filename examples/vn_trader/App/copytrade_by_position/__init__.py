from pathlib import Path

from vnpy.trader.app import BaseApp
from vnpy.trader.constant import Direction
from vnpy.trader.object import TickData, BarData, TradeData, OrderData
from vnpy.trader.utility import BarGenerator, ArrayManager
from .base import APP_NAME
from vnpy.app.cta_strategy.base import StopOrder
from .engine import CopytradeEngine

class CopytradeByPositionApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "跟单交易（Position）"
    engine_class = CopytradeEngine
    widget_name = "CopytradeManager"
    icon_name = "copytrade_position.ico"
