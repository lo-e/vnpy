from pathlib import Path

from vnpy.trader.app import BaseApp
from vnpy.trader.constant import Direction
from vnpy.trader.object import TickData, BarData, TradeData, OrderData
from vnpy.trader.utility import BarGenerator, ArrayManager
from .base import APP_NAME
from vnpy.app.cta_strategy.base import StopOrder
from .engine import SupportResistanceEngine

class SupportResistanceApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "支撑压力策略"
    engine_class = SupportResistanceEngine
    widget_name = "SupportResistanceManager"
    icon_name = "support_resistance.ico"
