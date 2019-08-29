from pathlib import Path

from vnpy.trader.app import BaseApp
from vnpy.trader.constant import Direction
from vnpy.trader.object import TickData, BarData, TradeData, OrderData
from vnpy.trader.utility import BarGenerator, ArrayManager

from .base import APP_NAME
from vnpy.app.cta_strategy.base import StopOrder

from .engine import TurtleEngine
from vnpy.app.cta_strategy.template import CtaTemplate


class TurtleCryptoApp(BaseApp):
    """"""

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "海归数字货币交易"
    engine_class = TurtleEngine
    widget_name = "TurtleManager"
    icon_name = "turtle_crypto.ico"
