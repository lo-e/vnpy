
from pathlib import Path

import importlib_metadata
from vnpy.trader.app import BaseApp
from vnpy.trader.constant import Direction
from vnpy.trader.object import TickData, BarData, TradeData, OrderData
from vnpy.trader.utility import BarGenerator, ArrayManager

from vnpy.app.cta_strategy.base import APP_NAME, StopOrder
from .engine import CtaEngine
from vnpy.app.cta_strategy.template import CtaTemplate, CtaSignal, TargetPosTemplate


try:
    __version__ = importlib_metadata.version("vnpy_ctastrategy")
except importlib_metadata.PackageNotFoundError:
    __version__ = "dev"


class CtaStrategyApp(BaseApp):
    """"""

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "CTA策略"
    engine_class = CtaEngine
    widget_name = "CtaManager"
    icon_name = "cta.ico"