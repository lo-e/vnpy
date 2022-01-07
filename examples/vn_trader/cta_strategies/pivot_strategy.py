from datetime import time
from vnpy.app.cta_strategy.template import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
)
from vnpy.trader.utility import BarGenerator, ArrayManager
from vnpy.app.cta_strategy.template import TradeMode
from vnpy.trader.constant import Interval, Direction
import datetime
from datetime import timedelta

class PivotStrategy(CtaTemplate):

    author = "loe"

    parameters = []
    variables = []
    syncs = []

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(window=500,
                               on_window_bar=self.on_generate_bar,
                               interval=Interval.MINUTE)

    def on_init(self):
        """
        Callback when strategy is inited.
        """

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        if self.trade_mode == TradeMode.ACTUAL:
            self.load_bar(days=20, interval=Interval.MINUTE, callback = self.on_bar)

        elif self.trade_mode == TradeMode.BACKTESTING:
            self.load_bar(days=2)
        else:
            raise(0)

        self.write_log("策略完成初始化")

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

    def on_tick(self, tick:TickData):
        """
        Callback of new tick data update.
        """

        if not self.trading:
            return
        self.put_timer_event()

    # 分钟数据处理逻辑
    def on_bar(self, bar:BarData):
        """
        Callback of new bar data update.
        """
        self.cancel_all()
        self.put_timer_event()

    # 周期数据源处理逻辑
    def on_generate_bar(self, bar:BarData):
        pass

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        # 邮件提醒
        super(PivotStrategy, self).on_trade(trade)

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass

    def calculate_pivot(self, bar:BarData):
        high = bar.high_price
        low = bar.low_price
        close = bar.close_price

        pivot = (high + low + 2*close) / 4
        r1 = 2*pivot - low
        s1 = 2*pivot - high
        r2 = pivot + (r1 - s1)
        s2 = pivot - (r1 - s1)
        r3 = high - (2*(low - pivot))
        s3 = low - (2*(high - pivot))

        sm1 = (pivot + s1) / 2
        sm2 = (s1 + s2) / 2
        sm3 = (s2 + s3) / 2
        rm1 = (pivot + r1) / 2
        rm2 = (r1 + r2) / 2
        rm3 = (r2 + r3) / 2

        if bar.datetime >= datetime.datetime.strptime('2022-1-6', '%Y-%m-%d'):
            a = 2

