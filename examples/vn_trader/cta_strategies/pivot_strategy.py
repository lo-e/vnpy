from vnpy.app.cta_strategy.template import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
)
from vnpy.trader.utility import BarGenerator
from vnpy.app.cta_strategy.template import TradeMode
from vnpy.trader.constant import Interval
from datetime import datetime, timedelta
from typing import Callable

class PivotStrategy(CtaTemplate):

    author = "loe"

    parameters = []
    variables = []
    syncs = []

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = CustomBarGenerator(on_bar=self.on_bar,
                                     window=0,
                                     on_window_bar=self.on_generate_bar,
                                     interval=Interval.MINUTE)

    def on_init(self):
        """
        Callback when strategy is inited.
        """

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        if self.trade_mode == TradeMode.ACTUAL:
            self.load_bar(days=20, interval=Interval.MINUTE, callback=self.on_bar)

        elif self.trade_mode == TradeMode.BACKTESTING:
            self.load_bar(days=2, interval=Interval.MINUTE, callback=self.on_bar)

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
        self.bg.update_bar(bar)

        self.put_timer_event()

    # 周期数据源处理逻辑
    def on_generate_bar(self, bar:BarData):
        if bar.datetime >= datetime.strptime('2022-1-1', '%Y-%m-%d'):
            a = 2
        self.calculate_pivot(bar)

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

class CustomBarGenerator(BarGenerator):
    def __init__(self,
                 on_bar: Callable,
                 window: int = 0,
                 on_window_bar: Callable = None,
                 interval: Interval = Interval.MINUTE):
        super().__init__(on_bar, window, on_window_bar, interval)
        self.window_time = ['00:00:00', '08:00:00', '16:00:00']
        self.window_start = None
        self.window_end = None
        self.next_datetime = None
        self.window_bar = None

    def update_bar(self, bar: BarData):
        if not self.window_start:
            self.window_start = self.next_window_bar_datetime(current_datetime=bar.datetime)
            self.window_end = (self.next_window_bar_datetime(current_datetime=self.window_start + timedelta(minutes=1))) - timedelta(minutes=1)
            self.next_datetime = self.window_start

        valid = False
        if bar.datetime == self.next_datetime:
            valid = True
            self.next_datetime += timedelta(minutes=1)

        elif bar.datetime > self.next_datetime:
            valid = True
            self.next_datetime = bar.datetime + timedelta(minutes=1)
            msg = f'{self.__class__.__name__}：分钟数据缺失【{self.next_datetime} -- {bar.datetime - timedelta(minutes=1)}】'

        else:
            if not self.window_bar:
                return
            else:
                raise ('出现异常，检查代码！')
        if valid:
            if bar.datetime == self.window_start:
                self.window_bar = BarData(gateway_name='',
                                          symbol=bar.symbol,
                                          exchange=bar.exchange,
                                          datetime=self.window_start,
                                          endDatetime=self.window_end)
                self.window_bar.open_price = bar.open_price
                self.window_bar.high_price = bar.high_price
                self.window_bar.low_price = bar.low_price
                self.window_bar.close_price = bar.close_price
            else:
                if not self.window_bar:
                    raise ('出现异常，检查代码！')

                self.window_bar.close_price = bar.close_price
                self.window_bar.high_price = max(self.window_bar.high_price, bar.high_price)
                self.window_bar.low_price = min(self.window_bar.low_price, bar.low_price)

            if bar.datetime == self.window_end:
                self.on_window_bar(self.window_bar)

                self.window_start = self.next_window_bar_datetime(current_datetime=bar.datetime)
                self.window_end = (self.next_window_bar_datetime(current_datetime=self.window_start + timedelta(minutes=1))) - timedelta(minutes=1)



    def next_window_bar_datetime(self, current_datetime:datetime) -> datetime:
        the_datetime = current_datetime
        next_datetime = None
        n = 0
        while True:
            n += 1
            if next_datetime or n > 2:
                break

            year = the_datetime.year
            month = the_datetime.month
            day = the_datetime.day
            for x in self.window_time:
                temp_datetime = datetime.strptime(f'{year}-{month}-{day} {x}', '%Y-%m-%d %H:%M:%S')
                if current_datetime <= temp_datetime:
                    next_datetime = temp_datetime
                    break
            the_datetime += timedelta(days=1)

        return next_datetime


