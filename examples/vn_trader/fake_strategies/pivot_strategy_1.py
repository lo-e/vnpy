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
from vnpy.trader.utility import round_to
from vnpy.trader.constant import Offset

window_time = ['00:00:00', '08:00:00', '16:00:00']

class PivotStrategy(CtaTemplate):

    author = "loe"

    move_profit_rate = 0.002
    min_volume = 0.001

    long_entry3 = 0
    long_entry2 = 0
    long_volume2 = 0
    long_entry1 = 0
    long_volume1 = 0
    pivot = 0
    short_entry1 = 0
    short_volume1 = 0
    short_entry2 = 0
    short_volume2 = 0
    short_entry3 = 0

    long_entry_high1 = 0
    long_entry_high2 = 0
    short_entry_low1 = 0
    short_entry_low2 = 0
    long_orderid1 = ''
    long_orderid2 = ''
    short_orderid1 = ''
    short_orderid2 = ''
    long_cross1 = False
    long_cross2 = False
    short_cross1 = False
    short_cross2 = False

    parameters = ['move_profit_rate',
                  'min_volume']

    variables = ['long_entry3',
                 'long_entry2',
                 'long_volume2',
                 'long_entry1',
                 'long_volume1',
                 'pivot',
                 'short_entry1',
                 'short_volume1',
                 'short_entry2',
                 'short_volume2',
                 'short_entry3',
                 'entry_high',
                 'entry_low']
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
        self.long_orderid1 = ''
        self.long_orderid2 = ''
        self.short_orderid1 = ''
        self.short_orderid2 = ''

        self.bg.update_bar(bar)

        if not self.inited:
            return

        #方案一
        #"""
        next_window_datetime = next_window_bar_datetime(current_datetime=bar.datetime + timedelta(minutes=1))
        if bar.datetime >= next_window_datetime - timedelta(minutes=6):
            # 周期结束前平仓
            if self.pos > 0:
                self.sell(price=bar.close_price - 100*self.cta_engine.pricetick, volume=abs(self.pos), stop=False)
                self.long_cross1 = False
                self.long_cross2 = False

            if self.pos < 0:
                self.cover(price=bar.close_price + 100*self.cta_engine.pricetick, volume=abs(self.pos), stop=False)
                self.short_cross1 = False
                self.short_cross2 = False

        elif self.pivot:
            """ fake """
            if bar.datetime >= datetime.strptime('2021-10-04 01:46:00', '%Y-%m-%d %H:%M:%S'):
                a = 2

            # 多头一级开平仓
            if not self.long_cross1:
                self.long_entry_high1 = 0
                self.long_orderid1 = self.buy(price=self.long_entry1, volume=abs(self.long_volume1), stop=True)[0]

            else:
                self.long_entry_high1 = max(self.long_entry_high1, bar.high_price)
                move_exit = self.long_entry_high1 * (1 - self.move_profit_rate)
                if move_exit < self.long_entry1:
                    exit_price = max(move_exit, self.pivot)
                else:
                    exit_price = self.long_entry1
                self.long_orderid1 = self.sell(price=exit_price, volume=abs(self.long_volume1), stop=True)[0]

            # 多头二级开平仓
            if not self.long_cross2:
                self.long_entry_high2 = 0
                self.long_orderid2 = self.buy(price=self.long_entry2, volume=abs(self.long_volume2), stop=True)[0]

            else:
                self.long_entry_high2 = max(self.long_entry_high2, bar.high_price)
                move_exit = self.long_entry_high2 * (1 - self.move_profit_rate)
                if move_exit < self.long_entry2:
                    exit_price = max(move_exit, self.long_entry1)
                else:
                    exit_price = self.long_entry2
                self.long_orderid2 = self.sell(price=exit_price, volume=abs(self.long_volume2), stop=True)[0]

            # 空头一级开平仓
            if not self.short_cross1:
                self.short_entry_low1 = 0
                self.short_orderid1 = self.short(price=self.short_entry1, volume=abs(self.short_volume1), stop=True)[0]

            else:
                if not self.short_entry_low1:
                    self.short_entry_low1 = bar.low_price
                else:
                    self.short_entry_low1 = min(self.short_entry_low1, bar.low_price)
                move_exit = self.short_entry_low1 * (1 + self.move_profit_rate)
                if move_exit > self.short_entry1:
                    exit_price = min(move_exit, self.pivot)
                else:
                    exit_price = self.short_entry1
                self.short_orderid1 = self.cover(price=exit_price, volume=abs(self.short_volume1), stop=True)[0]

            # 空头二级开平仓
            if not self.short_cross2:
                self.short_entry_low2 = 0
                self.short_orderid2 = self.short(price=self.short_entry2, volume=abs(self.short_volume2), stop=True)[0]

            else:
                if not self.short_entry_low2:
                    self.short_entry_low2 = bar.low_price
                else:
                    self.short_entry_low2 = min(self.short_entry_low2, bar.low_price)
                move_exit = self.short_entry_low2 * (1 + self.move_profit_rate)
                if move_exit > self.short_entry2:
                    exit_price = min(move_exit, self.short_entry1)
                else:
                    exit_price = self.short_entry2
                self.short_orderid2 = self.cover(price=exit_price, volume=abs(self.short_volume2), stop=True)[0]
        #"""

        #方案二
        """
        next_window_datetime = next_window_bar_datetime(current_datetime=bar.datetime + timedelta(minutes=1))
        if bar.datetime >= next_window_datetime - timedelta(minutes=6):
            # 周期结束前平仓
            if self.pos > 0:
                self.sell(price=bar.close_price - 100 * self.cta_engine.pricetick, volume=abs(self.pos), stop=False)

            if self.pos < 0:
                self.cover(price=bar.close_price + 100 * self.cta_engine.pricetick, volume=abs(self.pos), stop=False)

        elif self.pivot:
            if not self.pos:
                self.entry_high = 0
                self.entry_low = 0

                self.long_orderid1 = self.buy(price=self.long_entry1, volume=self.long_volume1, stop=True)[0]
                self.long_orderid2 = self.buy(price=self.long_entry2, volume=self.long_volume2, stop=True)[0]

                self.short_orderid1 = self.short(price=self.short_entry1, volume=self.short_volume1, stop=True)[0]
                self.short_orderid2 = self.short(price=self.short_entry2, volume=self.short_volume2, stop=True)[0]

            elif self.pos > 0:
                self.entry_high = max(self.entry_high, bar.high_price)

                move_exit = self.entry_high * (1 - self.move_profit_rate)
                if self.pos > self.long_volume1:
                    # 已经建立二级仓位，发出平仓停止单
                    if move_exit < self.long_entry2:
                        exit_price = max(move_exit, self.long_entry1)
                    else:
                        exit_price = self.long_entry2

                else:
                    # 只建立一级仓位，同时发出二级开仓停止单和平仓停止单
                    self.buy(price=self.long_entry2, volume=self.long_volume2, stop=True)

                    if move_exit < self.long_entry1:
                        exit_price = max(move_exit, self.pivot)
                    else:
                        exit_price = self.long_entry1

                self.sell(price=exit_price, volume=abs(self.pos), stop=True)

            else:
                if not self.entry_low:
                    self.entry_low = bar.low_price
                else:
                    self.entry_low = min(self.entry_low, bar.low_price)

                move_exit = self.entry_low * (1 + self.move_profit_rate)
                if abs(self.pos) > self.short_volume1:
                    # 已经建立二级仓位，发出平仓停止单
                    if move_exit > self.short_entry2:
                        exit_price = min(move_exit, self.short_entry1)
                    else:
                        exit_price = self.short_entry2

                else:
                    # 只建立一级仓位，同时发出二级开仓停止单和平仓停止单
                    self.short(price=self.short_entry2, volume=self.short_volume2, stop=True)

                    if move_exit > self.short_entry1:
                        exit_price = min(move_exit, self.pivot)
                    else:
                        exit_price = self.short_entry1

                self.cover(price=exit_price, volume=abs(self.pos), stop=True)
        """

        self.put_timer_event()

    # 周期数据源处理逻辑
    def on_generate_bar(self, bar:BarData):
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
        if trade.orderid == self.long_orderid1:
            if trade.offset == Offset.OPEN:
                self.long_cross1 = True
            else:
                self.long_cross1 = False

        if trade.orderid == self.long_orderid2:
            if trade.offset == Offset.OPEN:
                self.long_cross2 = True
            else:
                self.long_cross2 = False

        if trade.orderid == self.short_orderid1:
            if trade.offset == Offset.OPEN:
                self.short_cross1 = True
            else:
                self.short_cross1 = False

        if trade.orderid == self.short_orderid2:
            if trade.offset == Offset.OPEN:
                self.short_cross2 = True
            else:
                self.short_cross2 = False

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

        self.pivot = (high + low + 2*close) / 4
        self.long_entry1 = 2*self.pivot - low
        self.short_entry1 = 2*self.pivot - high
        self.long_entry2 = self.pivot + (self.long_entry1 - self.short_entry1)
        self.short_entry2 = self.pivot - (self.long_entry1 - self.short_entry1)
        self.long_entry3 = high - (2*(low - self.pivot))
        self.short_entry3 = low - (2*(high - self.pivot))

        sm1 = (self.pivot + self.short_entry1) / 2
        sm2 = (self.short_entry1 + self.short_entry2) / 2
        sm3 = (self.short_entry2 + self.short_entry3) / 2
        rm1 = (self.pivot + self.long_entry1) / 2
        rm2 = (self.long_entry1 + self.long_entry2) / 2
        rm3 = (self.long_entry2 + self.long_entry3) / 2

        max_unit_loss = 0.01 * self.cta_engine.capital
        self.long_volume1 = round_to(max_unit_loss / (self.long_entry1 - self.pivot), self.min_volume)
        self.long_volume2 = round_to(max_unit_loss / (self.long_entry2 - self.long_entry1), self.min_volume)
        self.short_volume1 = round_to(max_unit_loss / (self.pivot - self.short_entry1), self.min_volume)
        self.short_volume2 = round_to(max_unit_loss / (self.short_entry1 - self.short_entry2), self.min_volume)

class CustomBarGenerator(BarGenerator):
    def __init__(self,
                 on_bar: Callable,
                 window: int = 0,
                 on_window_bar: Callable = None,
                 interval: Interval = Interval.MINUTE):
        super().__init__(on_bar, window, on_window_bar, interval)
        self.window_start = None
        self.window_end = None
        self.next_datetime = None
        self.window_bar = None

    def update_bar(self, bar: BarData):
        if not self.window_start:
            self.window_start = next_window_bar_datetime(current_datetime=bar.datetime)
            self.window_end = (next_window_bar_datetime(current_datetime=self.window_start + timedelta(minutes=1))) - timedelta(minutes=1)
            self.next_datetime = self.window_start

        if bar.datetime == self.next_datetime:
            valid = True
            self.next_datetime += timedelta(minutes=1)

        elif bar.datetime > self.next_datetime:
            valid = True
            msg = f'{self.__class__.__name__}：分钟数据缺失【{self.next_datetime} -- {bar.datetime - timedelta(minutes=1)}】'
            print(msg)
            self.next_datetime = bar.datetime + timedelta(minutes=1)

            if not self.window_bar:
                self.window_start = bar.datetime
                self.window_end = (next_window_bar_datetime(current_datetime=self.window_start + timedelta(minutes=1))) - timedelta(minutes=1)

            if bar.datetime > self.window_end:
                # 提前end
                self.on_window_bar(self.window_bar)
                self.window_bar = None

                self.window_start = bar.datetime
                self.window_end = (next_window_bar_datetime(current_datetime=self.window_start + timedelta(minutes=1))) - timedelta(minutes=1)

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
            self.window_bar = None

            self.window_start = next_window_bar_datetime(current_datetime=bar.datetime)
            self.window_end = (next_window_bar_datetime(current_datetime=self.window_start + timedelta(minutes=1))) - timedelta(minutes=1)



def next_window_bar_datetime(current_datetime:datetime) -> datetime:
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
        for x in window_time:
            temp_datetime = datetime.strptime(f'{year}-{month}-{day} {x}', '%Y-%m-%d %H:%M:%S')
            if current_datetime <= temp_datetime:
                next_datetime = temp_datetime
                break
        the_datetime += timedelta(days=1)

    return next_datetime


