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
from vnpy.trader.constant import Interval
from datetime import datetime, timedelta
from typing import Callable
from vnpy.trader.utility import round_to, csv_saving
from vnpy.trader.constant import Offset

window_time = ['00:00:00', '08:00:00', '16:00:00']

class PivotStrategy_backtesting(CtaTemplate):

    author = "loe"

    exit_rate = 0.001
    exit_window = 500
    min_volume = 0.001

    # ======================================
    long_entry3 = 0
    long_volume3 = 0
    long_exit3 = 0

    long_entry2 = 0
    long_volume2 = 0
    long_exit2 = 0

    long_entry1 = 0
    long_volume1 = 0
    long_exit1 = 0

    pivot = 0

    short_entry1 = 0
    short_volume1 = 0
    short_exit1 = 0

    short_entry2 = 0
    short_volume2 = 0
    short_exit2 = 0

    short_entry3 = 0
    short_volume3 = 0
    short_exit3 = 0
    # ======================================

    base_datetime = None
    long_allowed1 = False
    long_allowed2 = False
    long_profit_ready1 = False
    long_profit_ready2 = False
    short_allowed1 = False
    short_allowed2 = False
    short_profit_ready1 = False
    short_profit_ready2 = False
    long_profit_exit = 0
    short_profit_exit = 0
    long_orderid1 = ''
    long_orderid2 = ''
    short_orderid1 = ''
    short_orderid2 = ''
    long_cross1 = False
    long_cross2 = False
    short_cross1 = False
    short_cross2 = False

    long_high1 = 0
    long_high2 = 0
    short_low1 = 0
    short_low2 = 0

    parameters = ['exit_rate',
                  'exit_window',
                  'min_volume']

    variables = ['base_datetime',
                 'long_entry3',
                 'long_volume3',
                 'long_exit3',
                 'long_entry2',
                 'long_volume2',
                 'long_exit2',
                 'long_entry1',
                 'long_volume1',
                 'long_exit1',
                 'pivot',
                 'short_exit1',
                 'short_volume1',
                 'short_entry1',
                 'short_exit2',
                 'short_volume2',
                 'short_entry2',
                 'short_exit3',
                 'short_volume3',
                 'short_entry3',
                 'long_allowed1',
                 'long_allowed2',
                 'short_allowed1',
                 'short_allowed2',
                 'long_profit_exit',
                 'short_profit_exit']
    syncs = []

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = CustomBarGenerator(on_bar=self.on_bar,
                                     window=0,
                                     on_window_bar=self.on_generate_bar,
                                     interval=Interval.MINUTE)
        self.am = ArrayManager(size=self.exit_window + 1)

        """ fake """
        self.csv_list = []

        """ fake """
        self.cross_count = 0
        self.window_count = 0
        self.cross_enable = False

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        # 载入历史数据，并采用回放计算的方式初始化策略数值
        self.load_bar(days=2, interval=Interval.MINUTE, callback=self.on_bar)

        self.write_log("策略完成初始化")
        self.put_timer_event()

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
        """ fake """
        self.bar = bar
        if (bar.high_price >= (3*(self.long_entry3 - self.pivot) + self.pivot) or bar.low_price <= (self.pivot - 3*(self.pivot - self.short_entry3))) and self.cross_enable:
            self.cross_enable = False
            self.cross_count += 1
            print(f'{bar.datetime}\tcross_count:{self.cross_count}\twindow_count:{self.window_count}')

        self.cancel_all()
        self.long_orderid1 = ''
        self.long_orderid2 = ''
        self.short_orderid1 = ''
        self.short_orderid2 = ''

        if not self.inited:
            self.bg.update_bar(bar)
            self.am.update_bar(bar)
            return

        if not self.am.inited:
            return

        # 计算移动止盈价格
        self.short_profit_exit, self.long_profit_exit = self.am.donchian(self.exit_window, False)
        self.long_profit_ready1 = False
        self.long_profit_ready2 = False
        self.short_profit_ready1 = False
        self.short_profit_ready2 = False

        """ fake """
        if bar.datetime >= datetime.strptime('2020-01-31 00:32:00', '%Y-%m-%d %H:%M:%S'):
            a = 2

        # 判断是否重新开仓准许
        if not self.long_cross1 and not self.long_allowed1 and bar.low_price <= self.long_exit1:
            self.long_allowed1 = True

        if not self.long_cross2 and not self.long_allowed2 and bar.low_price <= self.long_exit2:
            self.long_allowed2 = True

        if not self.short_cross1 and not self.short_allowed1 and bar.high_price >= self.short_exit1:
            self.short_allowed1 = True

        if not self.short_cross2 and not self.short_allowed2 and bar.high_price >= self.short_exit2:
            self.short_allowed2 = True

        next_window_datetime = next_window_bar_datetime(current_datetime=bar.datetime + timedelta(minutes=2))
        if bar.datetime >= next_window_datetime - timedelta(minutes=6):
            # 周期结束前平仓
            if self.pos > 0:
                if self.long_cross1:
                    self.sell(price=bar.close_price - 100*self.cta_engine.pricetick, volume=abs(self.long_volume1), stop=False)

                if self.long_cross2:
                    self.sell(price=bar.close_price - 100 * self.cta_engine.pricetick, volume=abs(self.long_volume2), stop=False)

                self.long_cross1 = False
                self.long_cross2 = False

            if self.pos < 0:
                if self.short_cross1:
                    self.cover(price=bar.close_price + 100*self.cta_engine.pricetick, volume=abs(self.short_volume1), stop=False)

                if self.short_cross2:
                    self.cover(price=bar.close_price + 100*self.cta_engine.pricetick, volume=abs(self.short_volume2), stop=False)

                self.short_cross1 = False
                self.short_cross2 = False

        elif self.pivot:

            # 多头一级开平仓
            if not self.long_cross1:
                self.long_high1 = 0

                if self.long_allowed1:
                    self.long_orderid1 = self.buy(price=self.long_entry1, volume=abs(self.long_volume1), stop=True)[0]

            else:
                self.long_high1 = max(self.long_high1, bar.high_price)

                if self.long_profit_exit > self.long_entry1:
                    # 止盈
                    exit_price = self.long_profit_exit
                    self.long_profit_ready1 = True
                else:
                    # 止损
                    exit_rate_price = self.long_high1*(1-self.exit_rate)
                    exit_price = min(exit_rate_price, self.long_entry1)
                    exit_price = max(exit_price, self.long_exit1)

                self.long_orderid1 = self.sell(price=exit_price, volume=abs(self.long_volume1), stop=True)[0]

            # 多头二级开平仓
            if not self.long_cross2:
                self.long_high2 = 0

                if self.long_allowed2:
                    self.long_orderid2 = self.buy(price=self.long_entry2, volume=abs(self.long_volume2), stop=True)[0]

            else:
                self.long_high2 = max(self.long_high2, bar.high_price)

                if self.long_profit_exit > self.long_entry2:
                    # 止盈
                    exit_price = self.long_profit_exit
                    self.long_profit_ready2 = True
                else:
                    # 止损
                    exit_rate_price = self.long_high2 * (1 - self.exit_rate)
                    exit_price = min(exit_rate_price, self.long_entry2)
                    exit_price = max(exit_price, self.long_exit2)

                self.long_orderid2 = self.sell(price=exit_price, volume=abs(self.long_volume2), stop=True)[0]

            # 空头一级开平仓
            if not self.short_cross1:
                self.short_low1 = 0

                if self.short_allowed1:
                    self.short_orderid1 = self.short(price=self.short_entry1, volume=abs(self.short_volume1), stop=True)[0]

            else:
                if not self.short_low1:
                    self.short_low1 = bar.low_price
                else:
                    self.short_low1 = min(self.short_low1, bar.low_price)

                if self.short_profit_exit < self.short_entry1:
                    # 止盈
                    exit_price = self.short_profit_exit
                    self.short_profit_ready1 = True
                else:
                    # 止损
                    exit_rate_price = self.short_low1 * (1 + self.exit_rate)
                    exit_price = max(exit_rate_price, self.short_entry1)
                    exit_price = min(exit_price, self.short_exit1)

                self.short_orderid1 = self.cover(price=exit_price, volume=abs(self.short_volume1), stop=True)[0]

            # 空头二级开平仓
            if not self.short_cross2:
                self.short_low2 = 0

                if self.short_allowed2:
                    self.short_orderid2 = self.short(price=self.short_entry2, volume=abs(self.short_volume2), stop=True)[0]

            else:
                if not self.short_low2:
                    self.short_low2 = bar.low_price
                else:
                    self.short_low2 = min(self.short_low2, bar.low_price)

                if self.short_profit_exit < self.short_entry2:
                    # 止盈
                    exit_price = self.short_profit_exit
                    self.short_profit_ready2 = True
                else:
                    # 止损
                    exit_rate_price = self.short_low2 * (1 + self.exit_rate)
                    exit_price = max(exit_rate_price, self.short_entry2)
                    exit_price = min(exit_price, self.short_exit2)

                self.short_orderid2 = self.cover(price=exit_price, volume=abs(self.short_volume2), stop=True)[0]

        self.bg.update_bar(bar)
        self.am.update_bar(bar)
        self.put_timer_event()

    # 周期数据源处理逻辑
    def on_generate_bar(self, bar:BarData):
        """ fake """
        self.window_count += 1
        self.cross_enable = True
        if bar.datetime >= datetime.strptime('2022-2-21', '%Y-%m-%d'):
            print(self.window_count)

        self.calculate_pivot(bar)
        self.base_datetime = bar.datetime
        self.long_allowed1 = True
        self.long_allowed2 = True
        self.short_allowed1 = True
        self.short_allowed2 = True

        """ fake """
        dict = {'symbol':self.vt_symbol,
                'datetime':bar.datetime,
                'long_entry3': self.long_entry3,
                'long_exit3': self.long_exit3,
                'long_entry2':self.long_entry2,
                'long_exit2':self.long_exit2,
                'long_entry1':self.long_entry1,
                'long_exit1':self.long_exit1,
                'pivot':self.pivot,
                'short_exit1': self.short_exit1,
                'short_entry1':self.short_entry1,
                'short_exit2': self.short_exit2,
                'short_entry2':self.short_entry2,
                'short_exit3': self.short_exit3,
                'short_entry3': self.short_entry3}
        self.csv_list.append(dict)
        if bar.datetime >= datetime.strptime('2022-01-02 00:00:00', '%Y-%m-%d %H:%M:%S'):
            csv_saving(file_name=f'{self.vt_symbol}.csv', data_list=self.csv_list)

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
                #self.long_allowed1 = False
            else:
                self.long_cross1 = False
                if self.long_profit_ready1:
                    self.long_allowed1 = False

        if trade.orderid == self.long_orderid2:
            if trade.offset == Offset.OPEN:
                self.long_cross2 = True
                #self.long_allowed2 = False
            else:
                self.long_cross2 = False
                if self.long_profit_ready2:
                    self.long_allowed2 = False

        if trade.orderid == self.short_orderid1:
            if trade.offset == Offset.OPEN:
                self.short_cross1 = True
                #self.short_allowed1 = False
            else:
                self.short_cross1 = False
                if self.short_profit_ready1:
                    self.short_allowed1 = False

        if trade.orderid == self.short_orderid2:
            if trade.offset == Offset.OPEN:
                self.short_cross2 = True
                #self.short_allowed2 = False
            else:
                self.short_cross2 = False
                if self.short_profit_ready2:
                    self.short_allowed2 = False

        # 邮件提醒
        super().on_trade(trade)

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

        self.short_exit1 = (self.pivot + self.short_entry1) / 2
        self.short_exit2 = (self.short_entry1 + self.short_entry2) / 2
        self.short_exit3 = (self.short_entry2 + self.short_entry3) / 2
        self.long_exit1 = (self.pivot + self.long_entry1) / 2
        self.long_exit2 = (self.long_entry1 + self.long_entry2) / 2
        self.long_exit3 = (self.long_entry2 + self.long_entry3) / 2

        """
        self.short_exit1 = self.pivot
        self.short_exit2 = self.short_entry1
        self.short_exit3 = self.short_entry2
        self.long_exit1 = self.pivot
        self.long_exit2 = self.long_entry1
        self.long_exit3 = self.long_entry2
        """

        max_unit_loss = 0.005 * self.cta_engine.capital
        self.long_volume1 = round_to(max_unit_loss / (self.long_entry1 - self.long_exit1), self.min_volume)
        self.long_volume2 = round_to(max_unit_loss / (self.long_entry2 - self.long_exit2), self.min_volume)
        self.long_volume3 = round_to(max_unit_loss / (self.long_entry3 - self.long_exit3), self.min_volume)
        self.short_volume1 = round_to(max_unit_loss / (self.short_exit1 - self.short_entry1), self.min_volume)
        self.short_volume2 = round_to(max_unit_loss / (self.short_exit2 - self.short_entry2), self.min_volume)
        self.short_volume3 = round_to(max_unit_loss / (self.short_exit3 - self.short_entry3), self.min_volume)

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


