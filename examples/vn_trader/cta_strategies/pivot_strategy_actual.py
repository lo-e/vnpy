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
from vnpy.trader.utility import round_to
from decimal import Decimal
import traceback

window_time = ['00:00:00', '08:00:00', '16:00:00']

class PivotStrategy_actual(CtaTemplate):

    author = "loe"

    capital = 0.0
    exit_rate = 0.002
    exit_window = 50
    min_volume = 0.001
    max_slipper = 10
    best_limit_algo_trading = False

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
    base_datetime_checking = False
    long_allowed1 = False
    long_allowed2 = False
    short_allowed1 = False
    short_allowed2 = False
    long_profit_exit = 0
    short_profit_exit = 0

    long_high1 = 0
    long_high2 = 0
    short_low1 = 0
    short_low2 = 0

    long_entry_algo1 = ''
    long_exit_algo1 = ''
    long_entry_algo2 = ''
    long_exit_algo2 = ''
    short_entry_algo1 = ''
    short_exit_algo1 = ''
    short_entry_algo2 = ''
    short_exit_algo2 = ''
    long_traded1 = 0
    long_traded2 = 0
    short_traded1 = 0
    short_traded2 = 0

    parameters = ['capital',
                  'exit_rate',
                  'exit_window',
                  'min_volume',
                  'best_limit_algo_trading']

    variables = ['base_datetime',
                 'long_traded1',
                 'long_traded2',
                 'short_traded1',
                 'short_traded2',
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
                 'long_entry_algo1',
                 'long_exit_algo1',
                 'long_entry_algo2',
                 'long_exit_algo2',
                 'short_entry_algo1',
                 'short_exit_algo1',
                 'short_entry_algo2',
                 'short_exit_algo2',
                 'long_high1',
                 'long_high2',
                 'short_low1',
                 'short_low2']

    syncs = ['long_traded1',
             'long_traded2',
             'short_traded1',
             'short_traded2',
             'long_high1',
             'long_high2',
             'short_low1',
             'short_low2']

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.actual_tick_price = self.get_tick_price()

        self.bg = CustomBarGenerator(on_bar=self.on_bar,
                                     window=0,
                                     on_window_bar=self.on_generate_bar,
                                     interval=Interval.MINUTE)
        self.am = ArrayManager(size=self.exit_window + 1)
        self.last_tick = None

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        # 初始化数据
        self.clear_data()

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        if self.trade_mode == TradeMode.ACTUAL:
            self.load_bar(days=2, interval=Interval.MINUTE, callback=self.on_bar)

        elif self.trade_mode == TradeMode.BACKTESTING:
            self.load_bar(days=2, interval=Interval.MINUTE, callback=self.on_bar)

        else:
            raise(0)

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

        if not self.inited:
            return

        if not self.trading:
            return

        if not self.am.inited:
            return

        # 只要最新tick
        if self.last_tick and self.last_tick.datetime >= tick.datetime:
                return
        self.last_tick = tick

        next_window_datetime = next_window_bar_datetime(current_datetime=tick.datetime - timedelta(minutes=5))
        if without_timezone(tick.datetime) >= next_window_datetime - timedelta(minutes=5):
            # 周期结束前平仓
            if self.long_traded1 and not self.long_exit_algo1:
                self.long_exit_algo1 = self.sell(price=tick.last_price-200*self.actual_tick_price, volume=abs(self.long_traded1))

            if self.long_traded2 and not self.long_exit_algo2:
                self.long_exit_algo2 = self.sell(price=tick.last_price-200*self.actual_tick_price, volume=abs(self.long_traded2))

            if self.short_traded1 and not self.short_exit_algo1:
               self.short_exit_algo1 = self.cover(price=tick.last_price+200*self.actual_tick_price, volume=abs(self.short_traded1))

            if self.short_traded2 and not self.short_exit_algo2:
               self.short_exit_algo2 = self.cover(price=tick.last_price+200*self.actual_tick_price, volume=abs(self.short_traded2))

        elif self.pivot and self.base_datetime_checking:
            has_exit = False
            # 一级多头平仓
            if self.long_traded1:
                self.long_high1 = max(self.long_high1, tick.last_price)

                exit_rate_price = self.long_high1 * (1 - self.exit_rate)
                exit_price = min(exit_rate_price, self.long_entry1)
                exit_price = max(exit_price, self.long_exit1)
                if tick.bid_price_1 <= exit_price:
                    # 止损
                    has_exit = True
                    if self.long_entry_algo1:
                        self.cta_engine.stop_algo(self.long_entry_algo1)

                    elif not self.long_exit_algo1:
                        self.long_exit_algo1 = self.sell(price=tick.last_price-200*self.actual_tick_price,
                                                         volume=abs(self.long_traded1))

            # 二级多头平仓
            if self.long_traded2:
                self.long_high2 = max(self.long_high2, tick.last_price)

                exit_rate_price = self.long_high2 * (1 - self.exit_rate)
                exit_price = min(exit_rate_price, self.long_entry2)
                exit_price = max(exit_price, self.long_exit2)
                if tick.bid_price_1 <= exit_price:
                    # 止损
                    has_exit = True
                    if self.long_entry_algo2:
                        self.cta_engine.stop_algo(self.long_entry_algo2)

                    elif not self.long_exit_algo2:
                        self.long_exit_algo2 = self.sell(price=tick.last_price - 200 * self.actual_tick_price,
                                                         volume=abs(self.long_traded2))

            # 一级空头平仓
            if self.short_traded1:
                if not self.short_low1:
                    self.short_low1 = tick.last_price
                else:
                    self.short_low1 = min(self.short_low1, tick.last_price)

                exit_rate_price = self.short_low1 * (1 + self.exit_rate)
                exit_price = max(exit_rate_price, self.short_entry1)
                exit_price = min(exit_price, self.short_exit1)
                if tick.ask_price_1 >= exit_price:
                    # 止损
                    has_exit = True
                    if self.short_entry_algo1:
                        self.cta_engine.stop_algo(self.short_entry_algo1)

                    elif not self.short_exit_algo1:
                        self.short_exit_algo1 = self.cover(price=tick.last_price+200*self.actual_tick_price,
                                                           volume=abs(self.short_traded1))

            # 二级空头平仓
            if self.short_traded2:
                if not self.short_low2:
                    self.short_low2 = tick.last_price
                else:
                    self.short_low2 = min(self.short_low2, tick.last_price)

                exit_rate_price = self.short_low2 * (1 + self.exit_rate)
                exit_price = max(exit_rate_price, self.short_entry2)
                exit_price = min(exit_price, self.short_exit2)
                if tick.ask_price_1 >= exit_price:
                    # 止损
                    has_exit = True
                    if self.short_entry_algo2:
                        self.cta_engine.stop_algo(self.short_entry_algo2)

                    elif not self.short_exit_algo2:
                        self.short_exit_algo2 = self.cover(price=tick.last_price + 200 * self.actual_tick_price,
                                                           volume=abs(self.short_traded2))

            if not has_exit and not self.long_exit_algo1 and not self.long_exit_algo2 and not self.short_exit_algo1 and not self.short_exit_algo2:
                has_long_entry = False
                # 一级多头开仓
                if not self.long_entry_algo1 and self.long_traded1 < self.long_volume1 and tick.ask_price_1 >= self.long_entry1:
                    # 避免滑点过高的过滤
                    limit_price = self.long_entry1 + self.max_slipper * self.actual_tick_price
                    if (tick.ask_price_1 - self.actual_tick_price) <= limit_price:
                        has_long_entry = True
                        volume = float(Decimal(str(self.long_volume1)) - Decimal(str(self.long_traded1)))
                        self.long_entry_algo1 = self.buy(price=tick.last_price + 200*self.actual_tick_price, volume=volume, limit_price=limit_price)

                # 二级多头开仓
                if not self.long_entry_algo2 and self.long_traded2 < self.long_volume2 and tick.ask_price_1 >= self.long_entry2:
                    # 避免滑点过高的过滤
                    limit_price = self.long_entry2 + self.max_slipper * self.actual_tick_price
                    if (tick.ask_price_1 - self.actual_tick_price) <= limit_price:
                        has_long_entry = True
                        volume = float(Decimal(str(self.long_volume2)) - Decimal(str(self.long_traded2)))
                        self.long_entry_algo2 = self.buy(price=tick.last_price + 200*self.actual_tick_price, volume=volume, limit_price=limit_price)

                if not has_long_entry:
                    # 一级空头开仓
                    if not self.short_entry_algo1 and self.short_traded1 < self.short_volume1 and tick.bid_price_1 <= self.short_entry1:
                        # 避免滑点过高的过滤
                        limit_price = self.short_entry1 - self.max_slipper * self.actual_tick_price
                        if (tick.bid_price_1 + self.actual_tick_price) >= limit_price:
                            volume = float(Decimal(str(self.short_volume1)) - Decimal(str(self.short_traded1)))
                            self.short_entry_algo1 = self.short(price=tick.last_price - 200*self.actual_tick_price, volume=volume, limit_price=limit_price)

                    # 二级空头开仓
                    if not self.short_entry_algo2 and self.short_traded2 < self.short_volume2 and tick.bid_price_1 <= self.short_entry2:
                        # 避免滑点过高的过滤
                        limit_price = self.short_entry2 - self.max_slipper * self.actual_tick_price
                        if (tick.bid_price_1 + self.actual_tick_price) >= limit_price:
                            volume = float(Decimal(str(self.short_volume2)) - Decimal(str(self.short_traded2)))
                            self.short_entry_algo2 = self.short(price=tick.last_price - 200*self.actual_tick_price, volume=volume, limit_price=limit_price)


        self.put_timer_event()

    # 分钟数据处理逻辑
    def on_bar(self, bar:BarData):
        """
        Callback of new bar data update.
        """
        self.bg.update_bar(bar)
        self.am.update_bar(bar)
        self.put_timer_event()

    # 周期数据源处理逻辑
    def on_generate_bar(self, bar:BarData):
        self.calculate_pivot(bar)
        self.base_datetime = next_window_bar_datetime(bar.datetime + timedelta(seconds=1))
        self.long_allowed1 = True
        self.long_allowed2 = True
        self.short_allowed1 = True
        self.short_allowed2 = True

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
        super().on_trade(trade)

        """
        try:
            # 邮件提醒
            super().on_trade(trade)
        except:
            content = f'【未知错误】\n\n{traceback.format_exc()}'
            print(content)
        """

    def on_algo_trade(self, algo, trade:TradeData):
        if algo.algo_name == self.long_entry_algo1:
            # 一级多头开仓成交
            self.long_traded1 = float(Decimal(str(self.long_traded1)) + Decimal(str(trade.volume)))

        if algo.algo_name == self.long_exit_algo1:
            # 一级多头平仓成交
            self.long_traded1 = float(Decimal(str(self.long_traded1)) - Decimal(str(trade.volume)))

        if algo.algo_name == self.long_entry_algo2:
            # 二级多头开仓成交
            self.long_traded2 = float(Decimal(str(self.long_traded2)) + Decimal(str(trade.volume)))

        if algo.algo_name == self.long_exit_algo2:
            # 二级多头平仓成交
            self.long_traded2 = float(Decimal(str(self.long_traded2)) - Decimal(str(trade.volume)))

        if algo.algo_name == self.short_entry_algo1:
            # 一级空头开仓成交
            self.short_traded1 = float(Decimal(str(self.short_traded1)) + Decimal(str(trade.volume)))

        if algo.algo_name == self.short_exit_algo1:
            # 一级空头平仓成交
            self.short_traded1 = float(Decimal(str(self.short_traded1)) - Decimal(str(trade.volume)))

        if algo.algo_name == self.short_entry_algo2:
            # 二级空头开仓成交
            self.short_traded2 = float(Decimal(str(self.short_traded2)) + Decimal(str(trade.volume)))

        if algo.algo_name == self.short_exit_algo2:
            # 二级空头平仓成交
            self.short_traded2 = float(Decimal(str(self.short_traded2)) - Decimal(str(trade.volume)))

        if not self.long_traded1:
            self.long_high1 = 0

        if not self.long_traded2:
            self.long_high2 = 0

        if not self.short_traded1:
            self.short_low1 = 0

        if not self.short_traded2:
            self.short_low2 = 0

        self.put_timer_event()

    def on_algo_stop(self, algo):
        if algo.algo_name == self.long_entry_algo1:
            self.long_entry_algo1 = ''

        if algo.algo_name == self.long_exit_algo1:
            self.long_exit_algo1 = ''

        if algo.algo_name == self.long_entry_algo2:
            self.long_entry_algo2 = ''

        if algo.algo_name == self.long_exit_algo2:
            self.long_exit_algo2 = ''

        if algo.algo_name == self.short_entry_algo1:
            self.short_entry_algo1 = ''

        if algo.algo_name == self.short_exit_algo1:
            self.short_exit_algo1 = ''

        if algo.algo_name == self.short_entry_algo2:
            self.short_entry_algo2 = ''

        if algo.algo_name == self.short_exit_algo2:
            self.short_exit_algo2 = ''

        self.put_timer_event()

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

        if self.trade_mode == TradeMode.ACTUAL:
            max_unit_loss = 0.005 * self.capital
        else:
            max_unit_loss = 0.005 * self.cta_engine.capital

        self.long_volume1 = round_to(max_unit_loss / (self.long_entry1 - self.long_exit1), self.min_volume)
        self.long_volume2 = round_to(max_unit_loss / (self.long_entry2 - self.long_exit2), self.min_volume)
        self.long_volume3 = round_to(max_unit_loss / (self.long_entry3 - self.long_exit3), self.min_volume)
        self.short_volume1 = round_to(max_unit_loss / (self.short_exit1 - self.short_entry1), self.min_volume)
        self.short_volume2 = round_to(max_unit_loss / (self.short_exit2 - self.short_entry2), self.min_volume)
        self.short_volume3 = round_to(max_unit_loss / (self.short_exit3 - self.short_entry3), self.min_volume)

    def buy(self, price: float, volume: float, limit_price:float=0.0):
        if self.best_limit_algo_trading:
            dict = {'template_name':'BestLimitAlgo',
                    'strategy':self,
                    'vt_symbol':self.vt_symbol,
                    'direction':'多',
                    'volume': volume,
                    'offset': '开',
                    'limit_price':limit_price,
                    'tick':self.last_tick}
            return self.cta_engine.algoTradingEngine.start_algo(setting=dict)
        else:
            return super(PivotStrategy_actual, self).buy(price, volume)

    def sell(self, price: float, volume: float, limit_price:float=0.0):
        if self.best_limit_algo_trading:
            dict = {'template_name':'BestLimitAlgo',
                    'strategy': self,
                    'vt_symbol':self.vt_symbol,
                    'direction':'空',
                    'volume': volume,
                    'offset': '平',
                    'limit_price': limit_price,
                    'tick':self.last_tick}
            return self.cta_engine.algoTradingEngine.start_algo(setting=dict)
        else:
            return super(PivotStrategy_actual, self).sell(price, volume)

    def short(self, price: float, volume: float, limit_price:float=0.0):
        if self.best_limit_algo_trading:
            dict = {'template_name':'BestLimitAlgo',
                    'strategy': self,
                    'vt_symbol':self.vt_symbol,
                    'direction':'空',
                    'volume': volume,
                    'offset': '开',
                    'limit_price': limit_price,
                    'tick':self.last_tick}
            return self.cta_engine.algoTradingEngine.start_algo(setting=dict)
        else:
            return super(PivotStrategy_actual, self).short(price, volume)

    def cover(self, price: float, volume: float, limit_price:float=0.0):
        if self.best_limit_algo_trading:
            dict = {'template_name':'BestLimitAlgo',
                    'strategy': self,
                    'vt_symbol':self.vt_symbol,
                    'direction':'多',
                    'volume': volume,
                    'offset': '平',
                    'limit_price': limit_price,
                    'tick':self.last_tick}
            return self.cta_engine.algoTradingEngine.start_algo(setting=dict)
        else:
            return super(PivotStrategy_actual, self).cover(price, volume)

    def clear_data(self):
        self.bg = CustomBarGenerator(on_bar=self.on_bar,
                                     window=0,
                                     on_window_bar=self.on_generate_bar,
                                     interval=Interval.MINUTE)
        self.am = ArrayManager(size=self.exit_window + 1)

        self.long_entry3 = 0
        self.long_volume3 = 0
        self.long_exit3 = 0

        self.long_entry2 = 0
        self.long_volume2 = 0
        self.long_exit2 = 0

        self.long_entry1 = 0
        self.long_volume1 = 0
        self.long_exit1 = 0

        self.pivot = 0

        self.short_entry1 = 0
        self.short_volume1 = 0
        self.short_exit1 = 0

        self.short_entry2 = 0
        self.short_volume2 = 0
        self.short_exit2 = 0

        self.short_entry3 = 0
        self.short_volume3 = 0
        self.short_exit3 = 0

    def get_tick_price(self):
        contract = self.cta_engine.main_engine.get_contract(vt_symbol=self.vt_symbol)
        if contract and contract.pricetick:
            return contract.pricetick
        else:
            return self.tick_price

    def check_base_datetime(self, current_tick:TickData):
        if not current_tick:
            return False

        next_base = next_window_bar_datetime(self.base_datetime + timedelta(seconds=1))
        if self.base_datetime <= without_timezone(current_tick.datetime) < next_base:
            return True
        else:
            return False

    def on_timer(self):
        super().on_timer()
        self.base_datetime_checking = self.check_base_datetime(self.last_tick)

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
    current_datetime = without_timezone(current_datetime)
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

def without_timezone(target:datetime):
    datetime_str = target.strftime('%Y-%m-%d %H:%M:%S')
    return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')


