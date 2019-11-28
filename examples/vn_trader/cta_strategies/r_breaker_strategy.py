from datetime import time
from vnpy.app.cta_strategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)

""" modify by loe """
from vnpy.app.cta_strategy.template import TradeMode
from vnpy.trader.constant import Interval, Direction
import datetime
from datetime import timedelta
import re

#====== 交易时间 ======
#商品期货
MORNING_START_CF = datetime.time(9, 0)
MORNING_REST_CF = datetime.time(10, 15)
MORNING_RESTART_CF = datetime.time(10, 30)
MORNING_END_CF = datetime.time(11, 30)
AFTERNOON_START_CF = datetime.time(13, 30)
AFTERNOON_END_CF = datetime.time(15, 0)

# 商品期货夜盘时间
NIGHT_START_CF = datetime.time(21, 0)
NIGHT_END_CF_N = datetime.time(23, 0) # 到夜间收盘
NIGHT_END_CF_NM = datetime.time(1, 0) # 到凌晨收盘
NIGHT_END_CF_M = datetime.time(2, 30) # 到凌晨收盘

#股指期货
MORNING_PRE_START_SF = datetime.time(6, 0)
MORNING_START_SF = datetime.time(9, 30)
MORNING_END_SF = datetime.time(11, 30)
AFTERNOON_START_SF = datetime.time(13, 0)
AFTERNOON_END_SF = datetime.time(15, 0)

def isFinanceSymbol(symbol):
    financeSymbols = ['IF', 'IC', 'IH']
    startSymbol = re.sub("\d", "", symbol)
    if startSymbol in financeSymbols:
        return True
    else:
        return False

class RBreakerStrategy(CtaTemplate):
    """"""
    author = "loe"

    # 原版参数
    #setup_coef = 0.25
    #break_coef = 0.2
    setup_coef = 0.1
    break_coef = 0.1
    enter_coef_1 = 1.07
    enter_coef_2 = 0.07
    fixed_size = 1
    donchian_window = 30

    trailing_long = 0.4
    trailing_short = 0.4
    multiplier = 1

    buy_break = 0  # 突破买入价
    sell_setup = 0  # 观察卖出价
    sell_enter = 0  # 反转卖出价
    buy_enter = 0  # 反转买入价
    buy_setup = 0  # 观察买入价
    sell_break = 0  # 突破卖出价

    intra_trade_high = 0
    intra_trade_low = 0

    day_high = 0
    day_open = 0
    day_close = 0
    day_low = 0
    tend_high = 0
    tend_low = 0

    """ modify by loe """
    today_setup_long = False                    # 是否突破多头观察
    today_setup_short = False                   # 是否突破空头观察
    virtual_pos = 0                             # 虚拟仓位
    trade_date = None                           # 当前交易日日期
    long_stop = 0                               # 多头止盈止损价
    short_stop = 0                              # 空头止盈止损价

    exit_time = time(hour=14, minute=55)

    """ modify by loe """
    parameters = ["setup_coef",
                  "break_coef",
                  "enter_coef_1",
                  "enter_coef_2",
                  "fixed_size",
                  "multiplier",
                  "trailing_long",
                  "trailing_short"]

    variables = ["virtual_pos",
                 "buy_break",
                 "sell_setup",
                 "sell_enter",
                 "buy_enter",
                 "buy_setup",
                 "sell_break",
                 "today_setup_long",
                 "today_setup_short",
                 "trade_date",
                 "intra_trade_high",
                 "long_stop",
                 "intra_trade_low",
                 "short_stop",
                 "day_high",
                 "day_low"]

    syncs = ["today_setup_long",
             "today_setup_short",
             "virtual_pos",
             "trade_date",
             "intra_trade_high",
             "intra_trade_low",
             "day_high",
             "day_low"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()
        self.bars = []

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        trade_date = self.get_trade_date()
        if not self.trade_date or self.trade_date != trade_date:
            # 新的一天
            self.trade_date = trade_date
            self.clear_variables()

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        if self.trade_mode == TradeMode.ACTUAL:
            self.load_bar(days=20, interval=Interval.DAILY, callback = self.calculate_indicator)

        elif self.trade_mode == TradeMode.BACKTESTING:
            self.load_bar(days=10)
        else:
            raise(0)

        self.write_log("策略完成初始化")

    def calculate_indicator(self, bar:BarData):
        self.buy_setup = bar.low_price - self.setup_coef * (bar.high_price - bar.close_price)  # 观察买入价
        self.sell_setup = bar.high_price + self.setup_coef * (bar.close_price - bar.low_price)  # 观察卖出价

        self.buy_enter = (self.enter_coef_1 / 2) * (
                bar.high_price + bar.low_price) - self.enter_coef_2 * bar.high_price  # 反转买入价
        self.sell_enter = (self.enter_coef_1 / 2) * (
                bar.high_price + bar.low_price) - self.enter_coef_2 * bar.low_price  # 反转卖出价

        self.buy_break = self.sell_setup + self.break_coef * (self.sell_setup - self.buy_setup)  # 突破买入价
        self.sell_break = self.buy_setup - self.break_coef * (self.sell_setup - self.buy_setup)  # 突破卖出价

    def clear_variables(self):
        self.today_setup_long = False
        self.today_setup_short = False
        self.virtual_pos = 0
        self.intra_trade_high = 0
        self.long_stop = 0
        self.intra_trade_low = 0
        self.short_stop = 0
        self.day_high = 0
        self.day_low = 0

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

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        # 保存tick数据到数据库
        #self.saveTick(tick)

        if not self.trading:
            return

        # 过滤无效tick
        t = tick.datetime.time()
        isFinance = isFinanceSymbol(tick.symbol)
        if not isFinance:
            if NIGHT_END_CF_M <= t < MORNING_START_CF or MORNING_REST_CF <= t < MORNING_RESTART_CF or MORNING_END_CF <= t < AFTERNOON_START_CF or AFTERNOON_END_CF <= t < NIGHT_START_CF or NIGHT_END_CF_M <= t < MORNING_START_CF:
                self.write_log(f'====== 过滤无效tick：{tick.vt_symbol}\t{tick.datetime} ======')
                return
        else:
            if t < MORNING_START_SF or MORNING_END_SF <= t < AFTERNOON_START_SF or AFTERNOON_END_SF <= t:
                self.write_log(f'====== 过滤无效tick：{tick.vt_symbol}\t{tick.datetime} ======')
                return

        # 记录当天最高最低价
        if not self.day_high:
            self.day_high = tick.last_price
        else:
            self.day_high = max(self.day_high, tick.last_price)

        if not self.day_low:
            self.day_low = tick.last_price
        else:
            self.day_low = min(self.day_low, tick.last_price)

        if tick.datetime.time() < self.exit_time:
            if self.virtual_pos == 0:
                self.intra_trade_high = 0
                self.long_stop = 0
                self.intra_trade_low = 0
                self.short_stop = 0

                if tick.last_price > self.sell_setup:
                    self.today_setup_long = True

                if tick.last_price < self.buy_setup:
                    self.today_setup_short = True

                if self.today_setup_long:
                    long_entry = max(self.buy_break, self.day_high)
                    if tick.last_price >= long_entry:
                        if self.pos:
                            self.send_email(f'R-Breaker策略出错！！\n多头突破开仓时Pos={self.pos}\n已终止策略')
                            raise (0)

                        self.buy(self.bestLimitOrderPrice(tick, Direction.LONG), abs(self.fixed_size))
                        self.virtual_pos = abs(self.fixed_size)
                        self.intra_trade_high = tick.last_price
                        self.intra_trade_low = tick.last_price
                        return

                    if tick.last_price <= self.sell_enter:
                        if self.pos:
                            self.send_email(f'R-Breaker策略出错！！\n空头反转开仓时Pos={self.pos}\n已终止策略')
                            raise (0)

                        self.short(self.bestLimitOrderPrice(tick, Direction.SHORT), abs(self.fixed_size))
                        self.virtual_pos = abs(self.fixed_size) * -1
                        self.intra_trade_high = tick.last_price
                        self.intra_trade_low = tick.last_price
                        return

                if self.today_setup_short:
                    short_entry = min(self.sell_break, self.day_low)
                    if tick.last_price <= short_entry:
                        if self.pos:
                            self.send_email(f'R-Breaker策略出错！！\n空头突破开仓时Pos={self.pos}\n已终止策略')
                            raise (0)

                        self.short(self.bestLimitOrderPrice(tick, Direction.SHORT), abs(self.fixed_size))
                        self.virtual_pos = abs(self.fixed_size) * -1
                        self.intra_trade_high = tick.last_price
                        self.intra_trade_low = tick.last_price
                        return

                    if tick.last_price >= self.buy_enter:
                        if self.pos:
                            self.send_email(f'R-Breaker策略出错！！\n多头反转开仓时Pos={self.pos}\n已终止策略')
                            raise (0)

                        self.buy(self.bestLimitOrderPrice(tick, Direction.LONG), abs(self.fixed_size))
                        self.virtual_pos = abs(self.fixed_size)
                        self.intra_trade_high = tick.last_price
                        self.intra_trade_low = tick.last_price
                        return

            elif self.virtual_pos > 0:
                self.today_setup_long = False
                self.today_setup_short = False

                self.intra_trade_high = max(self.intra_trade_high, tick.last_price)
                self.long_stop = self.intra_trade_high * (1 - self.trailing_long / 100)
                if tick.last_price <= self.long_stop:
                    if self.pos > 0:
                        self.sell(self.bestLimitOrderPrice(tick, Direction.SHORT), abs(self.pos))
                    self.virtual_pos = 0
                    return

            elif self.virtual_pos < 0:
                self.today_setup_long = False
                self.today_setup_short = False

                self.intra_trade_low = min(self.intra_trade_low, tick.last_price)
                self.short_stop = self.intra_trade_low * (1 + self.trailing_short / 100)
                if tick.last_price >= self.short_stop:
                    if self.pos < 0:
                        self.cover(self.bestLimitOrderPrice(tick, Direction.LONG), abs(self.pos))
                    self.virtual_pos = 0
                    return

        # Close existing position
        else:
            if self.virtual_pos > 0:
                if self.pos > 0:
                    self.sell(self.bestLimitOrderPrice(tick, Direction.SHORT), abs(self.pos))

            elif self.virtual_pos < 0:
                if self.pos < 0:
                    self.cover(self.bestLimitOrderPrice(tick, Direction.LONG), abs(self.pos))

            self.clear_variables()

        self.put_event()

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        self.cancel_all()

        am = self.am
        am.update_bar(bar)
        if not am.inited:
            return

        self.bars.append(bar)
        if len(self.bars) <= 2:
            return
        else:
            self.bars.pop(0)
        last_bar = self.bars[-2]

        """ fake """
        if not (self.pos == 0 or abs(self.pos) == self.fixed_size or abs(self.pos) == self.fixed_size * self.multiplier):
            raise(0)

        # New Day
        if last_bar.datetime.date() != bar.datetime.date():
            """ fake """
            if self.pos:
                raise(0)

            """ modify by loe """
            self.today_setup_long = False
            self.today_setup_short = False

            if self.day_open:
                self.buy_setup = self.day_low - self.setup_coef * (self.day_high - self.day_close)  # 观察买入价
                self.sell_setup = self.day_high + self.setup_coef * (self.day_close - self.day_low)  # 观察卖出价

                self.buy_enter = (self.enter_coef_1 / 2) * (
                            self.day_high + self.day_low) - self.enter_coef_2 * self.day_high  # 反转买入价
                self.sell_enter = (self.enter_coef_1 / 2) * (
                            self.day_high + self.day_low) - self.enter_coef_2 * self.day_low  # 反转卖出价

                """ modify by loe """
                """
                self.buy_break = self.buy_setup + self.break_coef * (self.sell_setup - self.buy_setup)  # 突破买入价
                self.sell_break = self.sell_setup - self.break_coef * (self.sell_setup - self.buy_setup)  # 突破卖出价
                """

                #"""
                self.buy_break = self.sell_setup + self.break_coef * (self.sell_setup - self.buy_setup)  # 突破买入价
                self.sell_break = self.buy_setup - self.break_coef * (self.sell_setup - self.buy_setup)  # 突破卖出价
                #"""

            self.day_open = bar.open_price
            self.day_high = bar.high_price
            self.day_close = bar.close_price
            self.day_low = bar.low_price

        # Today
        else:
            self.day_high = max(self.day_high, bar.high_price)
            self.day_low = min(self.day_low, bar.low_price)
            self.day_close = bar.close_price

        if not self.sell_setup:
            return

        self.tend_high, self.tend_low = am.donchian(self.donchian_window)

        if bar.datetime.time() < self.exit_time:

            if self.pos == 0:
                self.intra_trade_low = bar.low_price
                self.intra_trade_high = bar.high_price

                """ modify by loe """
                """
                if self.tend_high > self.sell_setup:
                    long_entry = max(self.buy_break, self.day_high)
                    self.buy(long_entry, self.fixed_size, stop=True)

                    self.short(self.sell_enter, self.multiplier * self.fixed_size, stop=True)

                elif self.tend_low < self.buy_setup:
                    short_entry = min(self.sell_break, self.day_low)
                    self.short(short_entry, self.fixed_size, stop=True)

                    self.buy(self.buy_enter, self.multiplier * self.fixed_size, stop=True)
                """

                #"""
                if bar.high_price > self.sell_setup:
                    self.today_setup_long = True

                if bar.low_price < self.buy_setup:
                    self.today_setup_short = True

                if self.today_setup_long:
                    long_entry = max(self.buy_break, self.day_high)
                    self.buy(long_entry, self.fixed_size, stop=True)

                    self.short(self.sell_enter, self.multiplier * self.fixed_size, stop=True)

                if self.today_setup_short:
                    short_entry = min(self.sell_break, self.day_low)
                    self.short(short_entry, self.fixed_size, stop=True)

                    self.buy(self.buy_enter, self.multiplier * self.fixed_size, stop=True)
                #"""
            elif self.pos > 0:
                """ modify by loe """
                self.today_setup_long = False
                self.today_setup_short = False

                self.intra_trade_high = max(self.intra_trade_high, bar.high_price)
                long_stop = self.intra_trade_high * (1 - self.trailing_long / 100)
                self.sell(long_stop, abs(self.pos), stop=True)

            elif self.pos < 0:
                """ modify by loe """
                self.today_setup_long = False
                self.today_setup_short = False

                self.intra_trade_low = min(self.intra_trade_low, bar.low_price)
                short_stop = self.intra_trade_low * (1 + self.trailing_short / 100)
                self.cover(short_stop, abs(self.pos), stop=True)

        # Close existing position
        else:
            if self.pos > 0:
                self.sell(bar.close_price * 0.99, abs(self.pos))
            elif self.pos < 0:
                self.cover(bar.close_price * 1.01, abs(self.pos))

        self.put_event()

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        """ modify by loe """
        if self.trade_mode == TradeMode.BACKTESTING:
            current_bond = trade.price * self.cta_engine.size * self.pos * 0.1
            max_bond = self.max_bond_dic['bond']
            if current_bond > max_bond:
                self.max_bond_dic['date'] = self.cta_engine.datetime.date()
                self.max_bond_dic['pos'] = self.pos
                self.max_bond_dic['bond'] = current_bond
        # 邮件提醒
        super(RBreakerStrategy, self).on_trade(trade)

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass

    """ modify by loe """
    def get_trade_date(self):
        now = datetime.datetime.now()
        hour = now.hour
        if hour >= 15:
            return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            return now.replace(hour=0, minute=0, second=0, microsecond=0)

